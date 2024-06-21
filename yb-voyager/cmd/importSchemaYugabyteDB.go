/*
Copyright (c) YugabyteDB, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

var deferredSqlStmts []sqlInfo
var finalFailedSqlStmts []string

func importSchemaInternal(exportDir string, importObjectList []string,
	skipFn func(string, string) bool) error {
	schemaDir := filepath.Join(exportDir, "schema")
	for _, importObjectType := range importObjectList {
		importObjectFilePath := utils.GetObjectFilePath(schemaDir, importObjectType)
		if !utils.FileOrFolderExists(importObjectFilePath) {
			continue
		}
		err := executeSqlFile(importObjectFilePath, importObjectType, skipFn)
		if err != nil {
			return err
		}
	}
	return nil
}

func executeSqlFile(file string, objType string, skipFn func(string, string) bool) error {
	log.Infof("Execute SQL file %q on target %q", file, tconf.Host)
	conn := newTargetConn()

	defer func() {
		if conn != nil {
			conn.Close(context.Background())
		}
	}()

	sqlInfoArr := parseSqlFileForObjectType(file, objType)
	var err error
	for _, sqlInfo := range sqlInfoArr {
		if conn == nil {
			conn = newTargetConn()
		}

		setOrSelectStmt := strings.HasPrefix(strings.ToUpper(sqlInfo.stmt), "SET ") ||
			strings.HasPrefix(strings.ToUpper(sqlInfo.stmt), "SELECT ")
		if !setOrSelectStmt && skipFn != nil && skipFn(objType, sqlInfo.stmt) {
			continue
		}

		if objType == "TABLE" {
			stmt := strings.ToUpper(sqlInfo.stmt)
			skip := strings.Contains(stmt, "ALTER TABLE") && strings.Contains(stmt, "REPLICA IDENTITY")
			if skip {
				//skipping DDLS like ALTER TABLE ... REPLICA IDENTITY .. as this is not supported in YB
				log.Infof("Skipping DDL: %s", sqlInfo.stmt)
				continue
			}
		}

		err = executeSqlStmtWithRetries(&conn, sqlInfo, objType)
		if err != nil {
			return err
		}
	}
	return nil
}

func executeSqlStmtWithRetries(conn **pgx.Conn, sqlInfo sqlInfo, objType string) error {
	var err error
	log.Infof("On %s run query:\n%s\n", tconf.Host, sqlInfo.formattedStmt)
	for retryCount := 0; retryCount <= DDL_MAX_RETRY_COUNT; retryCount++ {
		if retryCount > 0 { // Not the first iteration.
			log.Infof("Sleep for 5 seconds before retrying for %dth time", retryCount)
			time.Sleep(time.Second * 5)
			log.Infof("RETRYING DDL: %q", sqlInfo.stmt)
		}

		if bool(flagPostSnapshotImport) && strings.Contains(objType, "INDEX") {
			err = beforeIndexCreation(sqlInfo, conn, objType)
			if err != nil {
				(*conn).Close(context.Background())
				*conn = nil
				return fmt.Errorf("before index creation: %w", err)
			}
		}
		_, err = (*conn).Exec(context.Background(), sqlInfo.formattedStmt)
		if err == nil {
			utils.PrintSqlStmtIfDDL(sqlInfo.stmt, utils.GetObjectFileName(filepath.Join(exportDir, "schema"), objType))
			return nil
		}

		log.Errorf("DDL Execution Failed for %q: %s", sqlInfo.formattedStmt, err)
		if strings.Contains(strings.ToLower(err.Error()), "conflicts with higher priority transaction") {
			// creating fresh connection
			(*conn).Close(context.Background())
			*conn = newTargetConn()
			continue
		} else if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(SCHEMA_VERSION_MISMATCH_ERR)) &&
			(objType == "INDEX" || objType == "PARTITION_INDEX") { // retriable error
			// creating fresh connection
			(*conn).Close(context.Background())
			*conn = newTargetConn()

			// Extract the schema name and add to the index name
			fullyQualifiedObjName, err := getIndexName(sqlInfo.stmt, sqlInfo.objName)
			if err != nil {
				(*conn).Close(context.Background())
				*conn = nil
				return fmt.Errorf("extract qualified index name from DDL [%v]: %v", sqlInfo.stmt, err)
			}

			// DROP INDEX in case INVALID index got created
			// `err` is already being used for retries, so using `err2`
			err2 := dropIdx(*conn, fullyQualifiedObjName)
			if err2 != nil {
				(*conn).Close(context.Background())
				*conn = nil
				return fmt.Errorf("drop invalid index %q: %w", fullyQualifiedObjName, err2)
			}
			continue
		} else if missingRequiredSchemaObject(err) {
			log.Infof("deffering execution of SQL: %s", sqlInfo.formattedStmt)
			deferredSqlStmts = append(deferredSqlStmts, sqlInfo)
		} else if isAlreadyExists(err.Error()) {
			// pg_dump generates `CREATE SCHEMA public;` in the schemas.sql. Because the `public`
			// schema already exists on the target YB db, the create schema statement fails with
			// "already exists" error. Ignore the error.
			if bool(tconf.IgnoreIfExists) || strings.EqualFold(strings.Trim(sqlInfo.stmt, " \n"), "CREATE SCHEMA public;") {
				err = nil
			}
		}
		break // no more iteration in case of non retriable error
	}
	if err != nil {
		(*conn).Close(context.Background())
		*conn = nil
		if missingRequiredSchemaObject(err) {
			// Do nothing for deferred case
		} else {
			utils.PrintSqlStmtIfDDL(sqlInfo.stmt, utils.GetObjectFileName(filepath.Join(exportDir, "schema"), objType))
			color.Red(fmt.Sprintf("%s\n", err.Error()))
			if tconf.ContinueOnError {
				log.Infof("appending stmt to failedSqlStmts list: %s\n", utils.GetSqlStmtToPrint(sqlInfo.stmt))
				errString := "/*\n" + err.Error() + "\n*/\n"
				finalFailedSqlStmts = append(finalFailedSqlStmts, errString+sqlInfo.formattedStmt)
			} else {
				return err
			}
		}
		return nil
	}
	return err
}

/*
Try re-executing each DDL from deferred list.
If fails, silently avoid the error.
Else remove from deferredSQLStmts list
At the end, add the unsuccessful ones to a failedSqlStmts list and report to the user
*/
func importDeferredStatements() {
	if len(deferredSqlStmts) == 0 {
		return
	}
	log.Infof("Number of statements in deferredSQLStmts list: %d\n", len(deferredSqlStmts))

	utils.PrintAndLog("\nExecuting the remaining SQL statements...\n\n")
	maxIterations := len(deferredSqlStmts)
	conn := newTargetConn()
	defer func() { conn.Close(context.Background()) }()

	var err error
	var finalFailedDeferredStmts []string
	// max loop iterations to remove all errors
	for i := 1; i <= maxIterations && len(deferredSqlStmts) > 0; i++ {
		beforeDeferredSqlCount := len(deferredSqlStmts)
		var failedSqlStmtInIthIteration []string
		for j := 0; j < len(deferredSqlStmts); j++ {
			_, err = conn.Exec(context.Background(), deferredSqlStmts[j].formattedStmt)
			if err == nil {
				utils.PrintAndLog("%s\n", utils.GetSqlStmtToPrint(deferredSqlStmts[j].stmt))
				// removing successfully executed SQL
				deferredSqlStmts = append(deferredSqlStmts[:j], deferredSqlStmts[j+1:]...)
				break
			} else {
				log.Infof("failed retry of deferred stmt: %s\n%v", utils.GetSqlStmtToPrint(deferredSqlStmts[j].stmt), err)
				errString := fmt.Sprintf("/*\n%s\n*/\n", err.Error())
				failedSqlStmtInIthIteration = append(failedSqlStmtInIthIteration, errString+deferredSqlStmts[j].formattedStmt)
				err = conn.Close(context.Background())
				if err != nil {
					log.Warnf("error while closing the connection due to failed deferred stmt: %v", err)
				}
				conn = newTargetConn()
			}
		}

		afterDeferredSqlCount := len(deferredSqlStmts)
		if afterDeferredSqlCount == 0 {
			log.Infof("all of the deferred statements executed successfully in the %d iteration", i)
		} else if beforeDeferredSqlCount == afterDeferredSqlCount {
			// no need for further iterations since the deferred list will remain same
			log.Infof("none of the deferred statements executed successfully in the %d iteration", i)
			finalFailedDeferredStmts = failedSqlStmtInIthIteration
			break
		}
	}
	finalFailedSqlStmts = append(finalFailedSqlStmts, finalFailedDeferredStmts...)
}

func applySchemaObjectFilterFlags(importObjectOrderList []string) []string {
	var finalImportObjectList []string
	excludeObjectList := utils.CsvStringToSlice(tconf.ExcludeImportObjects)
	for i, item := range excludeObjectList {
		excludeObjectList[i] = strings.ToUpper(item)
	}
	if tconf.ImportObjects != "" {
		includeObjectList := utils.CsvStringToSlice(tconf.ImportObjects)
		for i, item := range includeObjectList {
			includeObjectList[i] = strings.ToUpper(item)
		}
		if importObjectsInStraightOrder {
			// Import the objects in the same order as when listed by the user.
			for _, listedObject := range includeObjectList {
				if slices.Contains(importObjectOrderList, listedObject) {
					finalImportObjectList = append(finalImportObjectList, listedObject)
				}
			}
		} else {
			// Import the objects in the default order.
			for _, supportedObject := range importObjectOrderList {
				if slices.Contains(includeObjectList, supportedObject) {
					finalImportObjectList = append(finalImportObjectList, supportedObject)
				}
			}
		}
	} else {
		finalImportObjectList = utils.SetDifference(importObjectOrderList, excludeObjectList)
	}
	if sourceDBType == "postgresql" && !slices.Contains(finalImportObjectList, "SCHEMA") && !bool(flagPostSnapshotImport) { // Schema should be migrated by default.
		finalImportObjectList = append([]string{"SCHEMA"}, finalImportObjectList...)
	}

	if !flagPostSnapshotImport {
		finalImportObjectList = append(finalImportObjectList, []string{"UNIQUE INDEX"}...)
	}
	return finalImportObjectList
}
