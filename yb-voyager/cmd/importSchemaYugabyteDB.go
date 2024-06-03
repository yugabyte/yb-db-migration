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

	log "github.com/sirupsen/logrus"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"golang.org/x/exp/slices"
)

var deferredSqlStmts []sqlInfo
var failedSqlStmts []string

func importSchemaInternal(exportDir string, importObjectList []string,
	skipFn func(string, string) bool) {
	schemaDir := filepath.Join(exportDir, "schema")
	for _, importObjectType := range importObjectList {
		importObjectFilePath := utils.GetObjectFilePath(schemaDir, importObjectType)
		if !utils.FileOrFolderExists(importObjectFilePath) {
			continue
		}
		executeSqlFile(importObjectFilePath, importObjectType, skipFn)
	}

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
			failedSqlStmts = failedSqlStmtInIthIteration
			break
		}
	}
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
