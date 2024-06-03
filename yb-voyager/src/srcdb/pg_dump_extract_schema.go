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
package srcdb

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

func pgdumpExtractSchema(source *Source, connectionUri string, exportDir string, schemaDir string) {
	pgDumpPath, err := GetAbsPathOfPGCommand("pg_dump")
	if err != nil {
		utils.ErrExit("could not get absolute path of pg_dump command: %v", err)
	}

	pgDumpArgs.Schema = source.Schema
	pgDumpArgs.SchemaTempFilePath = filepath.Join(exportDir, "temp", "schema.sql")
	pgDumpArgs.NoComments = strconv.FormatBool(!bool(source.CommentsOnObjects))
	pgDumpArgs.ExtensionPattern = `"*"`

	args := getPgDumpArgsFromFile("schema")
	cmd := fmt.Sprintf(`%s '%s' %s`, pgDumpPath, connectionUri, args)
	log.Infof("Running command: %s", cmd)

	preparedPgdumpCommand := exec.Command("/bin/bash", "-c", cmd)
	preparedPgdumpCommand.Env = append(os.Environ(), "PGPASSWORD="+source.Password)
	stdout, err := preparedPgdumpCommand.CombinedOutput()
	//pg_dump formats its stdout messages, %s is sufficient.
	if string(stdout) != "" {
		log.Infof("%s", string(stdout))
	}
	if err != nil {
		utils.WaitChannel <- 1
		<-utils.WaitChannel
		log.Infof("pg_dump failed to export schema with output: %s", string(stdout))
		utils.ErrExit("data export unsuccessful: %v. For more details check '%s/logs/yb-voyager-export-schema.log'.\n", err, exportDir)
	}
}

func readSchemaFile(path string) []string {
	file, err := os.Open(path)
	if err != nil {
		utils.ErrExit("error in opening schema file %s: %v", path, err)
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !shouldSkipLine(line) {
			lines = append(lines, line)
		}
	}

	if scanner.Err() != nil {
		utils.ErrExit("error in reading schema file %s: %v", path, scanner.Err())
	}

	return lines
}

// For example: -- Name: address address_city_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
var sqlInfoCommentRegex = regexp.MustCompile("-- Name:.*; Type:.*; Schema: .*")

// NOTE: This is for case when --schema-only option is provided with pg_dump[Data shouldn't be there]
func parseSchemaFile(exportDir string, schemaDir string, exportObjectTypesList []string) int {
	schemaFilePath := filepath.Join(exportDir, "temp", "schema.sql")
	if utils.FileOrFolderExists(filepath.Join(schemaDir, "schema.sql")) { // assess-migration workflow
		schemaFilePath = filepath.Join(schemaDir, "schema.sql")
	}

	log.Infof("begun parsing the schema file %q", schemaFilePath)
	lines := readSchemaFile(schemaFilePath)
	var delimiterIndexes []int
	for i, line := range lines {
		if isDelimiterLine(line) {
			delimiterIndexes = append(delimiterIndexes, i)
		}
	}

	// map to store the sql statements for each db object type
	// map's key are based on the elements of 'utils.postgresSchemaObjectList' array
	objSqlStmts := make(map[string]*strings.Builder)
	// initialize the map
	allObjectTypesList := utils.GetSchemaObjectList("postgresql")
	for _, objType := range allObjectTypesList {
		objSqlStmts[objType] = &strings.Builder{}
	}

	var alterAttachPartition, uncategorizedSqls, setSessionVariables strings.Builder
	for i := 0; i < len(delimiterIndexes); i++ {
		var stmts string
		if i == len(delimiterIndexes)-1 {
			stmts = strings.Join(lines[delimiterIndexes[i]+1:], "\n") + "\n\n\n"
		} else {
			stmts = strings.Join(lines[delimiterIndexes[i]+1:delimiterIndexes[i+1]], "\n") + "\n\n\n"
		}

		if i == 0 {
			// Deal with SET statments
			setSessionVariables.WriteString("-- setting variables for current session\n")
			setSessionVariables.WriteString(stmts)
		} else {
			delimiterLine := lines[delimiterIndexes[i]]
			sqlType := extractSqlTypeFromComment(delimiterLine)
			switch sqlType {
			case "SCHEMA", "TYPE", "DOMAIN", "RULE", "FUNCTION",
				"AGGREGATE", "PROCEDURE", "VIEW", "TRIGGER", "EXTENSION", "COMMENT", "COLLATION":
				objSqlStmts[sqlType].WriteString(stmts)
			case "SEQUENCE", "SEQUENCE OWNED BY":
				objSqlStmts["SEQUENCE"].WriteString(stmts)
			case "INDEX", "INDEX ATTACH":
				objSqlStmts["INDEX"].WriteString(stmts)
			case "TABLE", "DEFAULT", "CONSTRAINT", "FK CONSTRAINT", "CHECK CONSTRAINT":
				objSqlStmts["TABLE"].WriteString(stmts)
			case "TABLE ATTACH":
				alterAttachPartition.WriteString(stmts)
			case "MATERIALIZED VIEW":
				objSqlStmts["MVIEW"].WriteString(stmts)
			default:
				uncategorizedSqls.WriteString(stmts)
			}
		}
	}

	// merging TABLE ATTACH later with TABLE - to avoid alter add PK on partitioned tables
	objSqlStmts["TABLE"].WriteString(alterAttachPartition.String())

	for objType, sqlStmts := range objSqlStmts {
		if !utils.ContainsString(exportObjectTypesList, objType) || sqlStmts.Len() == 0 { // create .sql file only if there are DDLs or the user has asked for that object type
			continue
		}
		filePath := utils.GetObjectFilePath(schemaDir, objType)
		log.Infof("creating schema objects file for %q at %q", objType, filePath)
		dataBytes := []byte(setSessionVariables.String() + sqlStmts.String())

		err := os.WriteFile(filePath, dataBytes, 0644)
		if err != nil {
			utils.ErrExit("Failed to create sql file for %q: %v", objType, err)
		}
	}

	if uncategorizedSqls.Len() > 0 && filepath.Dir(schemaDir) == exportDir { // skipping in case of yb-voyager assess-migration cmd
		filePath := filepath.Join(schemaDir, "uncategorized.sql")
		// TODO: add it to the analyze-schema report in case of postgresql
		msg := fmt.Sprintf("\nIMPORTANT NOTE: Please, review and manually import the DDL statements from the %q\n", filePath)
		color.Red(msg)
		log.Infof(msg)
		os.WriteFile(filePath, []byte(setSessionVariables.String()+uncategorizedSqls.String()), 0644)
		return 1
	}

	log.Infof("schema file %q parsed successfully.", schemaFilePath)
	return 0
}

// Example sqlInfoComment: -- Name: address address_city_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
func extractSqlTypeFromComment(comment string) string {
	sqlInfoCommentSlice := strings.Split(comment, ";")
	for _, info := range sqlInfoCommentSlice {
		info = strings.Trim(info, " ")
		if info[:4] == "Type" {
			return strings.Trim(strings.Split(info, ":")[1], " ")
		}
	}

	utils.ErrExit("Unable to extract sqlType from comment: %s", comment)
	return "" // unreachable
}

func shouldSkipLine(line string) bool {
	return strings.HasPrefix(line, "SET default_table_access_method") ||
		strings.Compare(line, "--") == 0 || len(line) == 0 ||
		strings.EqualFold(line, "-- PostgreSQL database dump complete") ||
		strings.EqualFold(line, "-- PostgreSQL database dump") ||
		strings.HasPrefix(line, "-- Dumped from database version") ||
		strings.HasPrefix(line, "SET check_function_bodies = false")
}

func isDelimiterLine(line string) bool {
	return strings.HasPrefix(line, "-- Dumped by pg_dump version") || sqlInfoCommentRegex.MatchString(line)
}
