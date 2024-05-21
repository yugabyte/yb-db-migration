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
	"encoding/json"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/cp"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/metadb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/srcdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

type summaryInfo struct {
	totalCount   int
	invalidCount map[string]bool
	objSet       map[string]bool
	details      map[string]bool //any details about the object type
}

type sqlInfo struct {
	objName string
	// SQL statement after removing all new-lines from it. Simplifies analyze-schema regex matching.
	stmt string
	// Formatted SQL statement with new-lines and tabs
	formattedStmt string
}

var (
	anything                     = `.*`
	ws                           = `[\s\n\t]+`
	optionalWS                   = `[\s\n\t]*` //optional white spaces
	ident                        = `[a-zA-Z0-9_."]+`
	ifExists                     = opt("IF", "EXISTS")
	ifNotExists                  = opt("IF", "NOT", "EXISTS")
	optionalCommaSeperatedTokens = `[^,]+(?:,[^,]+){0,}`
	commaSeperatedTokens         = `[^,]+(?:,[^,]+){1,}`
	unqualifiedIdent             = `[a-zA-Z0-9_]+`
	fetchLocation                = `[a-zA-Z]+`
	supportedExtensionsOnYB      = []string{
		"adminpack", "amcheck", "autoinc", "bloom", "btree_gin", "btree_gist", "citext", "cube",
		"dblink", "dict_int", "dict_xsyn", "earthdistance", "file_fdw", "fuzzystrmatch", "hll", "hstore",
		"hypopg", "insert_username", "intagg", "intarray", "isn", "lo", "ltree", "moddatetime",
		"orafce", "pageinspect", "pg_buffercache", "pg_cron", "pg_freespacemap", "pg_hint_plan", "pg_prewarm", "pg_stat_monitor",
		"pg_stat_statements", "pg_trgm", "pg_visibility", "pgaudit", "pgcrypto", "pgrowlocks", "pgstattuple", "plpgsql",
		"postgres_fdw", "refint", "seg", "sslinfo", "tablefunc", "tcn", "timetravel", "tsm_system_rows",
		"tsm_system_time", "unaccent", "uuid-ossp", "yb_pg_metrics", "yb_test_extension",
	}
)

func cat(tokens ...string) string {
	str := ""
	for idx, token := range tokens {
		str += token
		nextToken := ""
		if idx < len(tokens)-1 {
			nextToken = tokens[idx+1]
		} else {
			break
		}

		if strings.HasSuffix(token, optionalWS) ||
			strings.HasPrefix(nextToken, optionalWS) || strings.HasSuffix(token, anything) {
			// White space already part of tokens. Nothing to do.
		} else {
			str += ws
		}
	}
	return str
}

func opt(tokens ...string) string {
	return fmt.Sprintf("(%s)?%s", cat(tokens...), optionalWS)
}

func capture(str string) string {
	return "(" + str + ")"
}

func re(tokens ...string) *regexp.Regexp {
	s := cat(tokens...)
	s = "(?i)" + s
	return regexp.MustCompile(s)
}

func parenth(s string) string {
	return `\(` + s + `\)`
}

var (
	analyzeSchemaReportFormat string
	sourceObjList             []string
	schemaAnalysisReport      utils.SchemaReport
	tblParts                  = make(map[string]string)
	// key is partitioned table, value is filename where the ADD PRIMARY KEY statement resides
	primaryCons      = make(map[string]string)
	summaryMap       = make(map[string]*summaryInfo)
	multiRegex       = regexp.MustCompile(`([a-zA-Z0-9_\.]+[,|;])`)
	dollarQuoteRegex = regexp.MustCompile(`(\$.*\$)`)
	//TODO: optional but replace every possible space or new line char with [\s\n]+ in all regexs
	createConvRegex           = re("CREATE", opt("DEFAULT"), optionalWS, "CONVERSION", capture(ident))
	alterConvRegex            = re("ALTER", "CONVERSION", capture(ident))
	gistRegex                 = re("CREATE", "INDEX", ifNotExists, capture(ident), "ON", capture(ident), anything, "USING", "GIST")
	brinRegex                 = re("CREATE", "INDEX", ifNotExists, capture(ident), "ON", capture(ident), anything, "USING", "brin")
	spgistRegex               = re("CREATE", "INDEX", ifNotExists, capture(ident), "ON", capture(ident), anything, "USING", "spgist")
	rtreeRegex                = re("CREATE", "INDEX", ifNotExists, capture(ident), "ON", capture(ident), anything, "USING", "rtree")
	ginRegex                  = re("CREATE", "INDEX", ifNotExists, capture(ident), "ON", capture(ident), anything, "USING", "GIN", capture(optionalCommaSeperatedTokens))
	viewWithCheckRegex        = re("VIEW", capture(ident), anything, "WITH", "CHECK", "OPTION")
	rangeRegex                = re("PRECEDING", "and", anything, ":float")
	fetchRegex                = re("FETCH", capture(fetchLocation), "FROM")
	notSupportedFetchLocation = []string{"FIRST", "LAST", "NEXT", "PRIOR", "RELATIVE", "ABSOLUTE", "NEXT", "FORWARD", "BACKWARD"}
	alterAggRegex             = re("ALTER", "AGGREGATE", capture(ident))
	dropCollRegex             = re("DROP", "COLLATION", ifExists, capture(commaSeperatedTokens))
	dropIdxRegex              = re("DROP", "INDEX", ifExists, capture(commaSeperatedTokens))
	dropViewRegex             = re("DROP", "VIEW", ifExists, capture(commaSeperatedTokens))
	dropSeqRegex              = re("DROP", "SEQUENCE", ifExists, capture(commaSeperatedTokens))
	dropForeignRegex          = re("DROP", "FOREIGN", "TABLE", ifExists, capture(commaSeperatedTokens))
	dropIdxConcurRegex        = re("DROP", "INDEX", "CONCURRENTLY", ifExists, capture(ident))
	trigRefRegex              = re("CREATE", "TRIGGER", capture(ident), anything, "REFERENCING")
	constrTrgRegex            = re("CREATE", "CONSTRAINT", "TRIGGER", capture(ident))
	currentOfRegex            = re("WHERE", "CURRENT", "OF")
	amRegex                   = re("CREATE", "ACCESS", "METHOD", capture(ident))
	idxConcRegex              = re("REINDEX", anything, capture(ident))
	storedRegex               = re(capture(unqualifiedIdent), capture(unqualifiedIdent), "GENERATED", "ALWAYS", anything, "STORED")
	createTableRegex          = re("CREATE", "TABLE", ifNotExists, capture(ident), anything)
	partitionColumnsRegex     = re("CREATE", "TABLE", ifNotExists, capture(ident), parenth(capture(optionalCommaSeperatedTokens)), "PARTITION BY", capture("[A-Za-z]+"), parenth(capture(optionalCommaSeperatedTokens)))
	likeAllRegex              = re("CREATE", "TABLE", ifNotExists, capture(ident), anything, "LIKE", anything, "INCLUDING ALL")
	likeRegex                 = re("CREATE", "TABLE", ifNotExists, capture(ident), anything, `\(LIKE`)
	inheritRegex              = re("CREATE", opt(capture(unqualifiedIdent)), "TABLE", ifNotExists, capture(ident), anything, "INHERITS", "[ |(]")
	withOidsRegex             = re("CREATE", "TABLE", ifNotExists, capture(ident), anything, "WITH", anything, "OIDS")
	intvlRegex                = re("CREATE", "TABLE", ifNotExists, capture(ident)+`\(`, anything, "interval", "PRIMARY")
	anydataRegex              = re("CREATE", "TABLE", ifNotExists, capture(ident), anything, "AnyData", anything)
	anydatasetRegex           = re("CREATE", "TABLE", ifNotExists, capture(ident), anything, "AnyDataSet", anything)
	anyTypeRegex              = re("CREATE", "TABLE", ifNotExists, capture(ident), anything, "AnyType", anything)
	uriTypeRegex              = re("CREATE", "TABLE", ifNotExists, capture(ident), anything, "URIType", anything)
	//super user role required, language c is errored as unsafe
	cLangRegex = re("CREATE", opt("OR REPLACE"), "FUNCTION", capture(ident), anything, "language c")

	alterOfRegex                    = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), anything, "OF", anything)
	alterSchemaRegex                = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), anything, "SET SCHEMA")
	createSchemaRegex               = re("CREATE", "SCHEMA", anything, "CREATE", "TABLE")
	alterNotOfRegex                 = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), anything, "NOT OF")
	alterColumnStatsRegex           = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), anything, "ALTER", "COLUMN", capture(ident), anything, "SET STATISTICS")
	alterColumnStorageRegex         = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), anything, "ALTER", "COLUMN", capture(ident), anything, "SET STORAGE")
	alterColumnSetAttributesRegex   = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), anything, "ALTER", "COLUMN", capture(ident), anything, "SET", `\(`)
	alterColumnResetAttributesRegex = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), anything, "ALTER", "COLUMN", capture(ident), anything, "RESET", anything)
	alterConstrRegex                = re("ALTER", opt(capture(unqualifiedIdent)), ifExists, "TABLE", capture(ident), anything, "ALTER", "CONSTRAINT")
	setOidsRegex                    = re("ALTER", opt(capture(unqualifiedIdent)), "TABLE", ifExists, capture(ident), anything, "SET WITH OIDS")
	clusterRegex                    = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), anything, "CLUSTER")
	withoutClusterRegex             = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), anything, "SET WITHOUT CLUSTER")
	alterSetRegex                   = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), "SET")
	alterIdxRegex                   = re("ALTER", "INDEX", capture(ident), "SET")
	alterResetRegex                 = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), "RESET")
	alterOptionsRegex               = re("ALTER", opt(capture(unqualifiedIdent)), "TABLE", ifExists, capture(ident), "OPTIONS")
	alterInhRegex                   = re("ALTER", opt(capture(unqualifiedIdent)), "TABLE", ifExists, capture(ident), "INHERIT")
	valConstrRegex                  = re("ALTER", opt(capture(unqualifiedIdent)), "TABLE", ifExists, capture(ident), "VALIDATE CONSTRAINT")
	deferRegex                      = re("ALTER", opt(capture(unqualifiedIdent)), "TABLE", ifExists, capture(ident), anything, "UNIQUE", anything, "deferrable")
	alterViewRegex                  = re("ALTER", "VIEW", capture(ident))
	dropAttrRegex                   = re("ALTER", "TYPE", capture(ident), "DROP ATTRIBUTE")
	alterTypeRegex                  = re("ALTER", "TYPE", capture(ident))
	alterTblSpcRegex                = re("ALTER", "TABLESPACE", capture(ident), "SET")

	// table partition. partitioned table is the key in tblParts map
	tblPartitionRegex = re("CREATE", "TABLE", ifNotExists, capture(ident), anything, "PARTITION", "OF", capture(ident))
	addPrimaryRegex   = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), anything, "ADD PRIMARY KEY")
	primRegex         = re("CREATE", "FOREIGN", "TABLE", capture(ident)+`\(`, anything, "PRIMARY KEY")
	foreignKeyRegex   = re("CREATE", "FOREIGN", "TABLE", capture(ident)+`\(`, anything, "REFERENCES", anything)

	// unsupported SQLs exported by ora2pg
	compoundTrigRegex          = re("CREATE", opt("OR REPLACE"), "TRIGGER", capture(ident), anything, "COMPOUND", anything)
	unsupportedCommentRegex1   = re("--", anything, "(unsupported)")
	packageSupportCommentRegex = re("--", anything, "Oracle package ", "'"+capture(ident)+"'", anything, "please edit to match PostgreSQL syntax")
	unsupportedCommentRegex2   = re("--", anything, "please edit to match PostgreSQL syntax")
	typeUnsupportedRegex       = re("Inherited types are not supported", anything, "replacing with inherited table")
	bulkCollectRegex           = re("BULK COLLECT") // ora2pg unable to convert this oracle feature into a PostgreSQL compatible syntax
	jsonFuncRegex              = re("CREATE", opt("OR REPLACE"), capture(unqualifiedIdent), capture(ident), anything, "JSON_ARRAYAGG")
)

const (
	INHERITANCE_ISSUE_REASON        = "TABLE INHERITANCE not supported in YugabyteDB"
	CONSTRAINT_TRIGGER_ISSUE_REASON = "CONSTRAINT TRIGGER not supported yet."
	COMPOUND_TRIGGER_ISSUE_REASON   = "COMPOUND TRIGGER not supported in YugabyteDB."

	STORED_GENERATED_COLUMN_ISSUE_REASON = "Stored generated column not supported."

	GIST_INDEX_ISSUE_REASON = "Schema contains GIST index which is not supported."
)

// Reports one case in JSON
func reportCase(filePath string, reason string, ghIssue string, suggestion string, objType string, objName string, sqlStmt string) {
	var issue utils.Issue
	issue.FilePath = filePath
	issue.Reason = reason
	issue.GH = ghIssue
	issue.Suggestion = suggestion
	issue.ObjectType = objType
	issue.ObjectName = objName
	issue.SqlStatement = sqlStmt

	schemaAnalysisReport.Issues = append(schemaAnalysisReport.Issues, issue)
}

func reportAddingPrimaryKey(fpath string, tbl string, line string) {
	reportCase(fpath, "Adding primary key to a partitioned table is not yet implemented.",
		"https://github.com/yugabyte/yugabyte-db/issues/10074", "", "", tbl, line)
}

func reportBasedOnComment(comment int, fpath string, issue string, suggestion string, objName string, objType string, line string) {
	if comment == 1 {
		reportCase(fpath, "Unsupported, please edit to match PostgreSQL syntax", issue, suggestion, objType, objName, line)
		summaryMap[objType].invalidCount[objName] = true
	} else if comment == 2 {
		// reportCase(fpath, "PACKAGE in oracle are exported as Schema, please review and edit to match PostgreSQL syntax if required, Package is "+objName, issue, suggestion, objType)
		summaryMap["PACKAGE"].objSet[objName] = true
	} else if comment == 3 {
		reportCase(fpath, "SQLs in file might be unsupported please review and edit to match PostgreSQL syntax if required. ", issue, suggestion, objType, objName, line)
	} else if comment == 4 {
		summaryMap[objType].details["Inherited Types are present which are not supported in PostgreSQL syntax, so exported as Inherited Tables"] = true
	}

}

// return summary about the schema objects like tables, indexes, functions, sequences
func reportSchemaSummary(sourceDBConf *srcdb.Source) utils.SchemaSummary {
	var schemaSummary utils.SchemaSummary

	if !tconf.ImportMode && sourceDBConf != nil { // this info is available only if we are exporting from source
		schemaSummary.DBName = sourceDBConf.DBName
		schemaSummary.SchemaNames = strings.Split(sourceDBConf.Schema, "|")
		schemaSummary.DBVersion = sourceDBConf.DBVersion
	}

	addSummaryDetailsForIndexes()
	for _, objType := range sourceObjList {
		if summaryMap[objType].totalCount == 0 {
			continue
		}

		var dbObject utils.DBObject
		dbObject.ObjectType = objType
		dbObject.TotalCount = summaryMap[objType].totalCount
		dbObject.InvalidCount = len(lo.Keys(summaryMap[objType].invalidCount))
		dbObject.ObjectNames = getMapKeysString(summaryMap[objType].objSet)
		dbObject.Details = strings.Join(lo.Keys(summaryMap[objType].details), "\n")
		schemaSummary.DBObjects = append(schemaSummary.DBObjects, dbObject)
	}
	filePath := filepath.Join(exportDir, "schema", "uncategorized.sql")
	if utils.FileOrFolderExists(filePath) {
		note := fmt.Sprintf("Review and manually import the DDL statements from the file %s", filePath)
		schemaSummary.Notes = append(schemaAnalysisReport.SchemaSummary.Notes, note)
	}
	return schemaSummary
}

func addSummaryDetailsForIndexes() {
	var indexesInfo []utils.IndexInfo
	found, err := metaDB.GetJsonObject(nil, metadb.SOURCE_INDEXES_INFO_KEY, &indexesInfo)
	if err != nil {
		utils.ErrExit("analyze schema report summary: load indexes info: %s", err)
	}
	if !found {
		return
	}
	exportedIndexes := lo.Keys(summaryMap["INDEX"].objSet)
	unexportedIdxsMsg := "Indexes which are neither exported by yb-voyager as they are unsupported in YB and needs to be handled manually:\n"
	unexportedIdxsPresent := false
	for _, indexInfo := range indexesInfo {
		sourceIdxName := indexInfo.TableName + "_" + strings.Join(indexInfo.Columns, "_")
		if !slices.Contains(exportedIndexes, strings.ToLower(sourceIdxName)) {
			unexportedIdxsPresent = true
			unexportedIdxsMsg += fmt.Sprintf("\t\tIndex Name=%s, Index Type=%s\n", indexInfo.IndexName, indexInfo.IndexType)
		}
	}
	if unexportedIdxsPresent {
		summaryMap["INDEX"].details[unexportedIdxsMsg] = true
	}
}

// Checks Whether there is a GIN index
/*
Following type of SQL queries are being taken care of by this function -
	1. CREATE INDEX index_name ON table_name USING gin(column1, column2 ...)
	2. CREATE INDEX index_name ON table_name USING gin(column1 [ASC/DESC/HASH])
	3. CREATE EXTENSION btree_gin;
*/
func checkGin(sqlInfoArr []sqlInfo, fpath string) {
	for _, sqlInfo := range sqlInfoArr {
		matchGin := ginRegex.FindStringSubmatch(sqlInfo.stmt)
		if matchGin != nil {
			columnsFromGin := strings.Trim(matchGin[4], `()`)
			columnList := strings.Split(columnsFromGin, ",")
			if len(columnList) > 1 {
				summaryMap["INDEX"].invalidCount[matchGin[2]] = true
				reportCase(fpath, "Schema contains gin index on multi column which is not supported.",
					"https://github.com/yugabyte/yugabyte-db/issues/7850", "", "INDEX", matchGin[2], sqlInfo.formattedStmt)
			} else {
				if strings.Contains(strings.ToUpper(columnList[0]), "ASC") || strings.Contains(strings.ToUpper(columnList[0]), "DESC") || strings.Contains(strings.ToUpper(columnList[0]), "HASH") {
					summaryMap["INDEX"].invalidCount[matchGin[2]] = true
					reportCase(fpath, "Schema contains gin index on column with ASC/DESC/HASH Clause which is not supported.",
						"https://github.com/yugabyte/yugabyte-db/issues/7850", "", "INDEX", matchGin[2], sqlInfo.formattedStmt)
				}
			}
		}
		if strings.Contains(strings.ToLower(sqlInfo.stmt), "using gin") {
			summaryMap["INDEX"].details["There are some gin indexes present in the schema, but gin indexes are partially supported in YugabyteDB as mentioned in (https://github.com/yugabyte/yugabyte-db/issues/7850) so take a look and modify them if not supported."] = true
		}
	}
}

// Checks whether there is gist index
func checkGist(sqlInfoArr []sqlInfo, fpath string) {
	for _, sqlInfo := range sqlInfoArr {
		if idx := gistRegex.FindStringSubmatch(sqlInfo.stmt); idx != nil {
			summaryMap["INDEX"].invalidCount[idx[2]] = true
			reportCase(fpath, GIST_INDEX_ISSUE_REASON,
				"https://github.com/YugaByte/yugabyte-db/issues/1337", "", "INDEX", idx[2], sqlInfo.formattedStmt)
		} else if idx := brinRegex.FindStringSubmatch(sqlInfo.stmt); idx != nil {
			summaryMap["INDEX"].invalidCount[idx[2]] = true
			reportCase(fpath, "index method 'brin' not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1337", "", "INDEX", idx[2], sqlInfo.formattedStmt)
		} else if idx := spgistRegex.FindStringSubmatch(sqlInfo.stmt); idx != nil {
			summaryMap["INDEX"].invalidCount[idx[2]] = true
			reportCase(fpath, "index method 'spgist' not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1337", "", "INDEX", idx[2], sqlInfo.formattedStmt)
		} else if idx := rtreeRegex.FindStringSubmatch(sqlInfo.stmt); idx != nil {
			summaryMap["INDEX"].invalidCount[idx[2]] = true
			reportCase(fpath, "index method 'rtree' is superceded by 'gist' which is not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1337", "", "INDEX", idx[2], sqlInfo.formattedStmt)
		}
	}
}

// Checks compatibility of views
func checkViews(sqlInfoArr []sqlInfo, fpath string) {
	for _, sqlInfo := range sqlInfoArr {
		/*if dropMatViewRegex.MatchString(sqlInfo.stmt) {
			reportCase(fpath, "DROP MATERIALIZED VIEW not supported yet.",a
				"https://github.com/YugaByte/yugabyte-db/issues/10102", "")
		} else if view := matViewRegex.FindStringSubmatch(sqlInfo.stmt); view != nil {
			reportCase(fpath, "Schema contains materialized view which is not supported. The view is: "+view[1],
				"https://github.com/yugabyte/yugabyte-db/issues/10102", "")
		} else */
		if view := viewWithCheckRegex.FindStringSubmatch(sqlInfo.stmt); view != nil {
			summaryMap["VIEW"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "Schema containing VIEW WITH CHECK OPTION is not supported yet.", "", "", "VIEW", view[1], sqlInfo.formattedStmt)
		}
	}
}

// Separates the input line into multiple statements which are accepted by YB.
func separateMultiObj(objType string, line string) string {
	indexes := multiRegex.FindAllStringSubmatchIndex(line, -1)
	suggestion := ""
	for _, match := range indexes {
		start := match[2]
		end := match[3]
		obj := strings.Replace(line[start:end], ",", ";", -1)
		suggestion += objType + " " + obj
	}
	return suggestion
}

// Checks compatibility of SQL statements
func checkSql(sqlInfoArr []sqlInfo, fpath string) {
	for _, sqlInfo := range sqlInfoArr {
		if rangeRegex.MatchString(sqlInfo.stmt) {
			reportCase(fpath,
				"RANGE with offset PRECEDING/FOLLOWING is not supported for column type numeric and offset type double precision",
				"https://github.com/yugabyte/yugabyte-db/issues/10692", "", "TABLE", "", sqlInfo.formattedStmt)
		} else if stmt := createConvRegex.FindStringSubmatch(sqlInfo.stmt); stmt != nil {
			reportCase(fpath, "CREATE CONVERSION not supported yet", "https://github.com/YugaByte/yugabyte-db/issues/10866", "", "CONVERSION", stmt[2], sqlInfo.formattedStmt)
		} else if stmt := alterConvRegex.FindStringSubmatch(sqlInfo.stmt); stmt != nil {
			reportCase(fpath, "ALTER CONVERSION not supported yet", "https://github.com/YugaByte/yugabyte-db/issues/10866", "", "CONVERSION", stmt[1], sqlInfo.formattedStmt)
		} else if stmt := fetchRegex.FindStringSubmatch(sqlInfo.stmt); stmt != nil {
			location := strings.ToUpper(stmt[1])
			if slices.Contains(notSupportedFetchLocation, location) {
				summaryMap["PROCEDURE"].invalidCount[sqlInfo.objName] = true
				reportCase(fpath, "This FETCH clause might not be supported yet", "https://github.com/YugaByte/yugabyte-db/issues/6514", "Please verify the DDL on your YugabyteDB version before proceeding", "CURSOR", sqlInfo.objName, sqlInfo.formattedStmt)
			}
		} else if stmt := alterAggRegex.FindStringSubmatch(sqlInfo.stmt); stmt != nil {
			reportCase(fpath, "ALTER AGGREGATE not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/2717", "", "AGGREGATE", stmt[1], sqlInfo.formattedStmt)
		} else if dropCollRegex.MatchString(sqlInfo.stmt) {
			reportCase(fpath, "DROP multiple objects not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/880", separateMultiObj("DROP COLLATION", sqlInfo.formattedStmt), "COLLATION", "", sqlInfo.formattedStmt)
		} else if dropIdxRegex.MatchString(sqlInfo.stmt) {
			reportCase(fpath, "DROP multiple objects not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/880", separateMultiObj("DROP INDEX", sqlInfo.formattedStmt), "INDEX", "", sqlInfo.formattedStmt)
		} else if dropViewRegex.MatchString(sqlInfo.stmt) {
			reportCase(fpath, "DROP multiple objects not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/880", separateMultiObj("DROP VIEW", sqlInfo.formattedStmt), "VIEW", "", sqlInfo.formattedStmt)
		} else if dropSeqRegex.MatchString(sqlInfo.stmt) {
			reportCase(fpath, "DROP multiple objects not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/880", separateMultiObj("DROP SEQUENCE", sqlInfo.formattedStmt), "SEQUENCE", "", sqlInfo.formattedStmt)
		} else if dropForeignRegex.MatchString(sqlInfo.stmt) {
			reportCase(fpath, "DROP multiple objects not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/880", separateMultiObj("DROP FOREIGN TABLE", sqlInfo.formattedStmt), "FOREIGN TABLE", "", sqlInfo.formattedStmt)
		} else if idx := dropIdxConcurRegex.FindStringSubmatch(sqlInfo.stmt); idx != nil {
			reportCase(fpath, "DROP INDEX CONCURRENTLY not supported yet",
				"", "", "INDEX", idx[2], sqlInfo.formattedStmt)
		} else if trig := trigRefRegex.FindStringSubmatch(sqlInfo.stmt); trig != nil {
			summaryMap["TRIGGER"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "REFERENCING clause (transition tables) not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1668", "", "TRIGGER", trig[1], sqlInfo.formattedStmt)
		} else if trig := constrTrgRegex.FindStringSubmatch(sqlInfo.stmt); trig != nil {
			reportCase(fpath, CONSTRAINT_TRIGGER_ISSUE_REASON,
				"https://github.com/YugaByte/yugabyte-db/issues/1709", "", "TRIGGER", trig[1], sqlInfo.formattedStmt)
		} else if currentOfRegex.MatchString(sqlInfo.stmt) {
			reportCase(fpath, "WHERE CURRENT OF not supported yet", "https://github.com/YugaByte/yugabyte-db/issues/737", "", "CURSOR", "", sqlInfo.formattedStmt)
		} else if bulkCollectRegex.MatchString(sqlInfo.stmt) {
			reportCase(fpath, "BULK COLLECT keyword of oracle is not converted into PostgreSQL compatible syntax", "", "", "", "", sqlInfo.formattedStmt)
		}
	}
}

// Checks unsupported DDL statements
func checkDDL(sqlInfoArr []sqlInfo, fpath string) {

	for _, sqlInfo := range sqlInfoArr {
		if am := amRegex.FindStringSubmatch(sqlInfo.stmt); am != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "CREATE ACCESS METHOD is not supported.",
				"https://github.com/yugabyte/yugabyte-db/issues/10693", "", "ACCESS METHOD", am[1], sqlInfo.formattedStmt)
		} else if tbl := idxConcRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "REINDEX is not supported.",
				"https://github.com/yugabyte/yugabyte-db/issues/10267", "", "TABLE", tbl[1], sqlInfo.formattedStmt)
		} else if col := storedRegex.FindStringSubmatch(sqlInfo.stmt); col != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			// fetch table name using different regex
			tbl := createTableRegex.FindStringSubmatch(sqlInfo.stmt)
			reportCase(fpath, STORED_GENERATED_COLUMN_ISSUE_REASON+fmt.Sprintf(" Column is: %s", col[1]),
				"https://github.com/yugabyte/yugabyte-db/issues/10695", "", "TABLE", tbl[2], sqlInfo.formattedStmt)
		} else if tbl := likeAllRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "LIKE ALL is not supported yet.",
				"https://github.com/yugabyte/yugabyte-db/issues/10697", "", "TABLE", tbl[2], sqlInfo.formattedStmt)
		} else if tbl := likeRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "LIKE clause not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1129", "", "TABLE", tbl[2], sqlInfo.formattedStmt)
		} else if tbl := tblPartitionRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			tblParts[tbl[2]] = tbl[3]
			if filename, ok := primaryCons[tbl[2]]; ok {
				reportAddingPrimaryKey(filename, tbl[2], sqlInfo.formattedStmt)
			}
		} else if tbl := addPrimaryRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			if _, ok := tblParts[tbl[3]]; ok {
				reportAddingPrimaryKey(fpath, tbl[2], sqlInfo.formattedStmt)
			}
			primaryCons[tbl[2]] = fpath
		} else if tbl := inheritRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, INHERITANCE_ISSUE_REASON,
				"https://github.com/YugaByte/yugabyte-db/issues/1129", "", "TABLE", tbl[4], sqlInfo.formattedStmt)
		} else if tbl := withOidsRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "OIDs are not supported for user tables.",
				"https://github.com/yugabyte/yugabyte-db/issues/10273", "", "TABLE", tbl[2], sqlInfo.formattedStmt)
		} else if tbl := intvlRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "PRIMARY KEY containing column of type 'INTERVAL' not yet supported.",
				"https://github.com/YugaByte/yugabyte-db/issues/1397", "", "TABLE", tbl[2], sqlInfo.formattedStmt)
		} else if tbl := alterOfRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE OF not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt)
		} else if tbl := alterSchemaRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE SET SCHEMA not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/3947", "", "TABLE", tbl[3], sqlInfo.formattedStmt)
		} else if createSchemaRegex.MatchString(sqlInfo.stmt) {
			reportCase(fpath, "CREATE SCHEMA with elements not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/10865", "", "SCHEMA", "", sqlInfo.formattedStmt)
		} else if tbl := alterNotOfRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE NOT OF not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt)
		} else if tbl := alterColumnStatsRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE ALTER column SET STATISTICS not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt)
		} else if tbl := alterColumnStorageRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE ALTER column SET STORAGE not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt)
		} else if tbl := alterColumnSetAttributesRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE ALTER column SET (attribute = value) not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt)
		} else if tbl := alterColumnResetAttributesRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE ALTER column RESET (attribute) not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt)
		} else if tbl := alterConstrRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE ALTER CONSTRAINT not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt)
		} else if tbl := setOidsRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE SET WITH OIDS not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[4], sqlInfo.formattedStmt)
		} else if tbl := withoutClusterRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE SET WITHOUT CLUSTER not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[2], sqlInfo.formattedStmt)
		} else if tbl := clusterRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE CLUSTER not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[2], sqlInfo.formattedStmt)
		} else if tbl := alterSetRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE SET not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[2], sqlInfo.formattedStmt)
		} else if tbl := alterIdxRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER INDEX SET not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "INDEX", tbl[1], sqlInfo.formattedStmt)
		} else if tbl := alterResetRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE RESET not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[2], sqlInfo.formattedStmt)
		} else if tbl := alterOptionsRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt)
		} else if typ := dropAttrRegex.FindStringSubmatch(sqlInfo.stmt); typ != nil {
			reportCase(fpath, "ALTER TYPE DROP ATTRIBUTE not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1893", "", "TYPE", typ[1], sqlInfo.formattedStmt)
		} else if typ := alterTypeRegex.FindStringSubmatch(sqlInfo.stmt); typ != nil {
			reportCase(fpath, "ALTER TYPE not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1893", "", "TYPE", typ[1], sqlInfo.formattedStmt)
		} else if tbl := alterInhRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE INHERIT not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt)
		} else if tbl := valConstrRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "ALTER TABLE VALIDATE CONSTRAINT not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt)
		} else if tbl := deferRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "DEFERRABLE unique constraints are not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1129", "", "TABLE", tbl[4], sqlInfo.formattedStmt)
		} else if spc := alterTblSpcRegex.FindStringSubmatch(sqlInfo.stmt); spc != nil {
			reportCase(fpath, "ALTER TABLESPACE not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1153", "", "TABLESPACE", spc[1], sqlInfo.formattedStmt)
		} else if spc := alterViewRegex.FindStringSubmatch(sqlInfo.stmt); spc != nil {
			reportCase(fpath, "ALTER VIEW not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1131", "", "VIEW", spc[1], sqlInfo.formattedStmt)
		} else if tbl := cLangRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "LANGUAGE C not supported yet.",
				"", "", "FUNCTION", tbl[2], sqlInfo.formattedStmt)
			summaryMap["FUNCTION"].invalidCount[sqlInfo.objName] = true
		} else if regMatch := partitionColumnsRegex.FindStringSubmatch(sqlInfo.stmt); regMatch != nil {
			// example1 - CREATE TABLE example1( 	id numeric NOT NULL, 	country_code varchar(3), 	record_type varchar(5), PRIMARY KEY (id,country_code) ) PARTITION BY RANGE (country_code, record_type) ;
			// example2 - CREATE TABLE example2 ( 	id numeric NOT NULL PRIMARY KEY, 	country_code varchar(3), 	record_type varchar(5) ) PARTITION BY RANGE (country_code, record_type) ;
			columnList := utils.CsvStringToSlice(strings.Trim(regMatch[3], " "))
			// example1 - columnList: [id numeric NOT NULL country_code varchar(3) record_type varchar(5) PRIMARY KEY (id country_code)]
			// example2 - columnList: [id numeric NOT NULL PRIMARY KEY country_code varchar(3) record_type varchar(5]
			openBracketSplits := strings.Split(strings.Trim(regMatch[3], " "), "(")
			// example1 -  openBracketSplits: [	id numeric NOT NULL, 	country_code varchar 3), 	record_type varchar 5), 	descriptions varchar 50), 	PRIMARY KEY  id,country_code)]
			stringbeforeLastOpenBracket := ""
			if len(openBracketSplits) > 1 {
				stringbeforeLastOpenBracket = strings.Join(strings.Fields(openBracketSplits[len(openBracketSplits)-2]), " ") //without extra spaces to easily check suffix
			}
			// example1 - stringbeforeLastBracket: 50), PRIMARY KEY
			// example2 - stringbeforeLastBracket: 5), descriptions varchar
			var primaryKeyColumnsList []string
			if strings.HasSuffix(strings.ToLower(stringbeforeLastOpenBracket), ", primary key") { //false for example2
				primaryKeyColumns := strings.Trim(openBracketSplits[len(openBracketSplits)-1], ") ")
				primaryKeyColumnsList = utils.CsvStringToSlice(primaryKeyColumns)
			} else {
				//this case can come by manual intervention
				for _, columnDefinition := range columnList {
					if strings.Contains(strings.ToLower(columnDefinition), "primary key") {
						partsOfColumnDefinition := strings.Split(columnDefinition, " ")
						columnName := partsOfColumnDefinition[0]
						primaryKeyColumnsList = append(primaryKeyColumnsList, columnName)
						break
					}
				}
			}
			partitionColumns := strings.Trim(regMatch[5], `()`)
			partitionColumnsList := utils.CsvStringToSlice(partitionColumns)
			if len(partitionColumnsList) == 1 {
				expressionChk := partitionColumnsList[0]
				if strings.ContainsAny(expressionChk, "()[]{}|/!@$#%^&*-+=") {
					summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
					reportCase(fpath, "Issue with Partition using Expression on a table which cannot contain Primary Key / Unique Key on any column",
						"https://github.com/yugabyte/yb-voyager/issues/698", "Remove the Constriant from the table definition", "TABLE", regMatch[2], sqlInfo.formattedStmt)
					continue
				}
			}
			if strings.ToLower(regMatch[4]) == "list" && len(partitionColumnsList) > 1 {
				summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
				reportCase(fpath, `cannot use "list" partition strategy with more than one column`,
					"https://github.com/yugabyte/yb-voyager/issues/699", "Make it a single column partition by list or choose other supported Partitioning methods", "TABLE", regMatch[2], sqlInfo.formattedStmt)
				continue
			}
			if len(primaryKeyColumnsList) == 0 { // if non-PK table, then no need to report
				continue
			}
			for _, partitionColumn := range partitionColumnsList {
				if !slices.Contains(primaryKeyColumnsList, partitionColumn) { //partition key not in PK
					summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
					reportCase(fpath, "insufficient columns in the PRIMARY KEY constraint definition in CREATE TABLE",
						"https://github.com/yugabyte/yb-voyager/issues/578", "Add all Partition columns to Primary Key", "TABLE", regMatch[2], sqlInfo.formattedStmt)
					break
				}
			}
		} else if strings.Contains(strings.ToLower(sqlInfo.stmt), "drop temporary table") {
			filePath := strings.Split(fpath, "/")
			fileName := filePath[len(filePath)-1]
			objType := strings.ToUpper(strings.Split(fileName, ".")[0])
			summaryMap[objType].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, `temporary table is not a supported clause for drop`,
				"https://github.com/yugabyte/yb-voyager/issues/705", `remove "temporary" and change it to "drop table"`, objType, sqlInfo.objName, sqlInfo.formattedStmt)
		} else if regMatch := anydataRegex.FindStringSubmatch(sqlInfo.stmt); regMatch != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "AnyData datatype doesn't have a mapping in YugabyteDB", "", `Remove the column with AnyData datatype or change it to a relevant supported datatype`, "TABLE", regMatch[2], sqlInfo.formattedStmt)
		} else if regMatch := anydatasetRegex.FindStringSubmatch(sqlInfo.stmt); regMatch != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "AnyDataSet datatype doesn't have a mapping in YugabyteDB", "", `Remove the column with AnyDataSet datatype or change it to a relevant supported datatype`, "TABLE", regMatch[2], sqlInfo.formattedStmt)
		} else if regMatch := anyTypeRegex.FindStringSubmatch(sqlInfo.stmt); regMatch != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "AnyType datatype doesn't have a mapping in YugabyteDB", "", `Remove the column with AnyType datatype or change it to a relevant supported datatype`, "TABLE", regMatch[2], sqlInfo.formattedStmt)
		} else if regMatch := uriTypeRegex.FindStringSubmatch(sqlInfo.stmt); regMatch != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "URIType datatype doesn't have a mapping in YugabyteDB", "", `Remove the column with URIType datatype or change it to a relevant supported datatype`, "TABLE", regMatch[2], sqlInfo.formattedStmt)
		} else if regMatch := jsonFuncRegex.FindStringSubmatch(sqlInfo.stmt); regMatch != nil {
			summaryMap[regMatch[2]].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "JSON_ARRAYAGG() function is not available in YugabyteDB", "", `Rename the function to YugabyteDB's equivalent JSON_AGG()`, regMatch[2], regMatch[3], sqlInfo.formattedStmt)
		}

	}
}

// check foreign table
func checkForeign(sqlInfoArr []sqlInfo, fpath string) {
	for _, sqlInfo := range sqlInfoArr {
		if tbl := primRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "Primary key constraints are not supported on foreign tables.",
				"https://github.com/yugabyte/yugabyte-db/issues/10698", "", "TABLE", tbl[1], sqlInfo.formattedStmt)
		} else if tbl := foreignKeyRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "Foreign key constraints are not supported on foreign tables.",
				"https://github.com/yugabyte/yugabyte-db/issues/10699", "", "TABLE", tbl[1], sqlInfo.formattedStmt)
		}
	}
}

// all other cases to check
func checkRemaining(sqlInfoArr []sqlInfo, fpath string) {
	for _, sqlInfo := range sqlInfoArr {
		if trig := compoundTrigRegex.FindStringSubmatch(sqlInfo.stmt); trig != nil {
			reportCase(fpath, COMPOUND_TRIGGER_ISSUE_REASON,
				"", "", "TRIGGER", trig[2], sqlInfo.formattedStmt)
			summaryMap["TRIGGER"].invalidCount[sqlInfo.objName] = true
		}
	}

}

// Checks whether the script, fpath, can be migrated to YB
func checker(sqlInfoArr []sqlInfo, fpath string) {
	if !utils.FileOrFolderExists(fpath) {
		return
	}
	checkViews(sqlInfoArr, fpath)
	checkSql(sqlInfoArr, fpath)
	checkGist(sqlInfoArr, fpath)
	checkGin(sqlInfoArr, fpath)
	checkDDL(sqlInfoArr, fpath)
	checkForeign(sqlInfoArr, fpath)
	checkRemaining(sqlInfoArr, fpath)
}

func checkExtensions(sqlInfoArr []sqlInfo, fpath string) {
	for _, sqlInfo := range sqlInfoArr {
		if sqlInfo.objName != "" && !slices.Contains(supportedExtensionsOnYB, sqlInfo.objName) {
			summaryMap["EXTENSION"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "This extension is not supported in YugabyteDB.", "", "", "EXTENSION",
				sqlInfo.objName, sqlInfo.formattedStmt)
		}
		if strings.ToLower(sqlInfo.objName) == "hll" {
			summaryMap["EXTENSION"].details[`'hll' extension is supported in YugabyteDB v2.18 onwards. Please verify this extension as per the target YugabyteDB version.`] = true
		}
	}
}

func getMapKeysString(receivedMap map[string]bool) string {
	keyString := strings.Join(lo.Keys(receivedMap), ", ")
	return keyString
}

func invalidSqlComment(line string) int {
	if cmt := unsupportedCommentRegex1.FindStringSubmatch(line); cmt != nil {
		return 1
	} else if cmt := packageSupportCommentRegex.FindStringSubmatch(line); cmt != nil {
		return 2
	} else if cmt := unsupportedCommentRegex2.FindStringSubmatch(line); cmt != nil {
		return 3
	} else if cmt := typeUnsupportedRegex.FindStringSubmatch(line); cmt != nil {
		return 4
	}
	return 0
}

func getCreateObjRegex(objType string) (*regexp.Regexp, int) {
	var createObjRegex *regexp.Regexp
	var objNameIndex int
	//replacing every possible space or new line char with [\s\n]+ in all regexs
	if objType == "MVIEW" {
		createObjRegex = re("CREATE", opt("OR REPLACE"), "MATERIALIZED", "VIEW", capture(ident))
		objNameIndex = 2
	} else if objType == "PACKAGE" {
		createObjRegex = re("CREATE", "SCHEMA", ifNotExists, capture(ident))
		objNameIndex = 2
	} else if objType == "SYNONYM" {
		createObjRegex = re("CREATE", opt("OR REPLACE"), "VIEW", capture(ident))
		objNameIndex = 2
	} else if objType == "INDEX" || objType == "PARTITION_INDEX" || objType == "FTS_INDEX" {
		createObjRegex = re("CREATE", opt("UNIQUE"), "INDEX", ifNotExists, capture(ident))
		objNameIndex = 3
	} else { //TODO: check syntaxes for other objects and add more cases if required
		createObjRegex = re("CREATE", opt("OR REPLACE"), objType, ifNotExists, capture(ident))
		objNameIndex = 3
	}

	return createObjRegex, objNameIndex
}

func processCollectedSql(fpath string, stmt string, formattedStmt string, objType string, reportNextSql *int) sqlInfo {
	createObjRegex, objNameIndex := getCreateObjRegex(objType)
	var objName = "" // to extract from sql statement

	//update about sqlStmt in the summary variable for the report generation part
	createObjStmt := createObjRegex.FindStringSubmatch(formattedStmt)
	if createObjStmt != nil {
		objName = createObjStmt[objNameIndex]

		if summaryMap != nil && summaryMap[objType] != nil { //when just createSqlStrArray() is called from someother file, then no summaryMap exists
			summaryMap[objType].totalCount += 1
			summaryMap[objType].objSet[objName] = true
		}
	}

	if *reportNextSql > 0 && (summaryMap != nil && summaryMap[objType] != nil) {
		reportBasedOnComment(*reportNextSql, fpath, "", "", objName, objType, formattedStmt)
		*reportNextSql = 0 //reset flag
	}

	formattedStmt = strings.TrimRight(formattedStmt, "\n") //removing new line from end

	sqlInfo := sqlInfo{
		objName:       objName,
		stmt:          stmt,
		formattedStmt: formattedStmt,
	}
	return sqlInfo
}

func parseSqlFileForObjectType(path string, objType string) []sqlInfo {
	log.Infof("Reading %s DDLs in file %s", objType, path)
	var sqlInfoArr []sqlInfo
	if !utils.FileOrFolderExists(path) {
		return sqlInfoArr
	}
	reportNextSql := 0
	file, err := os.ReadFile(path)
	if err != nil {
		utils.ErrExit("Error while reading %q: %s", path, err)
	}

	lines := strings.Split(string(file), "\n")
	for i := 0; i < len(lines); i++ {
		currLine := lines[i]
		if len(currLine) == 0 {
			continue
		}

		if strings.Contains(strings.TrimLeft(currLine, " "), "--") {
			reportNextSql = invalidSqlComment(currLine)
			continue
		}

		var stmt, formattedStmt string
		if isStartOfCodeBlockSqlStmt(currLine) {
			stmt, formattedStmt = collectSqlStmtContainingCode(lines, &i)
		} else {
			stmt, formattedStmt = collectSqlStmt(lines, &i)
		}
		sqlInfo := processCollectedSql(path, stmt, formattedStmt, objType, &reportNextSql)
		sqlInfoArr = append(sqlInfoArr, sqlInfo)
	}

	return sqlInfoArr
}

var reCreateProc, _ = getCreateObjRegex("PROCEDURE")
var reCreateFunc, _ = getCreateObjRegex("FUNCTION")
var reCreateTrigger, _ = getCreateObjRegex("TRIGGER")

// returns true when sql stmt is a CREATE statement for TRIGGER, FUNCTION, PROCEDURE
func isStartOfCodeBlockSqlStmt(line string) bool {
	return reCreateProc.MatchString(line) || reCreateFunc.MatchString(line) || reCreateTrigger.MatchString(line)
}

const (
	CODE_BLOCK_NOT_STARTED = 0
	CODE_BLOCK_STARTED     = 1
	CODE_BLOCK_COMPLETED   = 2
)

func collectSqlStmtContainingCode(lines []string, i *int) (string, string) {
	dollarQuoteFlag := CODE_BLOCK_NOT_STARTED
	// Delimiter to outermost Code Block if nested Code Blocks present
	codeBlockDelimiter := ""

	stmt := ""
	formattedStmt := ""

sqlParsingLoop:
	for ; *i < len(lines); *i++ {
		currLine := strings.TrimRight(lines[*i], " ")
		if len(currLine) == 0 {
			continue
		}

		stmt += currLine + " "
		formattedStmt += currLine + "\n"

		// Assuming that both the dollar quote strings will not be in same line
		switch dollarQuoteFlag {
		case CODE_BLOCK_NOT_STARTED:
			if isEndOfSqlStmt(currLine) { // in case, there is no body part or body part is in single line
				break sqlParsingLoop
			} else if matches := dollarQuoteRegex.FindStringSubmatch(currLine); matches != nil {
				dollarQuoteFlag = 1 //denotes start of the code/body part
				codeBlockDelimiter = matches[0]
			}
		case CODE_BLOCK_STARTED:
			if strings.Contains(currLine, codeBlockDelimiter) {
				dollarQuoteFlag = 2 //denotes end of code/body part
				if isEndOfSqlStmt(currLine) {
					break sqlParsingLoop
				}
			}
		case CODE_BLOCK_COMPLETED:
			if isEndOfSqlStmt(currLine) {
				break sqlParsingLoop
			}
		}
	}

	return stmt, formattedStmt
}

func collectSqlStmt(lines []string, i *int) (string, string) {
	stmt := ""
	formattedStmt := ""
	for ; *i < len(lines); *i++ {
		currLine := strings.TrimRight(lines[*i], " ")
		if len(currLine) == 0 {
			continue
		}

		stmt += currLine + " "
		formattedStmt += currLine + "\n"

		if isEndOfSqlStmt(currLine) {
			break
		}
	}
	return stmt, formattedStmt
}

func isEndOfSqlStmt(line string) bool {
	/*	checking for string with ending with `;`
		Also, cover cases like comment at the end after `;`
		example: "CREATE TABLE t1 (c1 int); -- table t1" */

	cmtStartIdx := strings.Index(line, "--")
	if cmtStartIdx != -1 {
		line = line[0:cmtStartIdx] // ignore comment
		line = strings.TrimRight(line, " ")
	}
	return strings.Contains(line, ";")
}

func initializeSummaryMap() {
	log.Infof("initializing report summary map")
	for _, objType := range sourceObjList {
		summaryMap[objType] = &summaryInfo{
			invalidCount: make(map[string]bool),
			objSet:       make(map[string]bool),
			details:      make(map[string]bool),
		}

		//executes only in case of oracle
		if objType == "PACKAGE" {
			summaryMap[objType].details["Packages in oracle are exported as schema, please review and edit them(if needed) to match your requirements"] = true
		} else if objType == "SYNONYM" {
			summaryMap[objType].details["Synonyms in oracle are exported as view, please review and edit them(if needed) to match your requirements"] = true
		}
	}

}

func generateHTMLReport(Report utils.SchemaReport) string {
	//appending to doc line by line for better readability

	//reading source db metainfo
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("generate html report: load migration status record: %s", err)
	}

	//Broad details
	htmlstring := "<html><body bgcolor='#EFEFEF'><h1>Database Migration Report</h1>"
	htmlstring += "<table><tr><th>Database Name</th><td>" + Report.SchemaSummary.DBName + "</td></tr>"
	htmlstring += "<tr><th>Schema Name(s)</th><td>" + strings.Join(Report.SchemaSummary.SchemaNames, ",") + "</td></tr>"
	htmlstring += "<tr><th>" + strings.ToUpper(msr.SourceDBConf.DBType) + " Version</th><td>" + Report.SchemaSummary.DBVersion + "</td></tr></table>"

	//Summary of report
	htmlstring += "<br><table width='100%' table-layout='fixed'><tr><th>Object</th><th>Total Count</th><th>Valid Count</th><th>Invalid Count</th><th width='40%'>Object Names</th><th width='30%'>Details</th></tr>"
	for i := 0; i < len(Report.SchemaSummary.DBObjects); i++ {
		if Report.SchemaSummary.DBObjects[i].TotalCount != 0 {
			htmlstring += "<tr><th>" + Report.SchemaSummary.DBObjects[i].ObjectType + "</th><td style='text-align: center;'>" + strconv.Itoa(Report.SchemaSummary.DBObjects[i].TotalCount) + "</td><td style='text-align: center;'>" + strconv.Itoa(Report.SchemaSummary.DBObjects[i].TotalCount-Report.SchemaSummary.DBObjects[i].InvalidCount) + "</td><td style='text-align: center;'>" + strconv.Itoa(Report.SchemaSummary.DBObjects[i].InvalidCount) + "</td><td width='40%'>" + Report.SchemaSummary.DBObjects[i].ObjectNames + "</td><td width='30%'>" + Report.SchemaSummary.DBObjects[i].Details + "</td></tr>"
		}
	}
	htmlstring += "</table><br>"

	//Issues/Error messages
	htmlstring += "<ul list-style-type='disc'>"
	for i := 0; i < len(Report.Issues); i++ {
		if Report.Issues[i].ObjectType != "" {
			htmlstring += "<li>Issue in Object " + Report.Issues[i].ObjectType + ":</li><ul>"
		} else {
			htmlstring += "<li>Issue " + Report.Issues[i].ObjectType + ":</li><ul>"
		}
		if Report.Issues[i].ObjectName != "" {
			htmlstring += "<li>Object Name: " + Report.Issues[i].ObjectName + "</li>"
		}
		if Report.Issues[i].Reason != "" {
			htmlstring += "<li>Reason: " + Report.Issues[i].Reason + "</li>"
		}
		if Report.Issues[i].SqlStatement != "" {
			htmlstring += "<li>SQL Statement: " + Report.Issues[i].SqlStatement + "</li>"
		}
		if Report.Issues[i].FilePath != "" {
			htmlstring += "<li>File Path: " + Report.Issues[i].FilePath + "<a href='" + Report.Issues[i].FilePath + "'> [Preview]</a></li>"
		}
		if Report.Issues[i].Suggestion != "" {
			htmlstring += "<li>Suggestion: " + Report.Issues[i].Suggestion + "</li>"
		}
		if Report.Issues[i].GH != "" {
			htmlstring += "<li><a href='" + Report.Issues[i].GH + "'>Github Issue Link</a></li>"
		}
		htmlstring += "</ul>"
	}
	htmlstring += "</ul>"
	if len(Report.SchemaSummary.Notes) > 0 {
		htmlstring += "<h3>Notes</h3>"
		htmlstring += "<ul list-style-type='disc'>"
		for i := 0; i < len(Report.SchemaSummary.Notes); i++ {
			htmlstring += "<li>" + Report.SchemaSummary.Notes[i] + "</li>"
		}
		htmlstring += "</ul>"
	}
	htmlstring += "</body></html>"
	return htmlstring

}

func generateTxtReport(Report utils.SchemaReport) string {
	txtstring := "+---------------------------+\n"
	txtstring += "| Database Migration Report |\n"
	txtstring += "+---------------------------+\n"
	txtstring += "Database Name\t" + Report.SchemaSummary.DBName + "\n"
	txtstring += "Schema Name(s)\t" + strings.Join(Report.SchemaSummary.SchemaNames, ",") + "\n"
	txtstring += "DB Version\t" + Report.SchemaSummary.DBVersion + "\n\n"
	txtstring += "Objects:\n\n"
	//if names for json objects need to be changed make sure to change the tab spaces accordingly as well.
	for i := 0; i < len(Report.SchemaSummary.DBObjects); i++ {
		if Report.SchemaSummary.DBObjects[i].TotalCount != 0 {
			txtstring += fmt.Sprintf("%-16s", "Object:") + Report.SchemaSummary.DBObjects[i].ObjectType + "\n"
			txtstring += fmt.Sprintf("%-16s", "Total Count:") + strconv.Itoa(Report.SchemaSummary.DBObjects[i].TotalCount) + "\n"
			txtstring += fmt.Sprintf("%-16s", "Valid Count:") + strconv.Itoa(Report.SchemaSummary.DBObjects[i].TotalCount-Report.SchemaSummary.DBObjects[i].InvalidCount) + "\n"
			txtstring += fmt.Sprintf("%-16s", "Invalid Count:") + strconv.Itoa(Report.SchemaSummary.DBObjects[i].InvalidCount) + "\n"
			txtstring += fmt.Sprintf("%-16s", "Object Names:") + Report.SchemaSummary.DBObjects[i].ObjectNames + "\n"
			if Report.SchemaSummary.DBObjects[i].Details != "" {
				txtstring += fmt.Sprintf("%-16s", "Details:") + Report.SchemaSummary.DBObjects[i].Details + "\n"
			}
			txtstring += "\n"
		}
	}
	if len(Report.Issues) != 0 {
		txtstring += "Issues:\n\n"
	}
	for i := 0; i < len(Report.Issues); i++ {
		txtstring += "Error in Object " + Report.Issues[i].ObjectType + ":\n"
		txtstring += "-Object Name: " + Report.Issues[i].ObjectName + "\n"
		txtstring += "-Reason: " + Report.Issues[i].Reason + "\n"
		txtstring += "-SQL Statement: " + Report.Issues[i].SqlStatement + "\n"
		txtstring += "-File Path: " + Report.Issues[i].FilePath + "\n"
		if Report.Issues[i].Suggestion != "" {
			txtstring += "-Suggestion: " + Report.Issues[i].Suggestion + "\n"
		}
		if Report.Issues[i].GH != "" {
			txtstring += "-Github Issue Link: " + Report.Issues[i].GH + "\n"
		}
		txtstring += "\n"
	}
	if len(Report.SchemaSummary.Notes) > 0 {
		txtstring += "Notes:\n\n"
		for i := 0; i < len(Report.SchemaSummary.Notes); i++ {
			txtstring += strconv.Itoa(i+1) + ". " + Report.SchemaSummary.Notes[i] + "\n"
		}
	}
	return txtstring
}

// add info to the 'reportStruct' variable and return
func analyzeSchemaInternal(sourceDBConf *srcdb.Source) utils.SchemaReport {
	/*
		NOTE: Don't create local var with name 'schemaAnalysisReport' since global one
		is used across all the internal functions called by analyzeSchemaInternal()
	*/
	schemaAnalysisReport = utils.SchemaReport{}
	sourceObjList = utils.GetSchemaObjectList(sourceDBConf.DBType)
	initializeSummaryMap()
	for _, objType := range sourceObjList {
		var sqlInfoArr []sqlInfo
		filePath := utils.GetObjectFilePath(schemaDir, objType)
		if objType != "INDEX" {
			sqlInfoArr = parseSqlFileForObjectType(filePath, objType)
		} else {
			sqlInfoArr = parseSqlFileForObjectType(filePath, objType)
			otherFPaths := utils.GetObjectFilePath(schemaDir, "PARTITION_INDEX")
			sqlInfoArr = append(sqlInfoArr, parseSqlFileForObjectType(otherFPaths, "PARTITION_INDEX")...)
			otherFPaths = utils.GetObjectFilePath(schemaDir, "FTS_INDEX")
			sqlInfoArr = append(sqlInfoArr, parseSqlFileForObjectType(otherFPaths, "FTS_INDEX")...)
		}
		if objType == "EXTENSION" {
			checkExtensions(sqlInfoArr, filePath)
		}
		checker(sqlInfoArr, filePath)
	}

	schemaAnalysisReport.SchemaSummary = reportSchemaSummary(sourceDBConf)
	return schemaAnalysisReport
}

func analyzeSchema() {
	err := retrieveMigrationUUID()
	if err != nil {
		utils.ErrExit("failed to get migration UUID: %w", err)
	}
	reportFile := "schema_analysis_report." + analyzeSchemaReportFormat

	schemaAnalysisStartedEvent := createSchemaAnalysisStartedEvent()
	controlPlane.SchemaAnalysisStarted(&schemaAnalysisStartedEvent)

	reportPath := filepath.Join(exportDir, "reports", reportFile)

	if !schemaIsExported() {
		utils.ErrExit("run export schema before running analyze-schema")
	}

	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("analyze schema : load migration status record: %s", err)
	}
	analyzeSchemaInternal(msr.SourceDBConf)

	var finalReport string

	switch analyzeSchemaReportFormat {
	case "html":
		htmlReport := generateHTMLReport(schemaAnalysisReport)
		finalReport = utils.PrettifyHtmlString(htmlReport)
	case "json":
		jsonBytes, err := json.Marshal(schemaAnalysisReport)
		if err != nil {
			panic(err)
		}
		reportJsonString := string(jsonBytes)
		finalReport = utils.PrettifyJsonString(reportJsonString)
	case "txt":
		finalReport = generateTxtReport(schemaAnalysisReport)
	case "xml":
		byteReport, _ := xml.MarshalIndent(schemaAnalysisReport, "", "\t")
		finalReport = string(byteReport)
	default:
		panic(fmt.Sprintf("invalid report format: %q", analyzeSchemaReportFormat))
	}

	//check & inform if file already exists
	if utils.FileOrFolderExists(reportPath) {
		fmt.Printf("\n%s already exists, overwriting it with a new generated report\n", reportFile)
	}

	file, err := os.OpenFile(reportPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		utils.ErrExit("Error while opening %q: %s", reportPath, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Errorf("Error while closing file %s: %v", reportPath, err)
		}
	}()

	_, err = file.WriteString(finalReport)
	if err != nil {
		utils.ErrExit("failed to write report to %q: %s", reportPath, err)
	}
	fmt.Printf("-- find schema analysis report at: %s\n", reportPath)

	payload := callhome.GetPayload(exportDir, migrationUUID)
	var callhomeIssues []utils.Issue
	for _, issue := range schemaAnalysisReport.Issues {
		issue.SqlStatement = "" // Obfuscate sensitive information before sending to callhome cluster
		callhomeIssues = append(callhomeIssues, issue)
	}
	issues, err := json.Marshal(callhomeIssues)
	if err != nil {
		log.Errorf("Error while parsing 'issues' json: %v", err)
	} else {
		payload.Issues = string(issues)
	}
	dbobjects, err := json.Marshal(schemaAnalysisReport.SchemaSummary.DBObjects)
	if err != nil {
		log.Errorf("Error while parsing 'database_objects' json: %v", err)
	} else {
		payload.DBObjects = string(dbobjects)
	}

	callhome.PackAndSendPayload(exportDir)

	schemaAnalysisReport := createSchemaAnalysisIterationCompletedEvent(schemaAnalysisReport)
	controlPlane.SchemaAnalysisIterationCompleted(&schemaAnalysisReport)
}

var analyzeSchemaCmd = &cobra.Command{
	Use: "analyze-schema",
	Short: "Analyze converted source database schema and generate a report about YB incompatible constructs.\n" +
		"For more details and examples, visit https://docs.yugabyte.com/preview/yugabyte-voyager/reference/schema-migration/analyze-schema/",
	Long: ``,
	PreRun: func(cmd *cobra.Command, args []string) {
		validOutputFormats := []string{"html", "json", "txt", "xml"}
		validateReportOutputFormat(validOutputFormats, analyzeSchemaReportFormat)
	},

	Run: func(cmd *cobra.Command, args []string) {
		analyzeSchema()
	},
}

func init() {
	rootCmd.AddCommand(analyzeSchemaCmd)
	registerCommonGlobalFlags(analyzeSchemaCmd)
	analyzeSchemaCmd.PersistentFlags().StringVar(&analyzeSchemaReportFormat, "output-format", "txt",
		"format in which report will be generated: (html, txt, json, xml)")
}

func validateReportOutputFormat(validOutputFormats []string, format string) {
	format = strings.ToLower(format)

	for i := 0; i < len(validOutputFormats); i++ {
		if format == validOutputFormats[i] {
			return
		}
	}
	utils.ErrExit("Error: Invalid output format: %s. Supported formats are %v", format, validOutputFormats)
}

func schemaIsAnalyzed() bool {
	path := filepath.Join(exportDir, "reports", "schema_analysis_report.*")
	return utils.FileOrFolderExistsWithGlobPattern(path)
}

func createSchemaAnalysisStartedEvent() cp.SchemaAnalysisStartedEvent {
	result := cp.SchemaAnalysisStartedEvent{}
	initBaseSourceEvent(&result.BaseEvent, "ANALYZE SCHEMA")
	return result
}

func createSchemaAnalysisIterationCompletedEvent(report utils.SchemaReport) cp.SchemaAnalysisIterationCompletedEvent {
	result := cp.SchemaAnalysisIterationCompletedEvent{}
	initBaseSourceEvent(&result.BaseEvent, "ANALYZE SCHEMA")
	result.AnalysisReport = report
	return result
}
