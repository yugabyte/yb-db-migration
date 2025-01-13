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
	"bytes"
	_ "embed"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/constants"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/cp"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/metadb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/query/queryissue"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/query/queryparser"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/srcdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/ybversion"
)

type summaryInfo struct {
	totalCount   int
	invalidCount map[string]bool
	objSet       []string        //TODO: fix it with the set and proper names for INDEX, TRIGGER and POLICY types as "obj_name on table_name"
	details      map[string]bool //any details about the object type
}

type sqlInfo struct {
	objName string
	// SQL statement after removing all new-lines from it. Simplifies analyze-schema regex matching.
	stmt string
	// Formatted SQL statement with new-lines and tabs
	formattedStmt string
	fileName      string
}

var (
	anything   = `.*`
	ws         = `[\s\n\t]+`
	optionalWS = `[\s\n\t]*` //optional white spaces
	//TODO: fix this ident regex for the proper PG identifiers syntax - refer: https://github.com/yugabyte/yb-voyager/pull/1547#discussion_r1629282309
	ident                   = `[a-zA-Z0-9_."-]+`
	operatorIdent           = `[a-zA-Z0-9_."-+*/<>=~!@#%^&|` + "`" + `]+` // refer https://www.postgresql.org/docs/current/sql-createoperator.html
	ifExists                = opt("IF", "EXISTS")
	ifNotExists             = opt("IF", "NOT", "EXISTS")
	commaSeperatedTokens    = `[^,]+(?:,[^,]+){1,}`
	unqualifiedIdent        = `[a-zA-Z0-9_]+`
	commonClause            = `[a-zA-Z]+`
	supportedExtensionsOnYB = []string{
		"adminpack", "amcheck", "autoinc", "bloom", "btree_gin", "btree_gist", "citext", "cube",
		"dblink", "dict_int", "dict_xsyn", "earthdistance", "file_fdw", "fuzzystrmatch", "hll", "hstore",
		"hypopg", "insert_username", "intagg", "intarray", "isn", "lo", "ltree", "moddatetime",
		"orafce", "pageinspect", "pg_buffercache", "pg_cron", "pg_freespacemap", "pg_hint_plan", "pg_prewarm", "pg_stat_monitor",
		"pg_stat_statements", "pg_trgm", "pg_visibility", "pgaudit", "pgcrypto", "pgrowlocks", "pgstattuple", "plpgsql",
		"postgres_fdw", "refint", "seg", "sslinfo", "tablefunc", "tcn", "timetravel", "tsm_system_rows",
		"tsm_system_time", "unaccent", `"uuid-ossp"`, "yb_pg_metrics", "yb_test_extension",
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

//commenting it as currently not used.
// func parenth(s string) string {
// 	return `\(` + s + `\)`
// }

var (
	analyzeSchemaReportFormat string
	sourceObjList             []string
	schemaAnalysisReport      utils.SchemaReport
	partitionTablesMap        = make(map[string]bool)
	// key is partitioned table, value is sqlInfo (sqlstmt, fpath) where the ADD PRIMARY KEY statement resides
	summaryMap          = make(map[string]*summaryInfo)
	parserIssueDetector = queryissue.NewParserIssueDetector()
	multiRegex          = regexp.MustCompile(`([a-zA-Z0-9_\.]+[,|;])`)
	dollarQuoteRegex    = regexp.MustCompile(`(\$.*\$)`)
	//TODO: optional but replace every possible space or new line char with [\s\n]+ in all regexs
	viewWithCheckRegex        = re("VIEW", capture(ident), anything, "WITH", opt(commonClause), "CHECK", "OPTION")
	rangeRegex                = re("PRECEDING", "and", anything, ":float")
	fetchRegex                = re("FETCH", capture(commonClause), "FROM")
	notSupportedFetchLocation = []string{"FIRST", "LAST", "NEXT", "PRIOR", "RELATIVE", "ABSOLUTE", "NEXT", "FORWARD", "BACKWARD"}
	alterAggRegex             = re("ALTER", "AGGREGATE", capture(ident))
	dropCollRegex             = re("DROP", "COLLATION", ifExists, capture(commaSeperatedTokens))
	dropIdxRegex              = re("DROP", "INDEX", ifExists, capture(commaSeperatedTokens))
	dropViewRegex             = re("DROP", "VIEW", ifExists, capture(commaSeperatedTokens))
	dropSeqRegex              = re("DROP", "SEQUENCE", ifExists, capture(commaSeperatedTokens))
	dropForeignRegex          = re("DROP", "FOREIGN", "TABLE", ifExists, capture(commaSeperatedTokens))
	dropIdxConcurRegex        = re("DROP", "INDEX", "CONCURRENTLY", ifExists, capture(ident))
	currentOfRegex            = re("WHERE", "CURRENT", "OF")
	amRegex                   = re("CREATE", "ACCESS", "METHOD", capture(ident))
	idxConcRegex              = re("REINDEX", anything, capture(ident))
	likeAllRegex              = re("CREATE", "TABLE", ifNotExists, capture(ident), anything, "LIKE", anything, "INCLUDING ALL")
	likeRegex                 = re("CREATE", "TABLE", ifNotExists, capture(ident), anything, `\(LIKE`)
	withOidsRegex             = re("CREATE", "TABLE", ifNotExists, capture(ident), anything, "WITH", anything, "OIDS")
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
	alterColumnResetAttributesRegex = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), anything, "ALTER", "COLUMN", capture(ident), anything, "RESET", anything)
	alterConstrRegex                = re("ALTER", opt(capture(unqualifiedIdent)), ifExists, "TABLE", capture(ident), anything, "ALTER", "CONSTRAINT")
	setOidsRegex                    = re("ALTER", opt(capture(unqualifiedIdent)), "TABLE", ifExists, capture(ident), anything, "SET WITH OIDS")
	withoutClusterRegex             = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), anything, "SET WITHOUT CLUSTER")
	alterSetRegex                   = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), "SET")
	alterIdxRegex                   = re("ALTER", "INDEX", capture(ident), "SET")
	alterResetRegex                 = re("ALTER", "TABLE", opt("ONLY"), ifExists, capture(ident), "RESET")
	alterOptionsRegex               = re("ALTER", opt(capture(unqualifiedIdent)), "TABLE", ifExists, capture(ident), "OPTIONS")
	alterInhRegex                   = re("ALTER", opt(capture(unqualifiedIdent)), "TABLE", ifExists, capture(ident), "INHERIT")
	valConstrRegex                  = re("ALTER", opt(capture(unqualifiedIdent)), "TABLE", ifExists, capture(ident), "VALIDATE CONSTRAINT")
	alterViewRegex                  = re("ALTER", "VIEW", capture(ident))
	dropAttrRegex                   = re("ALTER", "TYPE", capture(ident), "DROP ATTRIBUTE")
	alterTypeRegex                  = re("ALTER", "TYPE", capture(ident))
	alterTblSpcRegex                = re("ALTER", "TABLESPACE", capture(ident), "SET")

	primRegex       = re("CREATE", "FOREIGN", "TABLE", capture(ident)+`\(`, anything, "PRIMARY KEY")
	foreignKeyRegex = re("CREATE", "FOREIGN", "TABLE", capture(ident)+`\(`, anything, "REFERENCES", anything)

	// unsupported SQLs exported by ora2pg
	compoundTrigRegex          = re("CREATE", opt("OR REPLACE"), "TRIGGER", capture(ident), anything, "COMPOUND", anything)
	unsupportedCommentRegex1   = re("--", anything, "(unsupported)")
	packageSupportCommentRegex = re("--", anything, "Oracle package ", "'"+capture(ident)+"'", anything, "please edit to match PostgreSQL syntax")
	unsupportedCommentRegex2   = re("--", anything, "please edit to match PostgreSQL syntax")
	typeUnsupportedRegex       = re("Inherited types are not supported", anything, "replacing with inherited table")
	bulkCollectRegex           = re("BULK COLLECT") // ora2pg unable to convert this oracle feature into a PostgreSQL compatible syntax
	jsonFuncRegex              = re("CREATE", opt("OR REPLACE"), capture(unqualifiedIdent), capture(ident), anything, "JSON_ARRAYAGG")
	alterConvRegex             = re("ALTER", "CONVERSION", capture(ident), anything)
)

const (
	// Issues detected using regexp, reported in assessment and analyze both
	CONVERSION_ISSUE_REASON     = "CREATE CONVERSION is not supported yet"
	UNSUPPORTED_EXTENSION_ISSUE = "This extension is not supported in YugabyteDB by default."
	VIEW_CHECK_OPTION_ISSUE     = "Schema containing VIEW WITH CHECK OPTION is not supported yet."

	// Refactor: constants below used in some comparisions (use Issue Type there and remove these)
	INHERITANCE_ISSUE_REASON                    = "TABLE INHERITANCE not supported in YugabyteDB"
	ADDING_PK_TO_PARTITIONED_TABLE_ISSUE_REASON = "Adding primary key to a partitioned table is not supported yet."
	COMPOUND_TRIGGER_ISSUE_REASON               = "COMPOUND TRIGGER not supported in YugabyteDB."
	STORED_GENERATED_COLUMN_ISSUE_REASON        = "Stored generated columns are not supported."
	FOREIGN_TABLE_ISSUE_REASON                  = "Foreign tables require manual intervention."
	DEFERRABLE_CONSTRAINT_ISSUE                 = "DEFERRABLE constraints not supported yet"
	POLICY_ROLE_ISSUE                           = "Policy require roles to be created."
	ISSUE_INDEX_WITH_COMPLEX_DATATYPES          = `INDEX on column '%s' not yet supported`
	INDEX_METHOD_ISSUE_REASON                   = "Schema contains %s index which is not supported."

	UNSUPPORTED_DATATYPE                           = "Unsupported datatype"
	UNSUPPORTED_DATATYPE_LIVE_MIGRATION            = "Unsupported datatype for Live migration"
	UNSUPPORTED_DATATYPE_LIVE_MIGRATION_WITH_FF_FB = "Unsupported datatype for Live migration with fall-forward/fallback"
	UNSUPPORTED_PG_SYNTAX                          = "Unsupported PG syntax"

	INSUFFICIENT_COLUMNS_IN_PK_FOR_PARTITION = "insufficient columns in the PRIMARY KEY constraint definition in CREATE TABLE"
	GIN_INDEX_DETAILS                        = "There are some GIN indexes present in the schema, but GIN indexes are partially supported in YugabyteDB as mentioned in (https://github.com/yugabyte/yugabyte-db/issues/7850) so take a look and modify them if not supported."
)

// Reports one case in JSON
func reportCase(filePath string, reason string, ghIssue string, suggestion string, objType string, objName string, sqlStmt string, category string, docsLink string, impact string) {
	var issue utils.AnalyzeSchemaIssue
	issue.FilePath = filePath
	issue.Reason = reason
	issue.GH = ghIssue
	issue.Suggestion = suggestion
	issue.ObjectType = objType
	issue.ObjectName = objName
	issue.SqlStatement = sqlStmt
	issue.IssueType = category // IssueType field of analyze schema should be renamed to Category
	issue.Impact = lo.Ternary(impact != "", impact, constants.IMPACT_LEVEL_1)
	if sourceDBType == POSTGRESQL {
		issue.DocsLink = docsLink
	}

	schemaAnalysisReport.Issues = append(schemaAnalysisReport.Issues, issue)
}

func reportBasedOnComment(comment int, fpath string, issue string, suggestion string, objName string, objType string, line string) {
	if comment == 1 {
		reportCase(fpath, "Unsupported, please edit to match PostgreSQL syntax", "https://github.com/yugabyte/yb-voyager/issues/1625", suggestion, objType, objName, line, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		summaryMap[objType].invalidCount[objName] = true
	} else if comment == 2 {
		// reportCase(fpath, "PACKAGE in oracle are exported as Schema, please review and edit to match PostgreSQL syntax if required, Package is "+objName, issue, suggestion, objType, "")
		summaryMap["PACKAGE"].objSet = append(summaryMap["PACKAGE"].objSet, objName)
	} else if comment == 3 {
		reportCase(fpath, "SQLs in file might be unsupported please review and edit to match PostgreSQL syntax if required. ", "https://github.com/yugabyte/yb-voyager/issues/1625", suggestion, objType, objName, line, UNSUPPORTED_FEATURES_CATEGORY, "", "")
	} else if comment == 4 {
		summaryMap[objType].details["Inherited Types are present which are not supported in PostgreSQL syntax, so exported as Inherited Tables"] = true
	}

}

// return summary about the schema objects like tables, indexes, functions, sequences
func reportSchemaSummary(sourceDBConf *srcdb.Source) utils.SchemaSummary {
	var schemaSummary utils.SchemaSummary

	schemaSummary.Description = SCHEMA_SUMMARY_DESCRIPTION
	if sourceDBConf.DBType == ORACLE {
		schemaSummary.Description = SCHEMA_SUMMARY_DESCRIPTION_ORACLE
	}
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
		dbObject.ObjectNames = strings.Join(summaryMap[objType].objSet, ", ")
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
	exportedIndexes := summaryMap["INDEX"].objSet
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

func checkStmtsUsingParser(sqlInfoArr []sqlInfo, fpath string, objType string) {
	for _, sqlStmtInfo := range sqlInfoArr {
		_, err := queryparser.Parse(sqlStmtInfo.stmt)
		if err != nil { //if the Stmt is not already report by any of the regexes
			if !summaryMap[objType].invalidCount[sqlStmtInfo.objName] {
				reason := fmt.Sprintf("%s - '%s'", UNSUPPORTED_PG_SYNTAX, err.Error())
				reportCase(fpath, reason, "https://github.com/yugabyte/yb-voyager/issues/1625",
					"Fix the schema as per PG syntax", objType, sqlStmtInfo.objName, sqlStmtInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
			}
			continue
		}
		err = parserIssueDetector.ParseRequiredDDLs(sqlStmtInfo.formattedStmt)
		if err != nil {
			utils.ErrExit("error parsing stmt: [%s]: %v", sqlStmtInfo.formattedStmt, err)
		}
		if parserIssueDetector.IsGinIndexPresentInSchema {
			summaryMap["INDEX"].details[GIN_INDEX_DETAILS] = true
		}
		ddlIssues, err := parserIssueDetector.GetDDLIssues(sqlStmtInfo.formattedStmt, targetDbVersion)
		if err != nil {
			utils.ErrExit("error getting ddl issues for stmt: [%s]: %v", sqlStmtInfo.formattedStmt, err)
		}
		for _, i := range ddlIssues {
			schemaAnalysisReport.Issues = append(schemaAnalysisReport.Issues, convertIssueInstanceToAnalyzeIssue(i, fpath, false))
		}
	}
}

// Checks compatibility of views
func checkViews(sqlInfoArr []sqlInfo, fpath string) {
	for _, sqlInfo := range sqlInfoArr {
		if view := viewWithCheckRegex.FindStringSubmatch(sqlInfo.stmt); view != nil {
			summaryMap["VIEW"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, VIEW_CHECK_OPTION_ISSUE, "https://github.com/yugabyte/yugabyte-db/issues/22716",
				"Use Trigger with INSTEAD OF clause on INSERT/UPDATE on view to get this functionality", "VIEW", view[1], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, VIEW_CHECK_OPTION_DOC_LINK, "")
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
				"https://github.com/yugabyte/yugabyte-db/issues/10692", "", "TABLE", "", sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
		} else if stmt := fetchRegex.FindStringSubmatch(sqlInfo.stmt); stmt != nil {
			location := strings.ToUpper(stmt[1])
			if slices.Contains(notSupportedFetchLocation, location) {
				summaryMap["PROCEDURE"].invalidCount[sqlInfo.objName] = true
				reportCase(fpath, "This FETCH clause might not be supported yet", "https://github.com/YugaByte/yugabyte-db/issues/6514",
					"Please verify the DDL on your YugabyteDB version before proceeding", "CURSOR", sqlInfo.objName, sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
			}
		} else if stmt := alterAggRegex.FindStringSubmatch(sqlInfo.stmt); stmt != nil {
			summaryMap["AGGREGATE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER AGGREGATE not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/2717", "", "AGGREGATE", stmt[1], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if dropCollRegex.MatchString(sqlInfo.stmt) {
			summaryMap["COLLATION"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "DROP multiple objects not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/880", separateMultiObj("DROP COLLATION", sqlInfo.formattedStmt), "COLLATION", "", sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if dropIdxRegex.MatchString(sqlInfo.stmt) {
			summaryMap["INDEX"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "DROP multiple objects not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/880", separateMultiObj("DROP INDEX", sqlInfo.formattedStmt), "INDEX", "", sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if dropViewRegex.MatchString(sqlInfo.stmt) {
			summaryMap["VIEW"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "DROP multiple objects not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/880", separateMultiObj("DROP VIEW", sqlInfo.formattedStmt), "VIEW", "", sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if dropSeqRegex.MatchString(sqlInfo.stmt) {
			summaryMap["SEQUENCE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "DROP multiple objects not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/880", separateMultiObj("DROP SEQUENCE", sqlInfo.formattedStmt), "SEQUENCE", "", sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if dropForeignRegex.MatchString(sqlInfo.stmt) {
			summaryMap["FOREIGN TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "DROP multiple objects not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/880", separateMultiObj("DROP FOREIGN TABLE", sqlInfo.formattedStmt), "FOREIGN TABLE", "", sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if idx := dropIdxConcurRegex.FindStringSubmatch(sqlInfo.stmt); idx != nil {
			summaryMap["INDEX"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "DROP INDEX CONCURRENTLY not supported yet",
				"https://github.com/yugabyte/yugabyte-db/issues/22717", "", "INDEX", idx[2], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if currentOfRegex.MatchString(sqlInfo.stmt) {
			reportCase(fpath, "WHERE CURRENT OF not supported yet", "https://github.com/YugaByte/yugabyte-db/issues/737", "", "CURSOR", "", sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if bulkCollectRegex.MatchString(sqlInfo.stmt) {
			reportCase(fpath, "BULK COLLECT keyword of oracle is not converted into PostgreSQL compatible syntax", "https://github.com/yugabyte/yb-voyager/issues/1539", "", "", "", sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		}
	}
}

// Checks unsupported DDL statements
func checkDDL(sqlInfoArr []sqlInfo, fpath string, objType string) {

	for _, sqlInfo := range sqlInfoArr {
		if am := amRegex.FindStringSubmatch(sqlInfo.stmt); am != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "CREATE ACCESS METHOD is not supported.",
				"https://github.com/yugabyte/yugabyte-db/issues/10693", "", "ACCESS METHOD", am[1], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := idxConcRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "REINDEX is not supported.",
				"https://github.com/yugabyte/yugabyte-db/issues/10267", "", "TABLE", tbl[1], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := likeAllRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "LIKE ALL is not supported yet.",
				"https://github.com/yugabyte/yugabyte-db/issues/10697", "", "TABLE", tbl[2], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := likeRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "LIKE clause not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1129", "", "TABLE", tbl[2], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := withOidsRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "OIDs are not supported for user tables.",
				"https://github.com/yugabyte/yugabyte-db/issues/10273", "", "TABLE", tbl[2], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := alterOfRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE OF not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := alterSchemaRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE SET SCHEMA not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/3947", "", "TABLE", tbl[3], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if createSchemaRegex.MatchString(sqlInfo.stmt) {
			summaryMap["SCHEMA"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "CREATE SCHEMA with elements not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/10865", "", "SCHEMA", "", sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := alterNotOfRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE NOT OF not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := alterColumnStatsRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE ALTER column SET STATISTICS not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := alterColumnStorageRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE ALTER column SET STORAGE not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := alterColumnResetAttributesRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE ALTER column RESET (attribute) not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := alterConstrRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE ALTER CONSTRAINT not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := setOidsRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE SET WITH OIDS not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[4], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := withoutClusterRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE SET WITHOUT CLUSTER not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[2], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := alterSetRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE SET not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[2], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := alterIdxRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER INDEX SET not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "INDEX", tbl[1], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := alterResetRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE RESET not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[2], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := alterOptionsRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if typ := dropAttrRegex.FindStringSubmatch(sqlInfo.stmt); typ != nil {
			summaryMap["TYPE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TYPE DROP ATTRIBUTE not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1893", "", "TYPE", typ[1], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if typ := alterTypeRegex.FindStringSubmatch(sqlInfo.stmt); typ != nil {
			summaryMap["TYPE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TYPE not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1893", "", "TYPE", typ[1], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := alterInhRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE INHERIT not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := valConstrRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLE VALIDATE CONSTRAINT not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1124", "", "TABLE", tbl[3], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if spc := alterTblSpcRegex.FindStringSubmatch(sqlInfo.stmt); spc != nil {
			summaryMap["TABLESPACE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER TABLESPACE not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1153", "", "TABLESPACE", spc[1], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if spc := alterViewRegex.FindStringSubmatch(sqlInfo.stmt); spc != nil {
			summaryMap["VIEW"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "ALTER VIEW not supported yet.",
				"https://github.com/YugaByte/yugabyte-db/issues/1131", "", "VIEW", spc[1], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := cLangRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			reportCase(fpath, "LANGUAGE C not supported yet.",
				"https://github.com/yugabyte/yb-voyager/issues/1540", "", "FUNCTION", tbl[2], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
			summaryMap["FUNCTION"].invalidCount[sqlInfo.objName] = true
		} else if strings.Contains(strings.ToLower(sqlInfo.stmt), "drop temporary table") {
			filePath := strings.Split(fpath, "/")
			fileName := filePath[len(filePath)-1]
			objType := strings.ToUpper(strings.Split(fileName, ".")[0])
			summaryMap[objType].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, `temporary table is not a supported clause for drop`,
				"https://github.com/yugabyte/yb-voyager/issues/705", `remove "temporary" and change it to "drop table"`, objType, sqlInfo.objName, sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, DROP_TEMP_TABLE_DOC_LINK, "")
		} else if regMatch := anydataRegex.FindStringSubmatch(sqlInfo.stmt); regMatch != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "AnyData datatype doesn't have a mapping in YugabyteDB", "https://github.com/yugabyte/yb-voyager/issues/1541", `Remove the column with AnyData datatype or change it to a relevant supported datatype`, "TABLE", regMatch[2], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if regMatch := anydatasetRegex.FindStringSubmatch(sqlInfo.stmt); regMatch != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "AnyDataSet datatype doesn't have a mapping in YugabyteDB", "https://github.com/yugabyte/yb-voyager/issues/1541", `Remove the column with AnyDataSet datatype or change it to a relevant supported datatype`, "TABLE", regMatch[2], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if regMatch := anyTypeRegex.FindStringSubmatch(sqlInfo.stmt); regMatch != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "AnyType datatype doesn't have a mapping in YugabyteDB", "https://github.com/yugabyte/yb-voyager/issues/1541", `Remove the column with AnyType datatype or change it to a relevant supported datatype`, "TABLE", regMatch[2], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if regMatch := uriTypeRegex.FindStringSubmatch(sqlInfo.stmt); regMatch != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "URIType datatype doesn't have a mapping in YugabyteDB", "https://github.com/yugabyte/yb-voyager/issues/1541", `Remove the column with URIType datatype or change it to a relevant supported datatype`, "TABLE", regMatch[2], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if regMatch := jsonFuncRegex.FindStringSubmatch(sqlInfo.stmt); regMatch != nil {
			summaryMap[objType].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "JSON_ARRAYAGG() function is not available in YugabyteDB", "https://github.com/yugabyte/yb-voyager/issues/1542", `Rename the function to YugabyteDB's equivalent JSON_AGG()`, objType, regMatch[3], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		}

	}
}

// check foreign table
func checkForeign(sqlInfoArr []sqlInfo, fpath string) {
	for _, sqlInfo := range sqlInfoArr {
		//TODO: refactor it later to remove all the unneccessary regexes
		if tbl := primRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "Primary key constraints are not supported on foreign tables.",
				"https://github.com/yugabyte/yugabyte-db/issues/10698", "", "TABLE", tbl[1], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		} else if tbl := foreignKeyRegex.FindStringSubmatch(sqlInfo.stmt); tbl != nil {
			summaryMap["TABLE"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, "Foreign key constraints are not supported on foreign tables.",
				"https://github.com/yugabyte/yugabyte-db/issues/10699", "", "TABLE", tbl[1], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
		}
	}
}

// all other cases to check
func checkRemaining(sqlInfoArr []sqlInfo, fpath string) {
	for _, sqlInfo := range sqlInfoArr {
		if trig := compoundTrigRegex.FindStringSubmatch(sqlInfo.stmt); trig != nil {
			reportCase(fpath, COMPOUND_TRIGGER_ISSUE_REASON,
				"https://github.com/yugabyte/yb-voyager/issues/1543", "", "TRIGGER", trig[2], sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, "", "")
			summaryMap["TRIGGER"].invalidCount[sqlInfo.objName] = true
		}
	}

}

// Checks whether the script, fpath, can be migrated to YB
func checker(sqlInfoArr []sqlInfo, fpath string, objType string) {
	if !utils.FileOrFolderExists(fpath) {
		return
	}
	checkViews(sqlInfoArr, fpath)
	checkSql(sqlInfoArr, fpath)
	checkDDL(sqlInfoArr, fpath, objType)
	checkForeign(sqlInfoArr, fpath)
	checkRemaining(sqlInfoArr, fpath)
	checkStmtsUsingParser(sqlInfoArr, fpath, objType)
	if utils.GetEnvAsBool("REPORT_UNSUPPORTED_PLPGSQL_OBJECTS", true) {
		checkPlPgSQLStmtsUsingParser(sqlInfoArr, fpath, objType)
	}
}

func checkPlPgSQLStmtsUsingParser(sqlInfoArr []sqlInfo, fpath string, objType string) {
	for _, sqlInfoStmt := range sqlInfoArr {
		issues, err := parserIssueDetector.GetAllPLPGSQLIssues(sqlInfoStmt.formattedStmt, targetDbVersion)
		if err != nil {
			log.Infof("error in getting the issues-%s: %v", sqlInfoStmt.formattedStmt, err)
			continue
		}
		for _, issueInstance := range issues {
			issue := convertIssueInstanceToAnalyzeIssue(issueInstance, fpath, true)
			schemaAnalysisReport.Issues = append(schemaAnalysisReport.Issues, issue)
		}
	}

}

var MigrationCaveatsIssues = []string{
	ADDING_PK_TO_PARTITIONED_TABLE_ISSUE_REASON,
	FOREIGN_TABLE_ISSUE_REASON,
	POLICY_ROLE_ISSUE,
	UNSUPPORTED_DATATYPE_LIVE_MIGRATION,
	UNSUPPORTED_DATATYPE_LIVE_MIGRATION_WITH_FF_FB,
}

func convertIssueInstanceToAnalyzeIssue(issueInstance queryissue.QueryIssue, fileName string, isPlPgSQLIssue bool) utils.AnalyzeSchemaIssue {
	issueType := UNSUPPORTED_FEATURES_CATEGORY
	switch true {
	case isPlPgSQLIssue:
		issueType = UNSUPPORTED_PLPGSQL_OBJECTS_CATEGORY
	case slices.ContainsFunc(MigrationCaveatsIssues, func(i string) bool {
		//Adding the MIGRATION_CAVEATS issueType of the utils.Issue for these issueInstances in MigrationCaveatsIssues
		return strings.Contains(issueInstance.Name, i)
	}):
		issueType = MIGRATION_CAVEATS_CATEGORY
	case strings.HasPrefix(issueInstance.Name, UNSUPPORTED_DATATYPE):
		//Adding the UNSUPPORTED_DATATYPES issueType of the utils.Issue for these issues whose TypeName starts with "Unsupported datatype ..."
		issueType = UNSUPPORTED_DATATYPES_CATEGORY
	}

	var constraintIssues = []string{
		queryissue.EXCLUSION_CONSTRAINTS,
		queryissue.DEFERRABLE_CONSTRAINTS,
		queryissue.PK_UK_ON_COMPLEX_DATATYPE,
		queryissue.FOREIGN_KEY_REFERENCES_PARTITIONED_TABLE,
	}
	/*
		TODO:
		// unsupportedIndexIssue
		// ObjectType = INDEX
		// ObjectName = idx_name ON table_name
		// invalidCount.Type = INDEX
		// invalidCount.Name = ObjectName (because this is fully qualified)
		// DisplayName = ObjectName

		// deferrableConstraintIssue
		// ObjectType = TABLE
		// ObjectName = table_name
		// invalidCount.Type = TABLE
		// invalidCount.Name = ObjectName
		// DisplayName = table_name (constraint_name) (!= ObjectName)

		// Solutions
		// 1. Define a issue.ObjectDisplayName
		// 2. Keep it in issue.Details and write logic in UI layer to construct display name.
	*/
	displayObjectName := issueInstance.ObjectName

	constraintName, ok := issueInstance.Details[queryissue.CONSTRAINT_NAME]
	if slices.Contains(constraintIssues, issueInstance.Type) && ok {
		//In case of constraint issues we add constraint name to the object name as well
		displayObjectName = fmt.Sprintf("%s, constraint: (%s)", issueInstance.ObjectName, constraintName)
	}

	summaryMap[issueInstance.ObjectType].invalidCount[issueInstance.ObjectName] = true

	return utils.AnalyzeSchemaIssue{
		IssueType:              issueType,
		ObjectType:             issueInstance.ObjectType,
		ObjectName:             displayObjectName,
		Reason:                 issueInstance.Name,
		Type:                   issueInstance.Type,
		Impact:                 issueInstance.Impact,
		SqlStatement:           issueInstance.SqlStatement,
		DocsLink:               issueInstance.DocsLink,
		FilePath:               fileName,
		Suggestion:             issueInstance.Suggestion,
		GH:                     issueInstance.GH,
		MinimumVersionsFixedIn: issueInstance.MinimumVersionsFixedIn,
	}
}

func checkExtensions(sqlInfoArr []sqlInfo, fpath string) {
	for _, sqlInfo := range sqlInfoArr {
		if sqlInfo.objName != "" && !slices.Contains(supportedExtensionsOnYB, sqlInfo.objName) {
			summaryMap["EXTENSION"].invalidCount[sqlInfo.objName] = true
			reportCase(fpath, UNSUPPORTED_EXTENSION_ISSUE+" Refer to the docs link for the more information on supported extensions.", "https://github.com/yugabyte/yb-voyager/issues/1538", "", "EXTENSION",
				sqlInfo.objName, sqlInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, EXTENSION_DOC_LINK, constants.IMPACT_LEVEL_3)
		}
		if strings.ToLower(sqlInfo.objName) == "hll" {
			summaryMap["EXTENSION"].details[`'hll' extension is supported in YugabyteDB v2.18 onwards. Please verify this extension as per the target YugabyteDB version.`] = true
		}
	}
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
	} else if objType == "TABLE" || objType == "PARTITION" {
		createObjRegex = re("CREATE", opt("OR REPLACE"), "TABLE", ifNotExists, capture(ident))
		objNameIndex = 3
	} else if objType == "FOREIGN TABLE" {
		createObjRegex = re("CREATE", opt("OR REPLACE"), "FOREIGN", "TABLE", ifNotExists, capture(ident))
		objNameIndex = 3
	} else if objType == "OPERATOR" {
		// to include all three types of OPERATOR DDL
		// CREATE OPERATOR / CREATE OPERATOR FAMILY / CREATE OPERATOR CLASS
		createObjRegex = re("CREATE", opt("OR REPLACE"), "OPERATOR", opt("FAMILY"), opt("CLASS"), ifNotExists, capture(operatorIdent))
		objNameIndex = 5
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
		if slices.Contains([]string{"INDEX", "POLICY", "TRIGGER"}, objType) {
			//For the cases where index, trigger and policy names can be same as they are uniquely
			//identified by table name so adding that in object name
			// using parser for this case as the regex is complicated to handle all the cases for TRIGGER syntax
			objName = getObjectNameWithTable(stmt, createObjStmt[objNameIndex])
		} else {
			objName = createObjStmt[objNameIndex]
		}

		if objType == "PARTITION" || objType == "TABLE" {
			if summaryMap != nil && summaryMap["TABLE"] != nil {
				summaryMap["TABLE"].totalCount += 1
				summaryMap["TABLE"].objSet = append(summaryMap["TABLE"].objSet, objName)
			}
		} else {
			if summaryMap != nil && summaryMap[objType] != nil { //when just createSqlStrArray() is called from someother file, then no summaryMap exists
				summaryMap[objType].totalCount += 1
				summaryMap[objType].objSet = append(summaryMap[objType].objSet, objName)
			}
		}
	} else {
		if objType == "TYPE" {
			//in case of oracle there are some inherited types which can be exported as inherited tables but will be dumped in type.sql
			createObjRegex, objNameIndex = getCreateObjRegex("TABLE")
			createObjStmt = createObjRegex.FindStringSubmatch(formattedStmt)
			if createObjStmt != nil {
				objName = createObjStmt[objNameIndex]
				if summaryMap != nil && summaryMap["TABLE"] != nil {
					summaryMap["TABLE"].totalCount += 1
					summaryMap["TABLE"].objSet = append(summaryMap["TABLE"].objSet, objName)
				}
			}
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
		fileName:      fpath,
	}
	return sqlInfo
}

func getObjectNameWithTable(stmt string, regexObjName string) string {
	parsedTree, err := pg_query.Parse(stmt)
	if err != nil {
		// in case it is not able to parse stmt as its not in PG syntax so returning the regex name
		log.Errorf("Error parsing the the stmt %s - %v", stmt, err)
		return regexObjName
	}
	var objectName *sqlname.ObjectName
	var relName *pg_query.RangeVar
	createTriggerNode, isCreateTrigger := parsedTree.Stmts[0].Stmt.Node.(*pg_query.Node_CreateTrigStmt)
	createIndexNode, isCreateIndex := parsedTree.Stmts[0].Stmt.Node.(*pg_query.Node_IndexStmt)
	createPolicyNode, isCreatePolicy := parsedTree.Stmts[0].Stmt.Node.(*pg_query.Node_CreatePolicyStmt)
	if isCreateTrigger {
		relName = createTriggerNode.CreateTrigStmt.Relation
		trigName := createTriggerNode.CreateTrigStmt.Trigname
		objectName = sqlname.NewObjectName(POSTGRESQL, "", relName.Schemaname, trigName)
	} else if isCreateIndex {
		relName = createIndexNode.IndexStmt.Relation
		indexName := createIndexNode.IndexStmt.Idxname
		objectName = sqlname.NewObjectName(POSTGRESQL, "", relName.Schemaname, indexName)
	} else if isCreatePolicy {
		relName = createPolicyNode.CreatePolicyStmt.GetTable()
		policyName := createPolicyNode.CreatePolicyStmt.PolicyName
		objectName = sqlname.NewObjectName(POSTGRESQL, "", relName.Schemaname, policyName)
	}

	if isCreateIndex || isCreatePolicy || isCreateTrigger {
		schemaName := relName.Schemaname
		tableName := relName.Relname
		tableObjectName := sqlname.NewObjectName(POSTGRESQL, "", schemaName, tableName)
		return fmt.Sprintf(`%s ON %s`, objectName.Unqualified.MinQuoted, tableObjectName.MinQualified.MinQuoted)
	}
	return regexObjName

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
		utils.ErrExit("Error while reading file: %q: %s", path, err)
	}

	lines := strings.Split(string(file), "\n")
	for i := 0; i < len(lines); i++ {
		currLine := strings.TrimRight(lines[i], " ")
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
	line = strings.TrimRight(line, " ")
	if line[len(line)-1] == ';' {
		return true
	}

	cmtStartIdx := strings.LastIndex(line, "--")
	if cmtStartIdx != -1 {
		line = line[0:cmtStartIdx] // ignore comment
		line = strings.TrimRight(line, " ")
	}
	return line[len(line)-1] == ';'
}

func initializeSummaryMap() {
	log.Infof("initializing report summary map")
	for _, objType := range sourceObjList {
		summaryMap[objType] = &summaryInfo{
			invalidCount: make(map[string]bool),
			objSet:       make([]string, 0),
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

//go:embed templates/schema_analysis_report.html
var schemaAnalysisHtmlTmpl string

//go:embed templates/schema_analysis_report.txt
var schemaAnalysisTxtTmpl string

func applyTemplate(Report utils.SchemaReport, templateString string) (string, error) {
	tmpl, err := template.New("schema_analysis_report").Funcs(funcMap).Parse(templateString)
	if err != nil {
		return "", fmt.Errorf("failed to parse template file: %w", err)
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, Report)
	if err != nil {
		return "", fmt.Errorf("failed to execute parse template file: %w", err)
	}

	return buf.String(), nil
}

var funcMap = template.FuncMap{
	"join": func(arr []string, sep string) string {
		return strings.Join(arr, sep)
	},
	"sub": func(a int, b int) int {
		return a - b
	},
	"add": func(a int, b int) int {
		return a + b
	},
	"sumDbObjects": func(dbObjects []utils.DBObject, field string) int {
		total := 0
		for _, obj := range dbObjects {
			switch field {
			case "TotalCount":
				total += obj.TotalCount
			case "InvalidCount":
				total += obj.InvalidCount
			case "ValidCount":
				total += obj.TotalCount - obj.InvalidCount
			}
		}
		return total
	},
	"split":                     split,
	"getSupportedVersionString": getSupportedVersionString,
}

// add info to the 'reportStruct' variable and return
func analyzeSchemaInternal(sourceDBConf *srcdb.Source, detectIssues bool) utils.SchemaReport {
	/*
		NOTE: Don't create local var with name 'schemaAnalysisReport' since global one
		is used across all the internal functions called by analyzeSchemaInternal()
	*/
	sourceDBType = sourceDBConf.DBType
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
		if detectIssues {
			if objType == "EXTENSION" {
				checkExtensions(sqlInfoArr, filePath)
			}
			checker(sqlInfoArr, filePath, objType)

			if objType == "CONVERSION" {
				checkConversions(sqlInfoArr, filePath)
			}

			// Ideally all filtering of issues should happen in queryissue pkg layer,
			// but until we move all issue detection logic to queryissue pkg, we will filter issues here as well.
			schemaAnalysisReport.Issues = lo.Filter(schemaAnalysisReport.Issues, func(i utils.AnalyzeSchemaIssue, index int) bool {
				fixed, err := i.IsFixedIn(targetDbVersion)
				if err != nil {
					utils.ErrExit("checking if issue %v is supported: %v", i, err)
				}
				return !fixed
			})
		}
	}

	schemaAnalysisReport.SchemaSummary = reportSchemaSummary(sourceDBConf)
	schemaAnalysisReport.VoyagerVersion = utils.YB_VOYAGER_VERSION
	schemaAnalysisReport.TargetDBVersion = targetDbVersion
	return schemaAnalysisReport
}

func checkConversions(sqlInfoArr []sqlInfo, filePath string) {
	for _, sqlStmtInfo := range sqlInfoArr {
		parseTree, err := pg_query.Parse(sqlStmtInfo.stmt)
		if err != nil {
			utils.ErrExit("failed to parse the stmt: %v: %v", sqlStmtInfo.stmt, err)
		}

		createConvNode, ok := parseTree.Stmts[0].Stmt.Node.(*pg_query.Node_CreateConversionStmt)
		if ok {
			createConvStmt := createConvNode.CreateConversionStmt
			//Conversion name here is a list of items which are '.' separated
			//so 0th and 1st indexes are Schema and conv name respectively
			nameList := createConvStmt.GetConversionName()
			convName := nameList[0].GetString_().Sval
			if len(nameList) > 1 {
				convName = fmt.Sprintf("%s.%s", convName, nameList[1].GetString_().Sval)
			}
			reportCase(filePath, CONVERSION_ISSUE_REASON, "https://github.com/yugabyte/yugabyte-db/issues/10866",
				"Remove it from the exported schema", "CONVERSION", convName, sqlStmtInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, CREATE_CONVERSION_DOC_LINK, constants.IMPACT_LEVEL_3)
		} else {
			//pg_query doesn't seem to have a Node type of AlterConversionStmt so using regex for now
			if stmt := alterConvRegex.FindStringSubmatch(sqlStmtInfo.stmt); stmt != nil {
				reportCase(filePath, "ALTER CONVERSION is not supported yet", "https://github.com/YugaByte/yugabyte-db/issues/10866",
					"Remove it from the exported schema", "CONVERSION", stmt[1], sqlStmtInfo.formattedStmt, UNSUPPORTED_FEATURES_CATEGORY, CREATE_CONVERSION_DOC_LINK, "")
			}
		}

	}
}

func analyzeSchema() {

	utils.PrintAndLog("Analyzing schema for target YugabyteDB version %s\n", targetDbVersion)
	schemaAnalysisStartedEvent := createSchemaAnalysisStartedEvent()
	controlPlane.SchemaAnalysisStarted(&schemaAnalysisStartedEvent)

	if !schemaIsExported() {
		utils.ErrExit("run export schema before running analyze-schema")
	}

	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("analyze schema : load migration status record: %s", err)
	}
	analyzeSchemaInternal(msr.SourceDBConf, true)

	if analyzeSchemaReportFormat != "" {
		generateAnalyzeSchemaReport(msr, analyzeSchemaReportFormat)
	} else {
		generateAnalyzeSchemaReport(msr, HTML)
		generateAnalyzeSchemaReport(msr, JSON)
	}

	packAndSendAnalyzeSchemaPayload(COMPLETE, "")

	schemaAnalysisReport := createSchemaAnalysisIterationCompletedEvent(schemaAnalysisReport)
	controlPlane.SchemaAnalysisIterationCompleted(&schemaAnalysisReport)
}

func generateAnalyzeSchemaReport(msr *metadb.MigrationStatusRecord, reportFormat string) (err error) {
	var finalReport string
	switch reportFormat {
	case "html":
		var schemaNames = schemaAnalysisReport.SchemaSummary.SchemaNames
		if msr.SourceDBConf.DBType == POSTGRESQL {
			// marking this as empty to not display this in html report for PG
			schemaAnalysisReport.SchemaSummary.SchemaNames = []string{}
		}
		finalReport, err = applyTemplate(schemaAnalysisReport, schemaAnalysisHtmlTmpl)
		if err != nil {
			utils.ErrExit("failed to apply template for html schema analysis report: %v", err)
		}
		// restorting the value in struct for generating other format reports
		schemaAnalysisReport.SchemaSummary.SchemaNames = schemaNames
	case "json":
		jsonReportBytes, err := json.MarshalIndent(schemaAnalysisReport, "", "    ")
		if err != nil {
			utils.ErrExit("failed to marshal the report struct into json schema analysis report: %v", err)
		}
		finalReport = string(jsonReportBytes)
	case "txt":
		finalReport, err = applyTemplate(schemaAnalysisReport, schemaAnalysisTxtTmpl)
		if err != nil {
			utils.ErrExit("failed to apply template for txt schema analysis report: %v", err)
		}
	case "xml":
		xmlReportBytes, err := xml.MarshalIndent(schemaAnalysisReport, "", "\t")
		if err != nil {
			utils.ErrExit("failed to marshal the report struct into xml schema analysis report: %v", err)
		}
		finalReport = string(xmlReportBytes)
	default:
		panic(fmt.Sprintf("invalid report format: %q", reportFormat))
	}

	reportFile := fmt.Sprintf("%s.%s", ANALYSIS_REPORT_FILE_NAME, reportFormat)
	reportPath := filepath.Join(exportDir, "reports", reportFile)
	//check & inform if file already exists
	if utils.FileOrFolderExists(reportPath) {
		fmt.Printf("\n%s already exists, overwriting it with a new generated report\n", reportFile)
	}

	file, err := os.OpenFile(reportPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		utils.ErrExit("Error while opening: %q: %s", reportPath, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Errorf("Error while closing file %s: %v", reportPath, err)
		}
	}()

	_, err = file.WriteString(finalReport)
	if err != nil {
		utils.ErrExit("failed to write report to: %q: %s", reportPath, err)
	}
	fmt.Printf("-- find schema analysis report at: %s\n", reportPath)
	return nil
}

// analyze issue reasons to modify the reason before sending to callhome as will have sensitive information
var reasonsIncludingSensitiveInformationToCallhome = []string{
	UNSUPPORTED_PG_SYNTAX,
	POLICY_ROLE_ISSUE,
	UNSUPPORTED_DATATYPE,
	UNSUPPORTED_DATATYPE_LIVE_MIGRATION,
	UNSUPPORTED_DATATYPE_LIVE_MIGRATION_WITH_FF_FB,
	STORED_GENERATED_COLUMN_ISSUE_REASON,
	INSUFFICIENT_COLUMNS_IN_PK_FOR_PARTITION,
}

// analyze issue reasons to send the object names for to callhome
var reasonsToSendObjectNameToCallhome = []string{
	UNSUPPORTED_EXTENSION_ISSUE,
}

func packAndSendAnalyzeSchemaPayload(status string, errorMsg string) {
	if !shouldSendCallhome() {
		return
	}
	payload := createCallhomePayload()

	payload.MigrationPhase = ANALYZE_PHASE
	var callhomeIssues []utils.AnalyzeSchemaIssue
	for _, issue := range schemaAnalysisReport.Issues {
		issue.SqlStatement = "" // Obfuscate sensitive information before sending to callhome cluster
		if !lo.ContainsBy(reasonsToSendObjectNameToCallhome, func(r string) bool {
			return strings.Contains(issue.Reason, r)
		}) {
			issue.ObjectName = "XXX" // Redacting object name before sending in case reason is not in list
		}
		for _, sensitiveReason := range reasonsIncludingSensitiveInformationToCallhome {
			if strings.Contains(issue.Reason, sensitiveReason) {
				switch sensitiveReason {
				case UNSUPPORTED_DATATYPE, UNSUPPORTED_DATATYPE_LIVE_MIGRATION:
					//e.g. Reason "Unsupported datatype - xml on column - data"
					//sending only "Unsupported datatype - xml"
					issue.Reason = strings.Split(issue.Reason, "on column -")[0]
				default:
					issue.Reason = sensitiveReason
				}
			}
		}
		//no need to send this in callhome as we already have it documented.
		issue.Suggestion = "XXX"
		callhomeIssues = append(callhomeIssues, issue)
	}

	analyzePayload := callhome.AnalyzePhasePayload{
		TargetDBVersion: schemaAnalysisReport.TargetDBVersion,
		Issues:          callhome.MarshalledJsonString(callhomeIssues),
		DatabaseObjects: callhome.MarshalledJsonString(lo.Map(schemaAnalysisReport.SchemaSummary.DBObjects, func(dbObject utils.DBObject, _ int) utils.DBObject {
			dbObject.ObjectNames = ""
			return dbObject
		})),
		Error: callhome.SanitizeErrorMsg(errorMsg),
	}
	payload.PhasePayload = callhome.MarshalledJsonString(analyzePayload)
	payload.Status = status

	err := callhome.SendPayload(&payload)
	if err == nil && (status == COMPLETE || status == ERROR) {
		callHomeErrorOrCompletePayloadSent = true
	}
}

var analyzeSchemaCmd = &cobra.Command{
	Use: "analyze-schema",
	Short: "Analyze converted source database schema and generate a report about YB incompatible constructs.\n" +
		"For more details and examples, visit https://docs.yugabyte.com/preview/yugabyte-voyager/reference/schema-migration/analyze-schema/",
	Long: ``,
	PreRun: func(cmd *cobra.Command, args []string) {
		err := retrieveMigrationUUID()
		if err != nil {
			utils.ErrExit("failed to get migration UUID: %w", err)
		}
		validOutputFormats := []string{"html", "json", "txt", "xml"}
		validateReportOutputFormat(validOutputFormats, analyzeSchemaReportFormat)
		err = validateAndSetTargetDbVersionFlag()
		if err != nil {
			utils.ErrExit("%v", err)
		}
	},

	Run: func(cmd *cobra.Command, args []string) {
		analyzeSchema()
	},
}

func init() {
	rootCmd.AddCommand(analyzeSchemaCmd)
	registerCommonGlobalFlags(analyzeSchemaCmd)
	analyzeSchemaCmd.PersistentFlags().StringVar(&analyzeSchemaReportFormat, "output-format", "",
		"format in which report can be generated: ('html', 'txt', 'json', 'xml'). If not provided, reports will be generated in both 'json' and 'html' formats by default.")

	analyzeSchemaCmd.Flags().StringVar(&targetDbVersionStrFlag, "target-db-version", "",
		fmt.Sprintf("Target YugabyteDB version to analyze schema for (in format A.B.C.D). Defaults to latest stable version (%s)", ybversion.LatestStable.String()))
}

func validateReportOutputFormat(validOutputFormats []string, format string) {
	if format == "" {
		return
	}
	format = strings.ToLower(format)
	for i := 0; i < len(validOutputFormats); i++ {
		if format == validOutputFormats[i] {
			return
		}
	}
	utils.ErrExit("Error: Invalid output format: %s. Supported formats are %v", format, validOutputFormats)
}

func schemaIsAnalyzed() bool {
	path := filepath.Join(exportDir, "reports", fmt.Sprintf("%s.*", ANALYSIS_REPORT_FILE_NAME))
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
