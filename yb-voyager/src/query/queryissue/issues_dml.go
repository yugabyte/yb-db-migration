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

package queryissue

import (
	"sort"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/constants"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/issue"
)

var advisoryLocksIssue = issue.Issue{
	Type:        ADVISORY_LOCKS,
	Name:        "Advisory Locks",
	Impact:      constants.IMPACT_LEVEL_2,
	Description: "",
	Suggestion:  "",
	GH:          "",
	DocsLink:    "https://docs.yugabyte.com/preview/yugabyte-voyager/known-issues/postgresql/#advisory-locks-is-not-yet-implemented",
}

func NewAdvisoryLocksIssue(objectType string, objectName string, sqlStatement string) QueryIssue {
	return newQueryIssue(advisoryLocksIssue, objectType, objectName, sqlStatement, map[string]interface{}{})
}

var systemColumnsIssue = issue.Issue{
	Type:        SYSTEM_COLUMNS,
	Name:        "System Columns",
	Impact:      constants.IMPACT_LEVEL_2,
	Description: "",
	Suggestion:  "",
	GH:          "",
	DocsLink:    "https://docs.yugabyte.com/preview/yugabyte-voyager/known-issues/postgresql/#system-columns-is-not-yet-supported",
}

func NewSystemColumnsIssue(objectType string, objectName string, sqlStatement string) QueryIssue {
	return newQueryIssue(systemColumnsIssue, objectType, objectName, sqlStatement, map[string]interface{}{})
}

var xmlFunctionsIssue = issue.Issue{
	Type:        XML_FUNCTIONS,
	Name:        "XML Functions",
	Impact:      constants.IMPACT_LEVEL_2,
	Description: "",
	Suggestion:  "",
	GH:          "",
	DocsLink:    "https://docs.yugabyte.com/preview/yugabyte-voyager/known-issues/postgresql/#xml-functions-is-not-yet-supported",
}

func NewXmlFunctionsIssue(objectType string, objectName string, sqlStatement string) QueryIssue {
	return newQueryIssue(xmlFunctionsIssue, objectType, objectName, sqlStatement, map[string]interface{}{})
}

var regexFunctionsIssue = issue.Issue{
	Type:        REGEX_FUNCTIONS,
	Name:        "Regex Functions",
	Impact:      constants.IMPACT_LEVEL_2,
	Description: "",
	Suggestion:  "",
	GH:          "",
	DocsLink:    "",
}

func NewRegexFunctionsIssue(objectType string, objectName string, sqlStatement string) QueryIssue {
	return newQueryIssue(regexFunctionsIssue, objectType, objectName, sqlStatement, map[string]interface{}{})
}

var aggregateFunctionIssue = issue.Issue{
	Type:        AGGREGATE_FUNCTION,
	Name:        AGGREGATION_FUNCTIONS_NAME,
	Impact:      constants.IMPACT_LEVEL_2,
	Description: "any_value, range_agg and range_intersect_agg functions not supported yet in YugabyteDB",
	Suggestion:  "",
	GH:          "",
	DocsLink:    "",
}

func NewAggregationFunctionIssue(objectType string, objectName string, sqlStatement string, funcNames []string) QueryIssue {
	sort.Strings(funcNames)
	details := map[string]interface{}{
		FUNCTION_NAMES: funcNames, //TODO USE it later when we start putting these in reports
	}
	return newQueryIssue(aggregateFunctionIssue, objectType, objectName, sqlStatement, details)
}

var jsonConstructorFunctionsIssue = issue.Issue{
	Type:        JSON_CONSTRUCTOR_FUNCTION,
	Name:        JSON_CONSTRUCTOR_FUNCTION_NAME,
	Impact:      constants.IMPACT_LEVEL_2,
	Description: "Postgresql 17 features not supported yet in YugabyteDB",
	Suggestion:  "",
	GH:          "",
	DocsLink:    "",
}

func NewJsonConstructorFunctionIssue(objectType string, objectName string, sqlStatement string, funcNames []string) QueryIssue {
	sort.Strings(funcNames)
	details := map[string]interface{}{
		FUNCTION_NAMES: funcNames, //TODO USE it later when we start putting these in reports
	}
	return newQueryIssue(jsonConstructorFunctionsIssue, objectType, objectName, sqlStatement, details)
}

var jsonQueryFunctionIssue = issue.Issue{
	Type:        JSON_QUERY_FUNCTION,
	Name:        JSON_QUERY_FUNCTIONS_NAME,
	Impact:      constants.IMPACT_LEVEL_2,
	Description: "Postgresql 17 features not supported yet in YugabyteDB",
	Suggestion:  "",
	GH:          "",
	DocsLink:    "",
}

func NewJsonQueryFunctionIssue(objectType string, objectName string, sqlStatement string, funcNames []string) QueryIssue {
	sort.Strings(funcNames)
	details := map[string]interface{}{
		FUNCTION_NAMES: funcNames, //TODO USE it later when we start putting these in reports
	}
	return newQueryIssue(jsonQueryFunctionIssue, objectType, objectName, sqlStatement, details)
}

var loFunctionsIssue = issue.Issue{
	Type:        LARGE_OBJECT_FUNCTIONS,
	Name:        LARGE_OBJECT_FUNCTIONS_NAME,
	Impact:      constants.IMPACT_LEVEL_2,
	Description: "Large Objects functions are not supported in YugabyteDB",
	Suggestion:  "Large objects functions are not yet supported in YugabyteDB, no workaround available right now",
	GH:          "https://github.com/yugabyte/yugabyte-db/issues/25318",
	DocsLink:    "", //TODO
}

func NewLOFuntionsIssue(objectType string, objectName string, sqlStatement string, funcNames []string) QueryIssue {
	sort.Strings(funcNames)
	details := map[string]interface{}{
		FUNCTION_NAMES: funcNames, //TODO USE it later when we start putting these in reports
	}
	return newQueryIssue(loFunctionsIssue, objectType, objectName, sqlStatement, details)
}

var jsonbSubscriptingIssue = issue.Issue{
	Type:        JSONB_SUBSCRIPTING,
	Name:        JSONB_SUBSCRIPTING_NAME,
	Impact:      constants.IMPACT_LEVEL_2,
	Description: "Jsonb subscripting is not supported in YugabyteDB yet",
	Suggestion:  "Use Arrow operators (-> / ->>) to access the jsonb fields.",
	GH:          "",
	DocsLink:    "", //TODO
}

func NewJsonbSubscriptingIssue(objectType string, objectName string, sqlStatement string) QueryIssue {
	return newQueryIssue(jsonbSubscriptingIssue, objectType, objectName, sqlStatement, map[string]interface{}{})
}

var jsonPredicateIssue = issue.Issue{
	Type:        JSON_TYPE_PREDICATE,
	Name:        JSON_TYPE_PREDICATE_NAME,
	Impact:      constants.IMPACT_LEVEL_2,
	Description: "IS JSON predicate expressions not supported yet in YugabyteDB",
	Suggestion:  "",
	GH:          "",
	DocsLink:    "", //TODO
}

func NewJsonPredicateIssue(objectType string, objectName string, sqlStatement string) QueryIssue {
	return newQueryIssue(jsonPredicateIssue, objectType, objectName, sqlStatement, map[string]interface{}{})
}

var copyFromWhereIssue = issue.Issue{
	Type:        COPY_FROM_WHERE,
	Name:        "COPY FROM ... WHERE",
	Impact:      constants.IMPACT_LEVEL_2,
	Description: "",
	Suggestion:  "",
	GH:          "",
	DocsLink:    "",
}

func NewCopyFromWhereIssue(objectType string, objectName string, sqlStatement string) QueryIssue {
	return newQueryIssue(copyFromWhereIssue, objectType, objectName, sqlStatement, map[string]interface{}{})
}

var copyOnErrorIssue = issue.Issue{
	Type:        COPY_ON_ERROR,
	Name:        "COPY ... ON_ERROR",
	Impact:      constants.IMPACT_LEVEL_2,
	Description: "",
	Suggestion:  "",
	GH:          "",
	DocsLink:    "",
}

func NewCopyOnErrorIssue(objectType string, objectName string, sqlStatement string) QueryIssue {
	return newQueryIssue(copyOnErrorIssue, objectType, objectName, sqlStatement, map[string]interface{}{})
}

var fetchWithTiesIssue = issue.Issue{
	Type:        FETCH_WITH_TIES,
	Name:        "FETCH .. WITH TIES",
	Impact:      constants.IMPACT_LEVEL_2,
	Description: "FETCH .. WITH TIES is not supported in YugabyteDB",
	Suggestion:  "No workaround available right now",
	GH:          "",
	DocsLink:    "", //TODO
}

func NewFetchWithTiesIssue(objectType string, objectName string, sqlStatement string) QueryIssue {
	return newQueryIssue(fetchWithTiesIssue, objectType, objectName, sqlStatement, map[string]interface{}{})
}

var cteWithMaterializedIssue = issue.Issue{
	Type: CTE_WITH_MATERIALIZED_CLAUSE,
	Name: "Modifying Materialization of Common table expressions not supported",
	Impact: constants.IMPACT_LEVEL_2,
	Description: "",
	Suggestion: "No workaround available right now",
	GH: "",
	DocsLink: "", 
}

func NewCTEWithMaterializedIssue(objectType string, objectName string, sqlStatement string) QueryIssue {
	return newQueryIssue(cteWithMaterializedIssue, objectType, objectName, sqlStatement, map[string]interface{}{})
}