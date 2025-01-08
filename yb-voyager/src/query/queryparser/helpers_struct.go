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
package queryparser

import (
	"fmt"
	"slices"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

func IsPLPGSQLObject(parseTree *pg_query.ParseResult) bool {
	// CREATE FUNCTION is same parser NODE for FUNCTION/PROCEDURE
	_, isPlPgSQLObject := getCreateFuncStmtNode(parseTree)
	return isPlPgSQLObject
}

func IsViewObject(parseTree *pg_query.ParseResult) bool {
	_, isViewStmt := parseTree.Stmts[0].Stmt.Node.(*pg_query.Node_ViewStmt)
	return isViewStmt
}

func IsMviewObject(parseTree *pg_query.ParseResult) bool {
	createAsNode, isCreateAsStmt := getCreateTableAsStmtNode(parseTree) //for MVIEW case
	return isCreateAsStmt && createAsNode.CreateTableAsStmt.Objtype == pg_query.ObjectType_OBJECT_MATVIEW
}

func GetObjectTypeAndObjectName(parseTree *pg_query.ParseResult) (string, string) {
	createFuncNode, isCreateFunc := getCreateFuncStmtNode(parseTree)
	viewNode, isViewStmt := getCreateViewNode(parseTree)
	createAsNode, _ := getCreateTableAsStmtNode(parseTree)
	createTableNode, isCreateTable := getCreateTableStmtNode(parseTree)
	createIndexNode, isCreateIndex := getCreateIndexStmtNode(parseTree)
	alterTableNode, isAlterTable := getAlterStmtNode(parseTree)
	switch true {
	case isCreateFunc:
		/*
			version:160001 stmts:{stmt:{create_function_stmt:{replace:true funcname:{string:{sval:"public"}} funcname:{string:{sval:"add_employee"}}
			parameters:{function_parameter:{name:"emp_name" arg_type:{names:{string:{sval:"pg_catalog"}} names:{string:{sval:"varchar"}}
			typemod:-1 location:62} mode:FUNC_PARAM_DEFAULT}} parameters:{funct ...

			version:160001 stmts:{stmt:{create_function_stmt:{is_procedure:true replace:true funcname:{string:{sval:"public"}}
			funcname:{string:{sval:"add_employee"}} parameters:{function_parameter:{name:"emp_name" arg_type:{names:{string:{sval:"pg_catalog"}}
			names:{string:{sval:"varchar"}} typemod:-1 location:63} mode:FUNC_PARAM_DEFAULT}} ...
		*/
		stmt := createFuncNode.CreateFunctionStmt
		objectType := "FUNCTION"
		if stmt.IsProcedure {
			objectType = "PROCEDURE"
		}
		funcNameList := stmt.GetFuncname()
		funcSchemaName, funcName := getFunctionObjectName(funcNameList)
		return objectType, utils.BuildObjectName(funcSchemaName, funcName)
	case isViewStmt:
		viewName := viewNode.ViewStmt.View
		return "VIEW", getObjectNameFromRangeVar(viewName)
	case IsMviewObject(parseTree):
		intoMview := createAsNode.CreateTableAsStmt.Into.Rel
		return "MVIEW", getObjectNameFromRangeVar(intoMview)
	case isCreateTable:
		return "TABLE", getObjectNameFromRangeVar(createTableNode.CreateStmt.Relation)
	case isAlterTable:
		return "TABLE", getObjectNameFromRangeVar(alterTableNode.AlterTableStmt.Relation)
	case isCreateIndex:
		indexName := createIndexNode.IndexStmt.Idxname
		schemaName := createIndexNode.IndexStmt.Relation.GetSchemaname()
		tableName := createIndexNode.IndexStmt.Relation.GetRelname()
		fullyQualifiedName := utils.BuildObjectName(schemaName, tableName)
		displayObjName := fmt.Sprintf("%s ON %s", indexName, fullyQualifiedName)
		return "INDEX", displayObjName
	default:
		panic("unsupported type of parseResult")
	}
}

func isArrayType(typeName *pg_query.TypeName) bool {
	return len(typeName.GetArrayBounds()) > 0
}

// Range Var is the struct to get the relation information like relation name, schema name, persisted relation or not, etc..
func getObjectNameFromRangeVar(obj *pg_query.RangeVar) string {
	schema := obj.Schemaname
	name := obj.Relname
	return utils.BuildObjectName(schema, name)
}

func getFunctionObjectName(funcNameList []*pg_query.Node) (string, string) {
	funcName := ""
	funcSchemaName := ""
	if len(funcNameList) > 0 {
		funcName = funcNameList[len(funcNameList)-1].GetString_().Sval // func name can be qualified / unqualifed or native / non-native proper func name will always be available at last index
	}
	if len(funcNameList) >= 2 { // Names list will have all the parts of qualified func name
		funcSchemaName = funcNameList[len(funcNameList)-2].GetString_().Sval // // func name can be qualified / unqualifed or native / non-native proper schema name will always be available at last 2nd index
	}
	return funcSchemaName, funcName
}

func getTypeNameAndSchema(typeNames []*pg_query.Node) (string, string) {
	typeName := ""
	typeSchemaName := ""
	if len(typeNames) > 0 {
		typeName = typeNames[len(typeNames)-1].GetString_().Sval // type name can be qualified / unqualifed or native / non-native proper type name will always be available at last index
	}
	if len(typeNames) >= 2 { // Names list will have all the parts of qualified type name
		typeSchemaName = typeNames[len(typeNames)-2].GetString_().Sval // // type name can be qualified / unqualifed or native / non-native proper schema name will always be available at last 2nd index
	}

	return typeName, typeSchemaName
}

func getCreateTableAsStmtNode(parseTree *pg_query.ParseResult) (*pg_query.Node_CreateTableAsStmt, bool) {
	node, ok := parseTree.Stmts[0].Stmt.Node.(*pg_query.Node_CreateTableAsStmt)
	return node, ok
}

func getCreateViewNode(parseTree *pg_query.ParseResult) (*pg_query.Node_ViewStmt, bool) {
	node, ok := parseTree.Stmts[0].Stmt.Node.(*pg_query.Node_ViewStmt)
	return node, ok
}

func getCreateFuncStmtNode(parseTree *pg_query.ParseResult) (*pg_query.Node_CreateFunctionStmt, bool) {
	node, ok := parseTree.Stmts[0].Stmt.Node.(*pg_query.Node_CreateFunctionStmt)
	return node, ok
}

func getCreateTableStmtNode(parseTree *pg_query.ParseResult) (*pg_query.Node_CreateStmt, bool) {
	node, ok := parseTree.Stmts[0].Stmt.Node.(*pg_query.Node_CreateStmt)
	return node, ok
}

func getCreateIndexStmtNode(parseTree *pg_query.ParseResult) (*pg_query.Node_IndexStmt, bool) {
	node, ok := parseTree.Stmts[0].Stmt.Node.(*pg_query.Node_IndexStmt)
	return node, ok
}

func getAlterStmtNode(parseTree *pg_query.ParseResult) (*pg_query.Node_AlterTableStmt, bool) {
	node, ok := parseTree.Stmts[0].Stmt.Node.(*pg_query.Node_AlterTableStmt)
	return node, ok
}

func getCreateTriggerStmtNode(parseTree *pg_query.ParseResult) (*pg_query.Node_CreateTrigStmt, bool) {
	node, ok := parseTree.Stmts[0].Stmt.Node.(*pg_query.Node_CreateTrigStmt)
	return node, ok
}

func getPolicyStmtNode(parseTree *pg_query.ParseResult) (*pg_query.Node_CreatePolicyStmt, bool) {
	node, ok := parseTree.Stmts[0].Stmt.Node.(*pg_query.Node_CreatePolicyStmt)
	return node, ok
}

func getCompositeTypeStmtNode(parseTree *pg_query.ParseResult) (*pg_query.Node_CompositeTypeStmt, bool) {
	node, ok := parseTree.Stmts[0].Stmt.Node.(*pg_query.Node_CompositeTypeStmt)
	return node, ok
}

func getEnumTypeStmtNode(parseTree *pg_query.ParseResult) (*pg_query.Node_CreateEnumStmt, bool) {
	node, ok := parseTree.Stmts[0].Stmt.Node.(*pg_query.Node_CreateEnumStmt)
	return node, ok
}
func getForeignTableStmtNode(parseTree *pg_query.ParseResult) (*pg_query.Node_CreateForeignTableStmt, bool) {
	node, ok := parseTree.Stmts[0].Stmt.Node.(*pg_query.Node_CreateForeignTableStmt)
	return node, ok
}

func IsFunctionObject(parseTree *pg_query.ParseResult) bool {
	funcNode, ok := getCreateFuncStmtNode(parseTree)
	if !ok {
		return false
	}
	return !funcNode.CreateFunctionStmt.IsProcedure
}

/*
return type ex-
CREATE OR REPLACE FUNCTION public.process_combined_tbl(

	...

)
RETURNS public.combined_tbl.maddr%TYPE AS
return_type:{names:{string:{sval:"public"}}  names:{string:{sval:"combined_tbl"}}  names:{string:{sval:"maddr"}}
pct_type:true  typemod:-1  location:226}
*/
func GetReturnTypeOfFunc(parseTree *pg_query.ParseResult) string {
	funcNode, _ := getCreateFuncStmtNode(parseTree)
	returnType := funcNode.CreateFunctionStmt.GetReturnType()
	return convertParserTypeNameToString(returnType)
}

func getQualifiedTypeName(typeNames []*pg_query.Node) string {
	var typeNameStrings []string
	for _, n := range typeNames {
		typeNameStrings = append(typeNameStrings, n.GetString_().Sval)
	}
	return strings.Join(typeNameStrings, ".")
}

func convertParserTypeNameToString(typeVar *pg_query.TypeName) string {
	if typeVar == nil {
		return ""
	}
	typeNames := typeVar.GetNames()
	finalTypeName := getQualifiedTypeName(typeNames) // type name can qualified table_name.column in case of %TYPE
	if typeVar.PctType {                             // %TYPE declaration, so adding %TYPE for using it further
		return finalTypeName + "%TYPE"
	}
	return finalTypeName
}

/*
function ex -
CREATE OR REPLACE FUNCTION public.process_combined_tbl(

	    p_id int,
	    p_c public.combined_tbl.c%TYPE,
	    p_bitt public.combined_tbl.bitt%TYPE,
		..

)
parseTree-
parameters:{function_parameter:{name:"p_id"  arg_type:{names:{string:{sval:"pg_catalog"}}  names:{string:{sval:"int4"}}  typemod:-1  location:66}
mode:FUNC_PARAM_DEFAULT}}  parameters:{function_parameter:{name:"p_c"  arg_type:{names:{string:{sval:"public"}}  names:{string:{sval:"combined_tbl"}}
names:{string:{sval:"c"}}  pct_type:true  typemod:-1  location:87}  mode:FUNC_PARAM_DEFAULT}}  parameters:{function_parameter:{name:"p_bitt"
arg_type:{names:{string:{sval:"public"}}  names:{string:{sval:"combined_tbl"}}  names:{string:{sval:"bitt"}}  pct_type:true  typemod:-1
location:136}  mode:FUNC_PARAM_DEFAULT}}
*/
func GetFuncParametersTypeNames(parseTree *pg_query.ParseResult) []string {
	funcNode, _ := getCreateFuncStmtNode(parseTree)
	parameters := funcNode.CreateFunctionStmt.GetParameters()
	var paramTypeNames []string
	for _, param := range parameters {
		funcParam, ok := param.Node.(*pg_query.Node_FunctionParameter)
		if ok {
			paramType := funcParam.FunctionParameter.ArgType
			paramTypeNames = append(paramTypeNames, convertParserTypeNameToString(paramType))
		}
	}
	return paramTypeNames
}

func IsDDL(parseTree *pg_query.ParseResult) (bool, error) {
	ddlParser, err := GetDDLProcessor(parseTree)
	if err != nil {
		return false, fmt.Errorf("error getting a ddl parser: %w", err)
	}
	_, ok := ddlParser.(*NoOpProcessor)
	//Considering all the DDLs we have a Processor for as of now.
	//Not Full-proof as we don't have all DDL types but atleast we will skip all the types we know currently
	return !ok, nil
}

/*
this function checks whether the current node handles the jsonb data or not by evaluating all different type of nodes -
column ref - column of jsonb type
type cast - constant data with type casting to jsonb type
func call - function call returning the jsonb data
Expression - if any of left and right operands are of node type handling jsonb data
*/
func DoesNodeHandleJsonbData(node *pg_query.Node, jsonbColumns []string, jsonbFunctions []string) bool {
	switch {
	case node.GetColumnRef() != nil:
		/*
			SELECT numbers[1] AS first_number
			FROM array_data;
			{a_indirection:{arg:{column_ref:{fields:{string:{sval:"numbers"}}  location:69}}
			indirection:{a_indices:{uidx:{a_const:{ival:{ival:1}  location:77}}}}}}  location:69}}
		*/
		_, col := GetColNameFromColumnRef(node.GetColumnRef().ProtoReflect())
		if slices.Contains(jsonbColumns, col) {
			return true
		}

	case node.GetTypeCast() != nil:
		/*
			SELECT ('{"a": {"b": {"c": 1}}}'::jsonb)['a']['b']['c'];
			{a_indirection:{arg:{type_cast:{arg:{a_const:{sval:{sval:"{\"a\": {\"b\": {\"c\": 1}}}"}  location:280}}
			type_name:{names:{string:{sval:"jsonb"}}  typemod:-1  location:306}  location:304}}
		*/
		typeCast := node.GetTypeCast()
		typeName, _ := getTypeNameAndSchema(typeCast.GetTypeName().GetNames())
		if typeName == "jsonb" {
			return true
		}
	case node.GetFuncCall() != nil:
		/*
			SELECT (jsonb_build_object('name', 'PostgreSQL', 'version', 14, 'open_source', TRUE))['name'] AS json_obj;
			val:{a_indirection:{arg:{func_call:{funcname:{string:{sval:"jsonb_build_object"}}  args:{a_const:{sval:{sval:"name"}
			location:194}}  args:{a_const:{sval:{sval:"PostgreSQL"}  location:202}}  args:{a_const:{sval:{sval:"version"}  location:216}}
			args:{a_const:{ival:{ival:14}  location:227}}
		*/
		funcCall := node.GetFuncCall()
		_, funcName := getFunctionObjectName(funcCall.Funcname)
		if slices.Contains(jsonbFunctions, funcName) {
			return true
		}
	case node.GetAExpr() != nil:
		/*
			SELECT ('{"key": "value1"}'::jsonb || '{"key1": "value2"}'::jsonb)['key'] AS object_in_array;
			val:{a_indirection:{arg:{a_expr:{kind:AEXPR_OP  name:{string:{sval:"||"}}  lexpr:{type_cast:{arg:{a_const:{sval:{sval:"{\"key\": \"value1\"}"}
			location:81}}  type_name:{names:{string:{sval:"jsonb"}}  typemod:-1  location:102}  location:100}}  rexpr:{type_cast:{arg:{a_const:{sval:{sval:"{\"key1\": \"value2\"}"}
			location:111}}  type_name:{names:{string:{sval:"jsonb"}}  typemod:-1  location:132}  location:130}}  location:108}}  indirection:{a_indices:{uidx:{a_const:{sval:{sval:"key"}
			location:139}}}}}}

			SELECT (data || '{"new_key": "new_value"}' )['name'] FROM test_jsonb;
			{val:{a_indirection:{arg:{a_expr:{kind:AEXPR_OP  name:{string:{sval:"||"}}  lexpr:{column_ref:{fields:{string:{sval:"data"}}  location:10}}  rexpr:{a_const:{sval:{sval:"{\"new_key\": \"new_value\"}"}
			location:18}}  location:15}}  indirection:{a_indices:{uidx:{a_const:{sval:{sval:"name"}

			SELECT (jsonb_build_object('name', 'PostgreSQL', 'version', 14, 'open_source', TRUE) || '{"key": "value2"}')['name'] AS json_obj;
			{val:{a_indirection:{arg:{a_expr:{kind:AEXPR_OP  name:{string:{sval:"||"}}  lexpr:{column_ref:{fields:{string:{sval:"data"}}  location:10}}  rexpr:{a_const:{sval:{sval:"{\"new_key\": \"new_value\"}"}
			location:18}}  location:15}}  indirection:{a_indices:{uidx:{a_const:{sval:{sval:"name"}  location:47}}}}}}  location:9}}
		*/
		expr := node.GetAExpr()
		lExpr := expr.GetLexpr()
		rExpr := expr.GetRexpr()
		if lExpr != nil && DoesNodeHandleJsonbData(lExpr, jsonbColumns, jsonbFunctions) {
			return true
		}
		if rExpr != nil && DoesNodeHandleJsonbData(rExpr, jsonbColumns, jsonbFunctions) {
			return true
		}
	}
	return false
}
