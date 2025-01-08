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

import pg_query "github.com/pganalyze/pg_query_go/v6"

const (
	PLPGSQL_EXPR = "PLpgSQL_expr"
	QUERY        = "query"

	ACTION           = "action"
	DATUMS           = "datums"
	PLPGSQL_VAR      = "PLpgSQL_var"
	DATATYPE         = "datatype"
	TYPENAME         = "typname"
	PLPGSQL_TYPE     = "PLpgSQL_type"
	PLPGSQL_FUNCTION = "PLpgSQL_function"

	TABLE_OBJECT_TYPE             = "TABLE"
	TYPE_OBJECT_TYPE              = "TYPE"
	VIEW_OBJECT_TYPE              = "VIEW"
	MVIEW_OBJECT_TYPE             = "MVIEW"
	FOREIGN_TABLE_OBJECT_TYPE     = "FOREIGN TABLE"
	FUNCTION_OBJECT_TYPE          = "FUNCTION"
	PROCEDURE_OBJECT_TYPE         = "PROCEDURE"
	INDEX_OBJECT_TYPE             = "INDEX"
	POLICY_OBJECT_TYPE            = "POLICY"
	TRIGGER_OBJECT_TYPE           = "TRIGGER"
	PG_QUERY_CREATE_STMT          = "pg_query.CreateStmt"
	PG_QUERY_INDEX_STMT           = "pg_query.IndexStmt"
	PG_QUERY_ALTER_TABLE_STMT     = "pg_query.AlterTableStmt"
	PG_QUERY_POLICY_STMT          = "pg_query.CreatePolicyStmt"
	PG_QUERY_CREATE_TRIG_STMT     = "pg_query.CreateTrigStmt"
	PG_QUERY_COMPOSITE_TYPE_STMT  = "pg_query.CompositeTypeStmt"
	PG_QUERY_ENUM_TYPE_STMT       = "pg_query.CreateEnumStmt"
	PG_QUERY_FOREIGN_TABLE_STMT   = "pg_query.CreateForeignTableStmt"
	PG_QUERY_VIEW_STMT            = "pg_query.ViewStmt"
	PG_QUERY_CREATE_TABLE_AS_STMT = "pg_query.CreateTableAsStmt"
	PG_QUERY_CREATE_FUNCTION_STMT = "pg_query.CreateFunctionStmt"

	PG_QUERY_NODE_NODE           = "pg_query.Node"
	PG_QUERY_STRING_NODE         = "pg_query.String"
	PG_QUERY_ASTAR_NODE          = "pg_query.A_Star"
	PG_QUERY_ACONST_NODE         = "pg_query.A_Const"
	PG_QUERY_TYPECAST_NODE       = "pg_query.TypeCast"
	PG_QUERY_XMLEXPR_NODE        = "pg_query.XmlExpr"
	PG_QUERY_FUNCCALL_NODE       = "pg_query.FuncCall"
	PG_QUERY_COLUMNREF_NODE      = "pg_query.ColumnRef"
	PG_QUERY_RANGEFUNCTION_NODE  = "pg_query.RangeFunction"
	PG_QUERY_RANGEVAR_NODE       = "pg_query.RangeVar"
	PG_QUERY_RANGETABLEFUNC_NODE = "pg_query.RangeTableFunc"
	PG_QUERY_PARAMREF_NODE       = "pg_query.ParamRef"
	PG_QUERY_DEFELEM_NODE        = "pg_query.DefElem"

	PG_QUERY_INSERTSTMT_NODE = "pg_query.InsertStmt"
	PG_QUERY_UPDATESTMT_NODE = "pg_query.UpdateStmt"
	PG_QUERY_DELETESTMT_NODE = "pg_query.DeleteStmt"
	PG_QUERY_SELECTSTMT_NODE = "pg_query.SelectStmt"

	PG_QUERY_A_INDIRECTION_NODE              = "pg_query.A_Indirection"
	PG_QUERY_JSON_OBJECT_AGG_NODE            = "pg_query.JsonObjectAgg"
	PG_QUERY_JSON_ARRAY_AGG_NODE             = "pg_query.JsonArrayAgg"
	PG_QUERY_JSON_ARRAY_CONSTRUCTOR_AGG_NODE = "pg_query.JsonArrayConstructor"
	PG_QUERY_JSON_FUNC_EXPR_NODE             = "pg_query.JsonFuncExpr"
	PG_QUERY_JSON_OBJECT_CONSTRUCTOR_NODE    = "pg_query.JsonObjectConstructor"
	PG_QUERY_JSON_TABLE_NODE                 = "pg_query.JsonTable"
	PG_QUERY_JSON_IS_PREDICATE_NODE          = "pg_query.JsonIsPredicate"
	PG_QUERY_VIEWSTMT_NODE                   = "pg_query.ViewStmt"
	PG_QUERY_COPYSTSMT_NODE                  = "pg_query.CopyStmt"
	PG_QUERY_CONSTRAINT_NODE                 = "pg_query.Constraint"
	PG_QUERY_CTE_NODE                        = "pg_query.CommonTableExpr"

	LIMIT_OPTION_WITH_TIES   = pg_query.LimitOption_LIMIT_OPTION_WITH_TIES
	CTE_MATERIALIZED_DEFAULT = pg_query.CTEMaterialize_CTEMaterializeDefault
	ADD_CONSTRAINT           = pg_query.AlterTableType_AT_AddConstraint
	SET_OPTIONS              = pg_query.AlterTableType_AT_SetOptions
	DISABLE_RULE             = pg_query.AlterTableType_AT_DisableRule
	CLUSTER_ON               = pg_query.AlterTableType_AT_ClusterOn
	EXCLUSION_CONSTR_TYPE    = pg_query.ConstrType_CONSTR_EXCLUSION
	FOREIGN_CONSTR_TYPE      = pg_query.ConstrType_CONSTR_FOREIGN
	DEFAULT_SORTING_ORDER    = pg_query.SortByDir_SORTBY_DEFAULT
	PRIMARY_CONSTR_TYPE      = pg_query.ConstrType_CONSTR_PRIMARY
	UNIQUE_CONSTR_TYPE       = pg_query.ConstrType_CONSTR_UNIQUE
	LIST_PARTITION           = pg_query.PartitionStrategy_PARTITION_STRATEGY_LIST
)
