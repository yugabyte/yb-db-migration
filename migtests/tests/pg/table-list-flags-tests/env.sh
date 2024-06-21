export SOURCE_DB_TYPE="postgresql"
export SOURCE_DB_NAME=${SOURCE_DB_NAME:-"table_list_flags_test"}
export SOURCE_DB_SCHEMA="public"
export EXPORT_TABLE_LIST=${EXPORT_TABLE_LIST:-'session_*,"Recipients","*Case*",orders,products,"WITH",with_example2,with_example1'}
export EXPORT_EX_TABLE_LIST=${EXPORT_EX_TABLE_LIST:-'session_log2'}
export IMPORT_TABLE_LIST=${IMPORT_TABLE_LIST:-'session_*,"Recipients","*Case*",products,"WITH",with_example2'}
export IMPORT_EX_TABLE_LIST=${IMPORT_EX_TABLE_LIST:-'orders,session_log1'}