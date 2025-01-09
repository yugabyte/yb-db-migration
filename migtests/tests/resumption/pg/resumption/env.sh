export SOURCE_DB_TYPE="postgresql"
export SOURCE_DB_NAME=${SOURCE_DB_NAME:-"pg_resumption_source"}
export SOURCE_DB_SCHEMA="public,p1,p2,schema2"
export TARGET_DB_NAME=${TARGET_DB_NAME:-"pg_resumption_target"}