import os
import sys
from typing import Any, Dict, List
from collections import Counter
from xmlrpc.client import boolean
import psycopg2
import json


def run_checks(checkFn, db_type="yb"):
	if db_type == "source_replica":
		tgt = new_source_replica_db()
	elif db_type == "yb":
		tgt = new_target_db()
	elif db_type == "source":
		tgt = new_source_db()
	else:
		raise ValueError("Invalid database type. Use 'source' or 'source_replica'.")
	tgt.connect()
	print("Connected")
	checkFn(tgt)
	tgt.close()
	print("Disconnected")


def new_target_db():
	env = os.environ
	return PostgresDB(
		env.get("TARGET_DB_HOST", "127.0.0.1"),
		env.get("TARGET_DB_PORT", 5433),
		env.get("TARGET_DB_USER", "yugabyte"),
		env.get("TARGET_DB_PASSWORD", ""),
		env["TARGET_DB_NAME"])

def new_source_replica_db():
	env = os.environ
	return PostgresDB(
		env.get("SOURCE_REPLICA_DB_HOST", "127.0.0.1"),
		env.get("SOURCE_REPLICA_DB_PORT", "5432"),
		env.get("SOURCE_REPLICA_DB_USER", "postgres"),
		env.get("SOURCE_REPLICA_DB_PASSWORD", "secret"),
		env["SOURCE_REPLICA_DB_NAME"])

def new_source_db():
	env = os.environ
	return PostgresDB(
		env.get("SOURCE_DB_HOST", "127.0.0.1"),
		env.get("SOURCE_DB_PORT", "5432"),
		env.get("SOURCE_DB_USER", "postgres"),
		env.get("SOURCE_DB_PASSWORD", "secret"),
		env["SOURCE_DB_NAME"])

def verify_colocation(tgt, source_db_type):
	print("Verifying the colocation of the tables")
	export_dir = os.getenv("EXPORT_DIR", "export-dir")
	json_file = f"{export_dir}/assessment/reports/migration_assessment_report.json"

	sharded_tables, colocated_tables = fetch_sharded_and_colocated_tables(json_file)

	print("Sharded Tables: ", sharded_tables)
	print("Colocated Tables: ", colocated_tables)

	def convert_table_to_lowercase_if_needed(name):
        # Convert to lowercase if not quoted in case of oracle
		if not (name.startswith('"') and name.endswith('"')):
			return name.lower()
		return name

	for table in sharded_tables:
		print(f"verifying for sharded table: {table}")
		if source_db_type == "oracle":
			schema_name = os.environ.get("TARGET_DB_SCHEMA", "public")
			table_name = convert_table_to_lowercase_if_needed(table)
		else:
			schema_name, table_name = table.split(".")
		print(f"schema_name: {schema_name}, table_name: {table_name}")
		actual_colocation = tgt.check_table_colocation(table_name, schema_name)
		assert not actual_colocation, f"Table '{table_name}' colocation mismatch. Expected: False, Actual: {actual_colocation}"

	for table in colocated_tables:
		print(f"verifying for colocated table: {table}")
		if source_db_type == "oracle":
			schema_name = os.environ.get("TARGET_DB_SCHEMA", "public")
			table_name = convert_table_to_lowercase_if_needed(table)
		else:
			schema_name, table_name = table.split(".")
		print(f"schema_name: {schema_name}, table_name: {table_name}")
		actual_colocation = tgt.check_table_colocation(table_name, schema_name)
		assert actual_colocation, f"Table '{table_name}' colocation mismatch. Expected: True, Actual: {actual_colocation}"

def fetch_sharded_and_colocated_tables(json_file):
	with open(json_file, 'r') as file:
		data = json.load(file)
	sharded_tables = data['Sizing']['SizingRecommendation']['ShardedTables']
	colocated_tables = data['Sizing']['SizingRecommendation']['ColocatedTables']

	if sharded_tables is None:
		sharded_tables = []
	if colocated_tables is None:
		colocated_tables = []
	return sharded_tables, colocated_tables

class PostgresDB:
	  
	def __init__(self, host, port, user, password, database):
		self.host = host
		self.port = port
		self.user = user
		self.password = password
		self.database = database
		self.EXPECTED_ORAFCE_FUNCTIONS = "''"
		self.EXPECTED_ORAFCE_VIEWS = "''"
		self.EXPECTED_ORAFCE_SCHEMAS = "''"
		env = os.environ
		if env.get("SOURCE_DB_TYPE") == "oracle":
			#issue if function with same name as these is there with different set of arguments in ORACLE migrations then it will be ignored using this as we just compare name
			self.EXPECTED_ORAFCE_FUNCTIONS = "'nvarchar2typmodin', 'nvarchar2typmodout', 'sinh', 'dump', 'nvarchar2send', 'to_single_byte', 'varchar2out', 'to_multi_byte', 'varchar2_transform', 'varchar2recv', 'nvarchar2_transform', 'varchar2send', 'varchar2', 'nvarchar2in', 'nvl2', 'nvl', 'nanvl', 'decode', 'varchar2typmodout', 'nvarchar2out', 'tanh', 'nvarchar2recv', 'varchar2typmodin', 'varchar2in', 'cosh', 'nvarchar2', 'bitand'"
			self.EXPECTED_ORAFCE_VIEWS = "'dual'"
			self.EXPECTED_ORAFCE_SCHEMAS = "'dbms_alert','dbms_assert','dbms_output','dbms_pipe','dbms_random','dbms_utility','plvstr','plvdate','plvchr','plvsubst','plvlex','utl_file','plunit','oracle'"

	def connect(self):
		self.conn = psycopg2.connect(
			host=self.host,
			port=self.port,
			user=self.user,
			password=self.password,
			database=self.database
		)
	  
	def close(self):
		self.conn.close()

	def table_exists(self, table_name, table_schema="public") -> bool:
		cur = self.conn.cursor()
		cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=%s AND table_schema=%s)", (table_name, table_schema))
		result = cur.fetchone()
		return result[0]

	def count_tables(self, schema="public") -> int:
		cur = self.conn.cursor()
		cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_type='BASE TABLE'", (schema,))
		return cur.fetchone()[0]

	def get_table_names(self, schema="public") -> List[str]:
		cur = self.conn.cursor()
		q = "SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_type='BASE TABLE'"
		cur.execute(q, (schema,))
		return [table[0] for table in cur.fetchall()]
		
	def get_foreign_table_names(self, schema="public") -> List[str]:
		cur = self.conn.cursor()
		q = "SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_type='FOREIGN'"
		cur.execute(q, (schema,))
		return [table[0] for table in cur.fetchall()]

	def get_row_count(self, table_name, schema_name="public") -> int:
		cur = self.conn.cursor()
		cur.execute(f'SELECT COUNT(*) FROM {schema_name}."{table_name}"')
		return cur.fetchone()[0]

	def row_count_of_all_tables(self, schema_name="public") -> Dict[str, int]:
		tables = self.get_table_names(schema_name)
		return {table: self.get_row_count(table, schema_name) for table in tables}

	def get_objects_of_type(self, object_type, schema_name="public") -> List[str]:
		object_type = {
			"TABLE": "r",
			"VIEW": "v",
			"INDEX": "i",
			"SEQUENCE": "S",
			"MVIEW": "m",
		}[object_type]
		cur = self.conn.cursor()
		query= f"select relname from pg_class join pg_namespace on pg_class.relnamespace = pg_namespace.oid"
		query += f" where nspname = '{schema_name}' AND relkind = '{object_type}'"
		if object_type == 'v':
			query += f" AND relname NOT IN ({self.EXPECTED_ORAFCE_VIEWS})"
		cur.execute(query)
		return [obj[0] for obj in cur.fetchall()]
	
	def get_sum_of_column_of_table(self, table_name, column_name, schema_name="public") -> int:
		cur = self.conn.cursor()
		cur.execute(f"select sum({column_name}) from {schema_name}.{table_name}")
		return cur.fetchone()[0]
		
	def get_count_index_on_table(self, schema_name="public") -> Dict[str,int]:
		cur = self.conn.cursor()
		cur.execute(f"SELECT tablename, count(indexname) FROM pg_indexes WHERE schemaname = '{schema_name}' GROUP  BY tablename;")
		return {tablename: cnt for tablename,cnt in cur.fetchall()}

	def get_distinct_values_of_column_of_table(self, table_name, column_name, schema_name="public") -> List[Any]:
		cur = self.conn.cursor()
		cur.execute(f"select distinct({column_name}) from {schema_name}.{table_name}")
		return [value[0] for value in cur.fetchall()]

	# takes query and error_code and return true id the error_code you believe that query should throw matches
	def run_query_and_chk_error(self, query, error_code) -> boolean:
		cur = self.conn.cursor()
		try:
			cur.execute(f"{query}")
		except Exception as error:
			print("error", error)
			self.conn.rollback()
			return error_code == str(error.pgcode)
		return False

	def get_functions_count(self, schema_name="public") -> int:
		cur = self.conn.cursor()
		cur.execute(f"SELECT count(routine_name) FROM  information_schema.routines WHERE  routine_type = 'FUNCTION' AND routine_schema = '{schema_name}' AND routine_name NOT IN ({self.EXPECTED_ORAFCE_FUNCTIONS});")
		return cur.fetchone()[0]

	def execute_query(self, query) -> Any:
		cur=self.conn.cursor()
		cur.execute(f"{query}")
		return cur.fetchone()[0]

	def count_sequences(self,schema_name="public") -> int :	
		cur = self.conn.cursor()	
		cur.execute(f"select count(sequence_name) from information_schema.sequences where sequence_schema='{schema_name}';")	
		return cur.fetchone()[0]

	def get_column_to_data_type_mapping(self, schema_name="public") -> Dict[str, Dict[str,str]]:
		cur = self.conn.cursor()
		cur.execute(f"SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '{schema_name}' and table_name NOT IN ({self.EXPECTED_ORAFCE_VIEWS})")
		tables = {}
		for table_name, column_name, data_type in cur.fetchall():
			if table_name not in tables:
				tables[table_name] = {}
			tables[table_name][column_name] = data_type
		return tables

	def invalid_index_present(self, table_name, schema_name="public"):
		cur = self.conn.cursor()
		cur.execute(f"select indisvalid from pg_index where indrelid = '{schema_name}.{table_name}'::regclass::oid")

		for indIsValid in cur.fetchall():
			if indIsValid == False:
				return True

		return False


	def fetch_all_triggers(self, schema_name="public") -> List[str]:
		cur = self.conn.cursor()
		cur.execute(f"SELECT trigger_name FROM information_schema.triggers WHERE trigger_schema = '{schema_name}'")
		return [trigger[0] for trigger in cur.fetchall()]

	def fetch_all_procedures(self, schema_name="public") -> List[str]:
		cur = self.conn.cursor()
		cur.execute(f"SELECT routine_name FROM information_schema.routines WHERE routine_schema = '{schema_name}' AND routine_name NOT IN ({self.EXPECTED_ORAFCE_FUNCTIONS}); ")
		return [procedure[0] for procedure in cur.fetchall()]

	def fetch_partitions(self, table_name, schema_name) -> int:
		cur = self.conn.cursor()
		cur.execute(f"SELECT count(*) AS partitions FROM pg_catalog.pg_inherits WHERE inhparent = '{schema_name}.{table_name}'::regclass;")
		return cur.fetchone()[0]

	def fetch_all_rules(self, schema_name="public") -> List[str]:
		cur = self.conn.cursor()
		cur.execute(f"SELECT rulename from pg_rules where schemaname = '{schema_name}'")
		return [rule[0] for rule in cur.fetchall()]

	def fetch_all_function_names(self, schema_name="public") -> List[str]:
		cur = self.conn.cursor()
		cur.execute(f"SELECT routine_name FROM information_schema.routines WHERE routine_schema = '{schema_name}' AND routine_name NOT IN ({self.EXPECTED_ORAFCE_FUNCTIONS});")
		return [function[0] for function in cur.fetchall()]

	def fetch_all_table_rows(self, table_name, schema_name="public") -> set[str]:
		cur = self.conn.cursor()
		cur.execute(f"SELECT * FROM {schema_name}.{table_name}")
		return set(cur.fetchall())

	def fetch_all_pg_extension(self, schema_name = "public") -> set[str]:
		cur = self.conn.cursor()
		cur.execute(f"SELECT extname FROM pg_extension")
		return set(cur.fetchall())

	def fetch_all_schemas(self) -> set[str]:
		cur = self.conn.cursor()
		cur.execute(f"SELECT schema_name FROM information_schema.schemata where schema_name !~ '^pg_' and schema_name <> 'information_schema' and schema_name NOT IN ({self.EXPECTED_ORAFCE_SCHEMAS})")		
		return set(cur.fetchall())

	def get_identity_type_columns(self, type_name, table_name, schema_name="public") -> List[str]:
		cur = self.conn.cursor()
		cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND is_identity='YES' AND identity_generation='{type_name}'")
		return [column[0] for column in cur.fetchall()]
	
	def assert_distinct_values_of_col(self, table_name, column_name, schema_name="public", transform_func=None, expected_distinct_values=[]):
		distinct_values = self.get_distinct_values_of_column_of_table(table_name, column_name, schema_name)
		for distinct_value in distinct_values:
			if transform_func:
				transformed_distinct_value = transform_func(distinct_value) if distinct_value else distinct_value
			else:
				transformed_distinct_value = distinct_value
			print(f"{transformed_distinct_value}")
			assert transformed_distinct_value in expected_distinct_values

	def assert_all_values_of_col(self, table_name, column_name, schema_name="public", transform_func=None, expected_values=[]):
		cur = self.conn.cursor()
		cur.execute(f"select {column_name} from {schema_name}.{table_name}")
		all_values = [value[0] for value in cur.fetchall()]
		if transform_func:
			all_values = [transform_func(value) if value else value for value in all_values]
		assert Counter(all_values) == Counter(expected_values)

	def get_available_extensions(self) -> List[str]:
		cur = self.conn.cursor()
		cur.execute(f"SELECT name FROM pg_available_extensions")
		return [extension[0] for extension in cur.fetchall()]

	def get_target_version(self) -> str:
		cur = self.conn.cursor()
		cur.execute(f"SELECT version()")
		return cur.fetchone()[0]

	def get_text_length(self, primary_key, id, column, table_name, schema_name="public") -> int:
		cur = self.conn.cursor()
		cur.execute(f"SELECT length({column}) FROM {schema_name}.{table_name} WHERE {primary_key} = {id}")
		return cur.fetchone()[0]
	
	def check_table_colocation(self, table_name, schema_name="public") -> bool:
		cur = self.conn.cursor()
		cur.execute(f"SELECT is_colocated FROM yb_table_properties('\"{schema_name}\".\"{table_name}\"'::regclass);")
		return cur.fetchone()[0]
	
	def get_user_defined_collations(self) -> List[str]:
		cur = self.conn.cursor()
		cur.execute("SELECT collname FROM pg_collation JOIN pg_namespace ON pg_collation.collnamespace = pg_namespace.oid WHERE nspname NOT IN ('pg_catalog', 'information_schema');")
		return [collation[0] for collation in cur.fetchall()]

	def get_user_defined_aggregates(self) -> List[str]:
		cur = self.conn.cursor()
		cur.execute("SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_aggregate a ON p.oid = a.aggfnoid WHERE n.nspname NOT IN ('pg_catalog', 'information_schema');")
		return [aggregate[0] for aggregate in cur.fetchall()]
