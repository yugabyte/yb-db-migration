import os
import sys
from typing import Any, Dict, List
from xmlrpc.client import boolean

import psycopg2


def run_checks(checkFn):
	tgt = new_target_db()
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

	def get_table_names(self, schema="public") -> List[str]:
		cur = self.conn.cursor()
		q = "SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_type='BASE TABLE'"
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
		cur.execute(f"select relname from pg_class join pg_namespace on pg_class.relnamespace = pg_namespace.oid"+
			f" where nspname = '{schema_name}' AND relkind = '{object_type}'")
		return [obj[0] for obj in cur.fetchall()]
	
	def get_sum_of_column_of_table(self, table_name, column_name, schema_name="public") -> int:
		cur = self.conn.cursor()
		cur.execute(f"select sum({column_name}) from {schema_name}.{table_name}")
		return cur.fetchone()[0]
		
	def get_count_index_on_table(self, schema_name="public") -> Dict[str,int]:
		cur = self.conn.cursor()
		cur.execute(f"SELECT tablename, count(indexname) FROM pg_indexes WHERE schemaname = '{schema_name}' GROUP  BY tablename;")
		return {tablename: cnt for tablename,cnt in cur.fetchall()}

	def get_distinct_values__of_column_of_table(self, table_name, column_name, schema_name="public") -> List[Any]:
		cur = self.conn.cursor()
		cur.execute(f"select distinct({column_name}) from {schema_name}.{table_name}")
		return [value[0] for value in cur.fetchall()]

	# takes query and error_code and return true id the error_code you believe that query should throw matches
	def run_query_and_chk_error(self, query, error_code) -> boolean:
		cur = self.conn.cursor()
		try:
			cur.execute(f"{query}")
		except Exception as error:
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
		cur.execute(f"SELECT routine_name FROM information_schema.routines WHERE routine_schema = '{schema_name}'AND routine_name NOT IN ({self.EXPECTED_ORAFCE_FUNCTIONS}); ")
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

	def assert_distinct_values_of_col(self, table_name, column_name, schema_name="public", transform_func=None, expected_distinct_values=[]):
		distinct_values = self.get_distinct_values__of_column_of_table(table_name, column_name, schema_name)
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
		for value in all_values:
			if transform_func:
				transformed_value = transform_func(value) if value else value
			else:
				transformed_distinct_value = value
			print(f"{transformed_value}")
			assert transformed_value in expected_values
