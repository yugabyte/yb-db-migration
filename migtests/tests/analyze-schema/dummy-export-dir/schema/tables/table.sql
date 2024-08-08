-- cases for insufficient columns in PK constraint definition in CREATE TABLE to be reported in analyze-schema
CREATE TABLE sales_data (
        sales_id numeric NOT NULL,
        sales_date timestamp,
        sales_amount numeric,
        PRIMARY KEY (sales_id)
) PARTITION BY RANGE (sales_date) ;

-- cases for partition by expression cannot contain PK/Unique Key to be reported in analyze-schema 
CREATE TABLE salaries2 (
	emp_no bigint NOT NULL,
	salary bigint NOT NULL,
	from_date timestamp NOT NULL,
	to_date timestamp NOT NULL,
	PRIMARY KEY (emp_no,from_date)
) PARTITION BY RANGE (((from_date)::date - '0001-01-01bc')::integer) ;

CREATE TABLE sales (
	cust_id bigint NOT NULL,
	name varchar(40),
	store_id varchar(20) NOT NULL,
	bill_no bigint NOT NULL,
	bill_date timestamp NOT NULL,
	amount decimal(8,2) NOT NULL,
	PRIMARY KEY (bill_date)
) PARTITION BY RANGE (extract(year from date(bill_date))) ;

-- cases for multi column list partition, to be reported during analyze-schema
CREATE TABLE test_1 (
	id numeric NOT NULL,
	country_code varchar(3),
	record_type varchar(5),
	descriptions varchar(50),
	PRIMARY KEY (id)
) PARTITION BY LIST (country_code, record_type) ;

CREATE TABLE test_2 (
	id numeric NOT NULL PRIMARY KEY,
	country_code varchar(3),
	record_type varchar(5),
	descriptions varchar(50)
) PARTITION BY LIST (country_code, record_type) ;

CREATE TABLE test_non_pk_multi_column_list (
	id numeric NOT NULL PRIMARY KEY,
	country_code varchar(3),
	record_type varchar(5),
	descriptions varchar(50)
) PARTITION BY LIST (country_code, record_type) ;

-- no PK constraint, no need to report during analyze-schema
CREATE TABLE test_3 (
	id numeric,
	country_code varchar,
	record_type varchar(2)
) PARTITION BY RANGE (id) ;

CREATE TABLE test_4 (
	id numeric,
	country_code varchar
) PARTITION BY LIST (id) ;

-- all partition keys are not included in the PK constraint, to be reported during analyze-schema
CREATE TABLE test_5 (
	id numeric NOT NULL,
	country_code varchar(3),
	record_type varchar(5),
	descriptions varchar(50),
	PRIMARY KEY (id)
) PARTITION BY RANGE (country_code, record_type) ;

CREATE TABLE test_6 (
	id numeric NOT NULL,
	country_code varchar(3),
	record_type varchar(5),
	descriptions varchar(50),
	PRIMARY KEY (id,country_code)
) PARTITION BY RANGE (country_code, record_type) ;

CREATE TABLE test_7 (
	id numeric NOT NULL,
	country_code varchar(3),
	record_type varchar(5),
	descriptions varchar(50),
	PRIMARY KEY (id,country_code)
) PARTITION BY RANGE (descriptions, record_type) ;


CREATE TABLE test_8 (
	order_id bigint NOT NULL,
	order_date timestamp,
	order_mode varchar(8),
	customer_id integer,
	order_mode smallint,
	order_total double precision,
	sales_rep_id integer,
	promotion_id integer,
	PRIMARY KEY (order_id,order_mode,customer_id,order_total,sales_rep_id)
) PARTITION BY RANGE (promotion_id, order_date, sales_rep_id) ;

-- all partition key are in PK, no need to report during analyze-schema
CREATE TABLE test_9 (
	order_id bigint NOT NULL,
	order_date timestamp,
	order_mode varchar(8),
	customer_id integer,
	order_mode smallint,
	order_total double precision,
	sales_rep_id integer,
	promotion_id integer,
	PRIMARY KEY (order_id,order_mode,order_date,order_total,sales_rep_id)
) PARTITION BY RANGE (order_total, order_date, sales_rep_id) ;

--conversion not supported
CREATE CONVERSION myconv FOR 'UTF8' TO 'LATIN1' FROM myfunc;

ALTER CONVERSION myconv for  'UTF8' TO 'LATIN1' FROM myfunc1;

--Reindexing not supported
REINDEX TABLE my_table;

--Generated stored test cases with different datatype formats
CREATE TABLE order_details (
    detail_id integer NOT NULL,
    quantity integer,
    price_per_unit numeric,
    amount numeric GENERATED ALWAYS AS (((quantity)::numeric * price_per_unit)) STORED
);

CREATE TABLE public.employees4 (
    id integer NOT NULL,
    first_name character varying(50) NOT NULL,
    last_name character varying(50) NOT NULL,
    full_name character varying(101) GENERATED ALWAYS AS ((((first_name)::text || ' '::text) || (last_name)::text)) STORED
);

--like cases
CREATE TABLE table_xyz
  (LIKE xyz INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

CREATE TABLE table_abc
  (LIKE abc INCLUDING ALL);

--inherits cases
CREATE TABLE table_1 () INHERITS (xyz);

--oids case
Create table table_test (col1 text, col2 int) with (OIDS = TRUE);

--PK on interval column
create table test_interval(
    frequency interval primary key,
	col1 int
);

--alter table cases
ALTER TABLE oldschema.tbl_name SET SCHEMA newschema;

alter table table_name alter column column_name set statistics 100;

alter table test alter column col set STORAGE EXTERNAL;

alter table test_1 alter column col1 set (attribute_option=value);

ALTER TABLE address ALTER CONSTRAINT zipchk CHECK (char_length(zipcode) = 6);

ALTER TABLE IF EXISTS test_2 SET WITH OIDS;

alter table abc cluster on xyz;

alter table test SET WITHOUT CLUSTER;

alter table test_3 INHERIT test_2;

ALTER TABLE distributors VALIDATE CONSTRAINT distfk;

ALTER TABLE abc
ADD CONSTRAINT cnstr_id
 UNIQUE (id)
DEFERRABLE;

--adding pk to partitioned table
CREATE TABLE public.range_columns_partition_test (
    a bigint NOT NULL,
    b bigint NOT NULL
)
PARTITION BY RANGE (a, b);

ALTER TABLE ONLY public.range_columns_partition_test
    ADD CONSTRAINT range_columns_partition_test_pkey PRIMARY KEY (a, b);

CREATE TABLE public.range_columns_partition_test_copy (
    a bigint NOT NULL,
    b bigint NOT NULL,
	PRIMARY KEY(a, b)
)
PARTITION BY RANGE (a, b);



--foreign table issues
CREATE FOREIGN TABLE tbl_p(
	id int PRIMARY KEY
);
CREATE FOREIGN TABLE tbl_f(
	fid int, 
	pid int FOREIGN KEY REFERENCES tbl_p(id)
);

-- datatype mapping not supported
CREATE TABLE anydata_test (
	id numeric,
	content ANYDATA
) ;

CREATE TABLE anydataset_test (
	id numeric,
	content ANYDATASET
) ;

CREATE TABLE anytype_test (
	id numeric,
	content ANYTYPE
) ;

CREATE TABLE uritype_test (
	id numeric,
	content URITYPE
) ;
-- valid
Alter table only parent_tbl add constraint party_profile_pk primary key (party_profile_id);
-- alter table not supported
Alter table only party_profile_part of parent_tbl add constraint party_profile_pk primary key (party_profile_id);
