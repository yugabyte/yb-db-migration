DROP TABLE IF EXISTS one_m_rows;
DROP TABLE IF EXISTS survey;
DROP TABLE IF EXISTS smsa;

CREATE TABLE one_m_rows (
    userid_fill uuid,
    idtype_fill text,
    userid uuid,
    idtype text,
    level int,
    locationgroupid uuid,
    locationid uuid,
    parentid uuid,
    attrs jsonb,
    PRIMARY KEY (userid, level, locationgroupid, parentid, locationid)
);

CREATE TABLE survey (
    Industry_Year INT,
    Industry_Aggregation_Level VARCHAR(100),
    Industry_Code VARCHAR(10),
    Industry_Type TEXT,
    Dollar_Percentage TEXT,
    Industry_name CHAR(10),
    Variable_Sub_Category VARCHAR(100),
    Variable_Category VARCHAR(100),
    Industry_Valuation TEXT,
    Industry_Class TEXT
);

CREATE TABLE survey2 AS Select * from survey;

CREATE TABLE survey3 AS Select * from survey;

CREATE SCHEMA non_public;

CREATE TABLE non_public.smsa (
    City VARCHAR(50),
    city_state CHAR(8),
    Jan_temp SMALLINT,
    July_temp INTEGER,
    Relhum BIGINT,
    Rain INT,
    Mortality NUMERIC,
    Education REAL,
    Pop_density DOUBLE PRECISION,
    PNonWhite REAL,
    Pwc NUMERIC,
    Pop TEXT,
    Pop_house NUMERIC,
    Income TEXT,
    HcPot NUMERIC,
    NOxpot REAL,
    SO2pot DOUBLE PRECISION,
    NOx NUMERIC
);

CREATE TABLE public.accounts (
    block bigint NOT NULL,
    address text NOT NULL,
    dc_balance bigint DEFAULT 0 NOT NULL,
    dc_nonce bigint DEFAULT 0 NOT NULL,
    security_balance bigint DEFAULT 0 NOT NULL,
    security_nonce bigint DEFAULT 0 NOT NULL,
    balance bigint DEFAULT 0 NOT NULL,
    nonce bigint DEFAULT 0 NOT NULL,
    staked_balance bigint,
    PRIMARY KEY (block, address)
);

create table t1_quote_char (i int, j timestamp, k bigint, l varchar(30));

create table t1_quote_escape_char1 (i int, j timestamp, k bigint, l varchar(30));

create table t1_quote_escape_char2 (i int, j timestamp, k bigint, l varchar(30));

create table t1_delimiter_escape_same (i int, j timestamp, k bigint, l varchar(30));

create table t1_newline (i int, j timestamp, k bigint, l varchar(30));

create table t1_quote_escape_dq (i int, j timestamp, k bigint, l varchar(30));

create table t1_escape_backslash (i int, j varchar, k int);

create table s3_text (i int, j int, k int);

create table s3_csv (i int, j int);

create table s3_volume (i int, j int, k int);

create table s3_csv_with_header (i int, j int, k int);

create table s3_multitable_t1 (i int, j int, k int);

create table s3_multitable_t2 (i int, j int, k int);

create table gcs_text as select * from s3_text;

create table gcs_csv as select * from s3_csv;

create table gcs_csv_with_header as select * from s3_csv_with_header;

create table gcs_multitable_t1 as select * from s3_multitable_t1;

create table gcs_multitable_t2 as select * from s3_multitable_t2;

create table gcs_volume (
    block bigint NOT NULL,
    address text NOT NULL,
    dc_balance bigint DEFAULT 0 NOT NULL,
    dc_nonce bigint DEFAULT 0 NOT NULL,
    security_balance bigint DEFAULT 0 NOT NULL,
    security_nonce bigint DEFAULT 0 NOT NULL,
    balance bigint DEFAULT 0 NOT NULL,
    nonce bigint DEFAULT 0 NOT NULL,
    staked_balance bigint,
    PRIMARY KEY (block, address)
);

create table gcs_quote_escape_char1 as select * from t1_quote_escape_char1;

create table test_backspace_char(i bigint, j int, k text);

create table test_backspace_char2(c1 int, c2 text, c3 text, c4 text, c5 bigint, c6 bigint, c7 text, c8 text, c9 boolean);

create table test_backspace_quote_single_quote_escape(i bigint, j int, k text);

create table test_backspace_quote_double_quote_escape(i bigint, j int, k text);

create table test_backspace_escape_double_quote(i bigint, j int, k text);

create table test_delimiter_backspace(i bigint, j int, k text);

create table test_delimiter_backspace_text(i bigint, j int, k text);

create table test_default_delimiter(i int, j text);

create table test_default_delimiter_csv(i int, j text);

create table t1_null_string_csv(i int, j timestamp, k bigint, l varchar(30));

create table t1_null_string_text(i int, j timestamp, k bigint, l varchar(30));

create table t1_null_string_csv2(i int, j timestamp, k bigint, l varchar(30));

create table t1_null_string_text2(i int, j timestamp, k bigint, l varchar(30));

-- Table to test multiple-files-to-single-table import.
create table foo (k int primary key, v text);
create table foo2 (k int primary key, v text);
create table foo3 (k int primary key, v text);
