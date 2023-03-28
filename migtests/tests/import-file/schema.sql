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

CREATE TABLE smsa (
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