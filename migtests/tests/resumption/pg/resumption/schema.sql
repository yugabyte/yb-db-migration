-- Numeric and Decimal Types Table
-- Case Sensitive Name

CREATE TABLE "Case_Sensitive_Table" (
    id SERIAL PRIMARY KEY,
    v1 SMALLINT,
    v2 INTEGER,
    v3 BIGINT,
    v4 DECIMAL(6,3),
    v5 NUMERIC,
    v6 MONEY,
    n1 NUMERIC(108,9),
    n2 NUMERIC(19,2)
);

-- Unique Index on v1
CREATE UNIQUE INDEX idx_Case_Sensitive_Table_id_unique ON "Case_Sensitive_Table" (id);

-- Expression Index on v4 (DECIMAL)
CREATE INDEX idx_Case_Sensitive_Table_v4_expr ON "Case_Sensitive_Table" ((v4 > 10));

 
-- String and Enum Types Table
-- Reserved Word Name

CREATE TYPE week AS ENUM ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun');

CREATE TABLE "case" (
    id SERIAL PRIMARY KEY,
    bool_type BOOLEAN,
    char_type1 CHAR(1),
    varchar_type VARCHAR(100),
    byte_type BYTEA,
    enum_type WEEK
);

-- Partial Index on bool_type (Only TRUE values)
CREATE INDEX idx_case_bool_type_true_partial ON "case" (bool_type) WHERE bool_type = TRUE;

-- Multi-column Index on char_type1 and enum_type
CREATE INDEX idx_case_char_type1_enum_type ON "case" (char_type1, enum_type);

-- Datetime and Complex Types Table
-- Case Sensitive Reserved Word Name

CREATE TABLE "Table" (
    id SERIAL PRIMARY KEY,
    v1 DATE,
    v2 TIME,
    v3 TIMESTAMP,
    v4 TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP(0),
    v5 JSON,
    json_data JSON,
    bit_data BIT(10),
    int_array INT ARRAY[4],
    text_matrix TEXT[][],
    bit_varying_data BIT VARYING,
    default_bool BOOLEAN DEFAULT FALSE,
    default_int INTEGER DEFAULT 10,
    default_varchar VARCHAR DEFAULT 'testdefault'
);

CREATE INDEX idx_Table_int_array_gin ON "Table" USING GIN (int_array);
