CREATE OR REPLACE FUNCTION insert_numeric_and_decimal_types(row_count INT)
RETURNS void AS $$
BEGIN
    FOR i IN 1..row_count LOOP
        INSERT INTO "Case_Sensitive_Table" (v1, v2, v3, v4, v5, v6, n1, n2)
        VALUES (
            FLOOR(RANDOM() * 65535 - 32768)::SMALLINT,      -- Smallint range
            FLOOR(RANDOM() * 2147483647)::INTEGER,         -- Integer range
            FLOOR(RANDOM() * 9223372036854775807)::BIGINT, -- Bigint range
            ROUND((RANDOM() * 999.999)::NUMERIC, 3)::DECIMAL(6,3), -- Decimal
            RANDOM() * 1e6::NUMERIC,                      -- Numeric for v5
            ((RANDOM() * 1000)::NUMERIC)::MONEY,          -- Money
            RANDOM() * 1e90::NUMERIC(108,9),              -- Numeric(108,9)
            RANDOM() * 1e15::NUMERIC(19,2)                -- Numeric(19,2), within 10^15
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION insert_string_and_enum_types(row_count INT)
RETURNS void AS $$
BEGIN
    FOR i IN 1..row_count LOOP
        INSERT INTO "case" (bool_type, char_type1, varchar_type, byte_type, enum_type)
        VALUES (
            RANDOM() > 0.5,                                     -- Boolean
            CHR(FLOOR(RANDOM() * 26 + 65)::INT),               -- Char(1) (A-Z)
            CHR(FLOOR(RANDOM() * 26 + 97)::INT) || '_data',    -- Varchar
            ('random_bytes_' || i)::BYTEA,                     -- Bytea
            (ARRAY['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])[FLOOR(RANDOM() * 7 + 1)::INT]::week -- Enum
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION insert_datetime_and_complex_types(row_count INT)
RETURNS void AS $$
BEGIN
    FOR i IN 1..row_count LOOP
        INSERT INTO "Table" (
            v1, v2, v3, v4, v5, json_data, bit_data, int_array, text_matrix, bit_varying_data, default_bool, default_int, default_varchar
        )
        VALUES (
            CURRENT_DATE + (FLOOR(RANDOM() * 365) || ' days')::INTERVAL,    -- Random Date within a year
            CURRENT_TIME + INTERVAL '1 second' * FLOOR(RANDOM() * 86400),    -- Random Time within a day
            CURRENT_TIMESTAMP + INTERVAL '1 second' * FLOOR(RANDOM() * 31536000),  -- Random Timestamp within a year
            CURRENT_TIMESTAMP + INTERVAL '1 second' * FLOOR(RANDOM() * 31536000),  -- Timestamp without Time Zone
            '{"key":"value"}'::json,                                         -- JSON for v5
            '{"name":"example"}'::json,                                      -- JSON for json_data
            (SELECT STRING_AGG(CASE WHEN RANDOM() > 0.5 THEN '1' ELSE '0' END, '') 
             FROM generate_series(1, 10))::BIT(10),                           -- Generate 10 random bits (0 or 1)
            ARRAY[FLOOR(RANDOM() * 100), FLOOR(RANDOM() * 100), FLOOR(RANDOM() * 100), FLOOR(RANDOM() * 100)],  -- Integer array
            ARRAY[ARRAY['text1', 'text2'], ARRAY['text3', 'text4']],         -- Text matrix (2x2)
            (SELECT STRING_AGG(CASE WHEN RANDOM() > 0.5 THEN '1' ELSE '0' END, '') 
             FROM generate_series(1, 10))::BIT VARYING(10),                  -- Random valid BIT VARYING (10 bits)
            RANDOM() > 0.5,                                                  -- Random Boolean for default_bool
            FLOOR(RANDOM() * 100),                                           -- Random Integer for default_int
            'test' || i::TEXT                                                -- Random String for default_varchar
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT insert_numeric_and_decimal_types(5000000);
SELECT insert_string_and_enum_types(5000000); 
SELECT insert_datetime_and_complex_types(5000000);
