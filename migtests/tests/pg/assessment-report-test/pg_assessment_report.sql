-- Unsupported Datatypes
CREATE TABLE parent_table (
    id SERIAL PRIMARY KEY,
    common_column1 TEXT,
    common_column2 INTEGER
);

CREATE TABLE child_table (
    specific_column1 DATE
) INHERITS (parent_table);

CREATE TABLE Mixed_Data_Types_Table1 (
    id SERIAL PRIMARY KEY,
    point_data POINT,
    snapshot_data TXID_SNAPSHOT,
    lseg_data LSEG,
    box_data BOX
);

CREATE TABLE Mixed_Data_Types_Table2 (
    id SERIAL PRIMARY KEY,
    lsn_data PG_LSN,
    lseg_data LSEG,
    path_data PATH
);

-- GIST Index on point_data column
CREATE INDEX idx_point_data ON Mixed_Data_Types_Table1 USING GIST (point_data);

-- GIST Index on box_data column
CREATE INDEX idx_box_data ON Mixed_Data_Types_Table1 USING GIST (box_data);

CREATE TABLE orders2 (
    id SERIAL PRIMARY KEY,
    order_number VARCHAR(50) UNIQUE,
    status VARCHAR(50) NOT NULL,
    shipped_date DATE
);

CREATE OR REPLACE FUNCTION prevent_update_shipped_without_date()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.status = 'shipped' AND NEW.shipped_date IS NULL THEN
        RAISE EXCEPTION 'Cannot update status to shipped without setting shipped_date';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a Constraint Trigger
CREATE CONSTRAINT TRIGGER enforce_shipped_date_constraint
AFTER UPDATE ON orders2
FOR EACH ROW
WHEN (NEW.status = 'shipped' AND NEW.shipped_date IS NULL)
EXECUTE FUNCTION prevent_update_shipped_without_date();

-- Stored Generated Column
CREATE TABLE employees2 (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    full_name VARCHAR(101) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED
);
