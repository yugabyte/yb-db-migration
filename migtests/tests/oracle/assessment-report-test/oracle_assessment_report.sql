-- VOYAGER UNSUPPORTED ORACLE INDEXES examples

-- 1. CLUSTER Index

CREATE CLUSTER emp_dept_cluster (dept_id NUMBER(4)) SIZE 512;
CREATE TABLE emp (
    emp_id NUMBER(4),
    emp_name VARCHAR2(30),
    dept_id NUMBER(4)
) CLUSTER emp_dept_cluster (dept_id);
CREATE TABLE dept (
    dept_id NUMBER(4),
    dept_name VARCHAR2(30)
) CLUSTER emp_dept_cluster (dept_id);
-- cluster index
CREATE INDEX idx_emp_dept_cluster ON CLUSTER emp_dept_cluster;
INSERT INTO dept VALUES (1, 'HR');
INSERT INTO emp VALUES (1, 'John Doe', 1);

-- 2. DOMAIN Index
CREATE TABLE text_table (
    id NUMBER PRIMARY KEY,
    text_data CLOB
);
-- Create the domain index using Oracle Text
CREATE INDEX text_index ON text_table(text_data) INDEXTYPE IS CTXSYS.CONTEXT;
INSERT INTO text_table (id, text_data) VALUES (1, 'Sample text data');

-- 3. Index-Organized Table(IOT)
CREATE TABLE iot_table (
    id NUMBER,
    data VARCHAR2(100),
    CONSTRAINT pk_iot_table PRIMARY KEY (id)
) ORGANIZATION INDEX;

-- 4. Reverse Key Index
CREATE TABLE rev_table (
    id NUMBER PRIMARY KEY,
    data VARCHAR2(100)
);
-- Create a reversed key index
CREATE INDEX rev_index ON rev_table(data) REVERSE;
INSERT INTO rev_table (id, data) VALUES (1, 'Reversed key index data');

-- 5. Function Based Reverse Key Index
CREATE TABLE func_rev_table (
    id NUMBER PRIMARY KEY,
    data VARCHAR2(100)
);
-- Create a function-based reversed key index
CREATE INDEX func_rev_index ON func_rev_table(UPPER(data)) REVERSE;
INSERT INTO func_rev_table (id, data) VALUES (1, 'Function-based reversed key data');

COMMIT;


-- supported one: FUNCTION-BASED NORMAL Index
CREATE TABLE func_based_table (
    id NUMBER PRIMARY KEY,
    data VARCHAR2(100)
);
CREATE INDEX func_based_index ON func_based_table(UPPER(data));
INSERT INTO func_based_table (id, data) VALUES (1, 'Function-based data');

-- BITMAP indexes are converted to GIN. We post a note in case we encounter them since they are partially supported. Added this to verify if the note is properly populated or not in this case.

-- Bitmap Index
CREATE TABLE sales_bitmap_idx_table (
    sale_id NUMBER PRIMARY KEY,
    product_id NUMBER,
    sale_date DATE,
    quantity NUMBER
);
-- Create a bitmap index
CREATE BITMAP INDEX idx_product_id ON sales_bitmap_idx_table(product_id);
INSERT INTO sales_bitmap_idx_table (sale_id, product_id, sale_date, quantity) VALUES (1, 100, SYSDATE, 10);

-- BITMAP JOIN Index
CREATE TABLE customers (
    customer_id NUMBER PRIMARY KEY,
    customer_name VARCHAR2(100)
);
CREATE TABLE orders (
    order_id NUMBER PRIMARY KEY,
    customer_id NUMBER,
    order_date DATE,
    amount NUMBER,
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
INSERT INTO customers (customer_id, customer_name) VALUES (1, 'Customer A');
INSERT INTO orders (order_id, customer_id, order_date, amount) VALUES (1, 1, SYSDATE, 100);
-- bitmap index
CREATE BITMAP INDEX idx_bji_customer_order ON orders(o.customer_id)
FROM orders o, customers c
WHERE o.customer_id = c.customer_id;

-- Unsupported Columns

CREATE TABLE blob_table (
    id NUMBER PRIMARY KEY,
    data BLOB
);

INSERT INTO blob_table (id, data) VALUES (1, utl_raw.cast_to_raw('Sample BLOB data'));
INSERT INTO blob_table (id, data) VALUES (2, utl_raw.cast_to_raw('Another BLOB data'));

CREATE TABLE clob_table (
    id NUMBER PRIMARY KEY,
    data CLOB
);

INSERT INTO clob_table (id, data) VALUES (1, 'Sample CLOB data');
INSERT INTO clob_table (id, data) VALUES (2, 'Another CLOB data');

CREATE TABLE nclob_table (
    id NUMBER PRIMARY KEY,
    data NCLOB
);

INSERT INTO nclob_table (id, data) VALUES (1, N'Sample NCLOB data');
INSERT INTO nclob_table (id, data) VALUES (2, N'Another NCLOB data');

CREATE TABLE bfile_table (
    id NUMBER PRIMARY KEY,
    data BFILE
);

INSERT INTO bfile_table (id, data) VALUES (1, BFILENAME('MY_DIR', 'sample_file.txt'));

CREATE TABLE xml_table (
    id NUMBER PRIMARY KEY,
    data XMLTYPE
);

INSERT INTO xml_table (id, data) VALUES (1, XMLTYPE('<note><to>Tove</to><from>Jani</from><heading>Reminder</heading><body>Don''t forget me this weekend!</body></note>'));
INSERT INTO xml_table (id, data) VALUES (2, XMLTYPE('<note><to>John</to><from>Jane</from><heading>Meeting</heading><body>Let''s meet tomorrow at 10 AM.</body></note>'));

COMMIT;

-- Compound Trigger

CREATE SEQUENCE simple_log_seq
START WITH 1
INCREMENT BY 1;

CREATE TABLE simple_table (
    id NUMBER PRIMARY KEY,
    value VARCHAR2(100)
);

CREATE TABLE simple_log (
    log_id NUMBER PRIMARY KEY,
    table_id NUMBER,
    log_action VARCHAR2(10),
    log_date DATE
);

CREATE OR REPLACE TRIGGER trg_simple_insert
FOR INSERT ON simple_table
COMPOUND TRIGGER

    AFTER EACH ROW IS
    BEGIN
        -- Insert a log entry for each row inserted
        INSERT INTO simple_log (log_id, table_id, log_action, log_date)
        VALUES (simple_log_seq.NEXTVAL, :NEW.id, 'INSERT', SYSDATE);
    END AFTER EACH ROW;

END trg_simple_insert;
/


-- Virtual column

CREATE TABLE generated_column_table (
    order_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    quantity NUMBER,
    unit_price NUMBER(10, 2),
    total_price NUMBER(10, 2) GENERATED ALWAYS AS (quantity * unit_price) VIRTUAL
);

-- Inherited types

CREATE TYPE base_vehicle_type AS OBJECT (
    make VARCHAR2(50),
    model VARCHAR2(50),
    year NUMBER(4)
) NOT FINAL;
/

-- Create a derived type for a car that inherits from base_vehicle_type
CREATE TYPE simple_car_type UNDER base_vehicle_type (
    doors NUMBER
);
/

CREATE TABLE simple_cars_table (
    car simple_car_type
);
INSERT INTO simple_cars_table VALUES (simple_car_type('Toyota', 'Camry', 2020, 4));
INSERT INTO simple_cars_table VALUES (simple_car_type('Honda', 'Accord', 2019, 4));
COMMIT;

-- System / Referenced Partitioned tables

CREATE TABLE sales (
    sale_id NUMBER,
    sale_date DATE,
    amount NUMBER
) PARTITION BY SYSTEM (
    PARTITION p1,
    PARTITION p2,
    PARTITION p3
);

CREATE TABLE departments (
    dept_id NUMBER PRIMARY KEY,
    dept_name VARCHAR2(100)
) PARTITION BY RANGE (dept_id) (
    PARTITION p1 VALUES LESS THAN (100),
    PARTITION p2 VALUES LESS THAN (200),
    PARTITION p3 VALUES LESS THAN (MAXVALUE)
);

CREATE TABLE employees2 (
    emp_id NUMBER PRIMARY KEY,
    emp_name VARCHAR2(100),
    dept_id NUMBER NOT NULL,
    CONSTRAINT fk_dept FOREIGN KEY (dept_id) REFERENCES departments(dept_id) 
        ON DELETE CASCADE
) PARTITION BY REFERENCE (fk_dept);



