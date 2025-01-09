#!/bin/bash

set -e
set -x

source ${SCRIPTS}/functions.sh

# Set default row count (can be overridden by user input)
ROW_COUNT=${1:-1000}  # Default to 1000 if no argument is provided

REGIONS=('London' 'Boston' 'Sydney')
AMOUNTS=(1000 2000 5000)

# Insert into sales_region table
sql_sales_region="
WITH region_list AS (
    SELECT ARRAY['${REGIONS[0]}', '${REGIONS[1]}', '${REGIONS[2]}']::TEXT[] region
), amount_list AS (
    SELECT ARRAY[${AMOUNTS[0]}, ${AMOUNTS[1]}, ${AMOUNTS[2]}]::INT[] amount
) 
INSERT INTO sales_region  
(id, amount, branch, region) 
SELECT 
    n, 
    amount[1 + mod(n, array_length(amount, 1))], 
    'Branch ' || n as branch, 
    region[1 + mod(n, array_length(region, 1))] 
FROM amount_list, region_list, generate_series(1, $ROW_COUNT) as n;
"
run_psql "${SOURCE_DB_NAME}" "$sql_sales_region"

# Insert into test_partitions_sequences table
sql_test_partitions_sequences="
WITH region_list AS (
    SELECT ARRAY['${REGIONS[0]}', '${REGIONS[1]}', '${REGIONS[2]}']::TEXT[] region
), amount_list AS (
    SELECT ARRAY[${AMOUNTS[0]}, ${AMOUNTS[1]}, ${AMOUNTS[2]}]::INT[] amount
) 
INSERT INTO test_partitions_sequences  
(amount, branch, region) 
SELECT 
    amount[1 + mod(n, array_length(amount, 1))], 
    'Branch ' || n as branch, 
    region[1 + mod(n, array_length(region, 1))] 
FROM amount_list, region_list, generate_series(1, $ROW_COUNT) as n;
"
run_psql "${SOURCE_DB_NAME}" "$sql_test_partitions_sequences"

# Insert into p1.sales_region table
sql_p1_sales_region="
WITH region_list AS (
    SELECT ARRAY['${REGIONS[0]}', '${REGIONS[1]}', '${REGIONS[2]}']::TEXT[] region
), amount_list AS (
    SELECT ARRAY[${AMOUNTS[0]}, ${AMOUNTS[1]}, ${AMOUNTS[2]}]::INT[] amount
) 
INSERT INTO p1.sales_region  
(id, amount, branch, region) 
SELECT 
    n, 
    amount[1 + mod(n, array_length(amount, 1))], 
    'Branch ' || n as branch, 
    region[1 + mod(n, array_length(region, 1))] 
FROM amount_list, region_list, generate_series(1, $ROW_COUNT) as n;
"
run_psql "${SOURCE_DB_NAME}" "$sql_p1_sales_region"

# Insert into sales table
sql_sales="
WITH amount_list AS (
    SELECT ARRAY[${AMOUNTS[0]}, ${AMOUNTS[1]}, ${AMOUNTS[2]}]::INT[] amount
), date_list AS (
    SELECT ARRAY['2019-11-01'::TIMESTAMP, '2020-02-01'::TIMESTAMP, '2020-05-01'::TIMESTAMP] sale_date
) 
INSERT INTO sales
(id, p_name, amount, sale_date)
SELECT
    n,
    'Person ' || n as p_name,
    amount[1 + mod(n, array_length(amount, 1))],
    sale_date[1 + mod(n, array_length(amount, 1))]
FROM 
amount_list,
date_list,
generate_series(1, $ROW_COUNT) as n;
"
run_psql "${SOURCE_DB_NAME}" "$sql_sales"

# Insert into range_columns_partition_test table
sql_range_columns_partition_test="
INSERT INTO range_columns_partition_test
VALUES
    (5, 5),
    (3, 4),
    (5, 11),
    (5, 12),
    (4, 3),
    (3, 1);
"
run_psql "${SOURCE_DB_NAME}" "$sql_range_columns_partition_test"

sql_select_range_columns_partition_test="
SELECT
    tableoid :: regclass,
    *
FROM
    range_columns_partition_test;
"
run_psql "${SOURCE_DB_NAME}" "$sql_select_range_columns_partition_test"

# Insert into emp table
sql_emp="
INSERT INTO emp 
SELECT num, 'user_' || num , (RANDOM()*50)::INTEGER 
FROM generate_series(1, $ROW_COUNT) AS num;
"
run_psql "${SOURCE_DB_NAME}" "$sql_emp"

# Insert into customers table
sql_customers="
WITH status_list AS (
        SELECT '{"ACTIVE", "RECURRING", "REACTIVATED", "EXPIRED"}'::TEXT[] statuses
        ), arr_list AS (
            SELECT '{100, 200, 50, 250}'::INT[] arr
        )
        INSERT INTO customers
        (id, statuses, arr)
            SELECT  n,
                    statuses[1 + mod(n, array_length(statuses, 1))],
                    arr[1 + mod(n, array_length(arr, 1))]
                        FROM arr_list, generate_series(1,$ROW_COUNT) AS n, status_list;
"
run_psql "${SOURCE_DB_NAME}" "$sql_customers"

