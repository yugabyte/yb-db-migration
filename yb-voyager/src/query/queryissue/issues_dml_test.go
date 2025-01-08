//go:build issues_integration

/*
Copyright (c) YugabyteDB, Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package queryissue

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go/modules/yugabytedb"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/ybversion"
	testutils "github.com/yugabyte/yb-voyager/yb-voyager/test/utils"
)

func testLOFunctionsIssue(t *testing.T) {
	ctx := context.Background()
	conn, err := getConn()
	assert.NoError(t, err)

	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, `
	CREATE EXTENSION lo;
	SELECT lo_create('2342');`)

	assertErrorCorrectlyThrownForIssueForYBVersion(t, err, "Transaction for catalog table write operation 'pg_largeobject_metadata' not found", loDatatypeIssue)
}

func testJsonbSubscriptingIssue(t *testing.T) {
	ctx := context.Background()
	conn, err := getConn()
	assert.NoError(t, err)

	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, `SELECT ('{"a": {"b": {"c": 1}}}'::jsonb)['a']['b']['c'];`)

	assertErrorCorrectlyThrownForIssueForYBVersion(t, err, "cannot subscript type jsonb because it is not an array", loDatatypeIssue)
}

func testRegexFunctionsIssue(t *testing.T) {
	ctx := context.Background()
	conn, err := getConn()
	assert.NoError(t, err)

	defer conn.Close(context.Background())
	stmts := []string{
		`SELECT regexp_count('This is an example. Another example. Example is a common word.', 'example')`,
		`SELECT regexp_instr('This is an example. Another example. Example is a common word.', 'example')`,
		`SELECT regexp_like('This is an example. Another example. Example is a common word.', 'example')`,
	}

	for _, stmt := range stmts {
		_, err = conn.Exec(ctx, stmt)
		assertErrorCorrectlyThrownForIssueForYBVersion(t, err, "does not exist", regexFunctionsIssue)
	}
}

func testFetchWithTiesIssue(t *testing.T) {
	ctx := context.Background()
	conn, err := getConn()
	assert.NoError(t, err)

	defer conn.Close(context.Background())

	stmts := []string{
		`SELECT * FROM employees
		ORDER BY salary DESC
		FETCH FIRST 2 ROWS WITH TIES;`,
	}

	for _, stmt := range stmts {
		_, err = conn.Exec(ctx, stmt)
		assertErrorCorrectlyThrownForIssueForYBVersion(t, err, `syntax error at or near "WITH"`, regexFunctionsIssue)
	}
}

func testCopyOnErrorIssue(t *testing.T) {
	ctx := context.Background()
	conn, err := getConn()
	assert.NoError(t, err)

	defer conn.Close(context.Background())

	// In case the COPY ... ON_ERROR construct gets supported in the future, this test will fail with a different error message-something related to the data.csv file not being found.
	_, err = conn.Exec(ctx, `COPY pg_largeobject (loid, pageno, data) FROM '/path/to/data.csv' WITH (FORMAT csv, HEADER true, ON_ERROR IGNORE);`)
	assertErrorCorrectlyThrownForIssueForYBVersion(t, err, "ERROR: option \"on_error\" not recognized (SQLSTATE 42601)", copyOnErrorIssue)
}

func testCopyFromWhereIssue(t *testing.T) {
	ctx := context.Background()
	conn, err := getConn()
	assert.NoError(t, err)

	defer conn.Close(context.Background())
	// In case the COPY FROM ...  WHERE construct gets supported in the future, this test will fail with a different error message-something related to the data.csv file not being found.
	_, err = conn.Exec(ctx, `COPY pg_largeobject (loid, pageno, data) FROM '/path/to/data.csv' WHERE loid = 1 WITH (FORMAT csv, HEADER true);`)
	assertErrorCorrectlyThrownForIssueForYBVersion(t, err, "ERROR: syntax error at or near \"WHERE\" (SQLSTATE 42601)", copyFromWhereIssue)
}

func testJsonConstructorFunctions(t *testing.T) {
	ctx := context.Background()
	conn, err := getConn()
	assert.NoError(t, err)
	sqls := map[string]string{
		`select json_object('code' VALUE 'P123', 'title': 'Jaws');`: `syntax error at or near "VALUE"`,
		`select JSON_ARRAYAGG('[1, "2", null]');`:                   `does not exist`,
		`SELECT json_objectagg(k VALUE v) AS json_result
	FROM (VALUES ('a', 1), ('b', 2), ('c', 3)) AS t(k, v);`: `syntax error at or near "VALUE"`,
		`SELECT JSON_ARRAY('PostgreSQL', 12, TRUE, NULL) AS json_array;`: `does not exist`,
	}
	for sql, expectedErr := range sqls {
		defer conn.Close(context.Background())
		_, err = conn.Exec(ctx, sql)

		assertErrorCorrectlyThrownForIssueForYBVersion(t, err, expectedErr, jsonConstructorFunctionsIssue)
	}
}

func testJsonPredicateIssue(t *testing.T) {
	ctx := context.Background()
	conn, err := getConn()
	assert.NoError(t, err)

	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, `SELECT js, js IS JSON "json?" FROM (VALUES ('123'), ('"abc"'), ('{"a": "b"}'), ('[1,2]'),('abc')) foo(js);`)

	assertErrorCorrectlyThrownForIssueForYBVersion(t, err, `syntax error at or near "JSON"`, jsonConstructorFunctionsIssue)
}

func testJsonQueryFunctions(t *testing.T) {
	ctx := context.Background()
	conn, err := getConn()
	assert.NoError(t, err)
	sqls := []string{
		`SELECT id, JSON_QUERY(details, '$.author') AS author
FROM books;`,
		`SELECT 
    id, 
    JSON_VALUE(details, '$.title') AS title,
    JSON_VALUE(details, '$.price')::NUMERIC AS price
FROM books;`,
		`SELECT id, details
FROM books
WHERE JSON_EXISTS(details, '$.author');`,
	}
	for _, sql := range sqls {
		defer conn.Close(context.Background())
		_, err = conn.Exec(ctx, sql)

		assertErrorCorrectlyThrownForIssueForYBVersion(t, err, `does not exist`, jsonConstructorFunctionsIssue)
	}

	jsonTableSQL := `SELECT * FROM json_table(
			'[{"a":10,"b":20},{"a":30,"b":40}]'::jsonb,
			'$[*]'
			COLUMNS (
				column_a int4 path '$.a',
				column_b int4 path '$.b'
			)
		);`
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, jsonTableSQL)

	assertErrorCorrectlyThrownForIssueForYBVersion(t, err, `syntax error at or near "COLUMNS"`, jsonConstructorFunctionsIssue)
}

func testAggFunctions(t *testing.T) {
	sqls := []string{
		`CREATE TABLE any_value_ex (
    department TEXT,
    employee_name TEXT,
    salary NUMERIC
);

INSERT INTO any_value_ex VALUES
('HR', 'Alice', 50000),
('HR', 'Bob', 55000),
('IT', 'Charlie', 60000),
('IT', 'Diana', 62000);

SELECT
    department,
    any_value(employee_name) AS any_employee
FROM any_value_ex
GROUP BY department;`,

		`CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    event_range daterange
);

INSERT INTO events (event_range) VALUES
    ('[2024-01-01, 2024-01-10]'::daterange),
    ('[2024-01-05, 2024-01-15]'::daterange),
    ('[2024-01-20, 2024-01-25]'::daterange);

SELECT range_agg(event_range) AS union_of_ranges
FROM events;

SELECT range_intersect_agg(event_range) AS intersection_of_ranges
FROM events;`,
	}

	for _, sql := range sqls {
		ctx := context.Background()
		conn, err := getConn()
		assert.NoError(t, err)

		defer conn.Close(context.Background())
		_, err = conn.Exec(ctx, sql)

		assertErrorCorrectlyThrownForIssueForYBVersion(t, err, `does not exist`, aggregateFunctionIssue)
	}
}

func testCTEWithMaterializedIssue(t *testing.T) {
	sqls := map[string]string{`WITH w AS NOT MATERIALIZED (
		SELECT * FROM big_table
	)
	SELECT * FROM w AS w1 JOIN w AS w2 ON w1.key = w2.ref
	WHERE w2.key = 123;`: `syntax error at or near "NOT"`,
		`WITH moved_rows AS MATERIALIZED (
		DELETE FROM products
		WHERE
			"date" >= '2010-10-01' AND
			"date" < '2010-11-01'
		RETURNING *
	)
	INSERT INTO products_log
	SELECT * FROM moved_rows;`: `syntax error at or near "MATERIALIZED"`
	}
	for sql, errMsg := range sqls {
		ctx := context.Background()
		conn, err := getConn()
		assert.NoError(t, err)

		defer conn.Close(context.Background())
		_, err = conn.Exec(ctx, sql)

		assertErrorCorrectlyThrownForIssueForYBVersion(t, err, errMsg, cteWithMaterializedIssue)
	}
}

func TestDMLIssuesInYBVersion(t *testing.T) {
	var err error
	ybVersion := os.Getenv("YB_VERSION")
	if ybVersion == "" {
		panic("YB_VERSION env variable is not set. Set YB_VERSION=2024.1.3.0-b105 for example")
	}

	ybVersionWithoutBuild := strings.Split(ybVersion, "-")[0]
	testYbVersion, err = ybversion.NewYBVersion(ybVersionWithoutBuild)
	testutils.FatalIfError(t, err)

	testYugabytedbConnStr = os.Getenv("YB_CONN_STR")
	if testYugabytedbConnStr == "" {
		// spawn yugabytedb container
		var err error
		ctx := context.Background()
		testYugabytedbContainer, err = yugabytedb.Run(
			ctx,
			"yugabytedb/yugabyte:"+ybVersion,
		)
		assert.NoError(t, err)
		defer testYugabytedbContainer.Terminate(context.Background())
	}

	// run tests
	success := t.Run(fmt.Sprintf("%s-%s", "lo functions", ybVersion), testLOFunctionsIssue)
	assert.True(t, success)

	success = t.Run(fmt.Sprintf("%s-%s", "regex functions", ybVersion), testRegexFunctionsIssue)
	assert.True(t, success)

	success = t.Run(fmt.Sprintf("%s-%s", "fetch with ties", ybVersion), testFetchWithTiesIssue)
	assert.True(t, success)

	success = t.Run(fmt.Sprintf("%s-%s", "copy on error", ybVersion), testCopyOnErrorIssue)
	assert.True(t, success)

	success = t.Run(fmt.Sprintf("%s-%s", "copy from where", ybVersion), testCopyFromWhereIssue)
	assert.True(t, success)
	success = t.Run(fmt.Sprintf("%s-%s", "json constructor functions", ybVersion), testJsonConstructorFunctions)
	assert.True(t, success)

	success = t.Run(fmt.Sprintf("%s-%s", "json query functions", ybVersion), testJsonQueryFunctions)
	assert.True(t, success)

	success = t.Run(fmt.Sprintf("%s-%s", "json subscripting", ybVersion), testJsonbSubscriptingIssue)
	assert.True(t, success)
	success = t.Run(fmt.Sprintf("%s-%s", "aggregate functions", ybVersion), testAggFunctions)
	assert.True(t, success)

	success = t.Run(fmt.Sprintf("%s-%s", "json type predicate", ybVersion), testJsonPredicateIssue)
	assert.True(t, success)

	success = t.Run(fmt.Sprintf("%s-%s", "cte with materialized cluase", ybVersion), testCTEWithMaterializedIssue)
	assert.True(t, success)

}
