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
package migassessment

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/metadb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

const (
	TABLE_INDEX_IOPS         = "table_index_iops"
	TABLE_INDEX_SIZES        = "table_index_sizes"
	TABLE_ROW_COUNTS         = "table_row_counts"
	TABLE_COLUMNS_COUNT      = "table_columns_count"
	INDEX_TO_TABLE_MAPPING   = "index_to_table_mapping"
	OBJECT_TYPE_MAPPING      = "object_type_mapping"
	TABLE_COLUMNS_DATA_TYPES = "table_columns_data_types"
	TABLE_INDEX_STATS        = "table_index_stats"

	PARTITIONED_TABLE_OBJECT_TYPE = "partitioned table"
	PARTITIONED_INDEX_OBJECT_TYPE = "partitioned index"
)

type TableIndexStats struct {
	SchemaName      string  `json:"SchemaName"`
	ObjectName      string  `json:"ObjectName"`
	RowCount        *int64  `json:"RowCount"` // Pointer to allows null values
	ColumnCount     *int64  `json:"ColumnCount"`
	Reads           *int64  `json:"Reads"`
	Writes          *int64  `json:"Writes"`
	ReadsPerSecond  *int64  `json:"ReadsPerSecond"`
	WritesPerSecond *int64  `json:"WritesPerSecond"`
	IsIndex         bool    `json:"IsIndex"`
	ObjectType      string  `json:"ObjectType"`
	ParentTableName *string `json:"ParentTableName"`
	SizeInBytes     *int64  `json:"SizeInBytes"`
}

func GetSourceMetadataDBFilePath() string {
	return filepath.Join(AssessmentDir, "dbs", "assessment.db")
}

func GetTableIndexStatName() string {
	return TABLE_INDEX_STATS
}

func InitAssessmentDB() error {
	assessmentDBPath := GetSourceMetadataDBFilePath()
	log.Infof("initializing assessment db at %s", assessmentDBPath)
	conn, err := sql.Open("sqlite3", fmt.Sprintf("%s%s", assessmentDBPath, metadb.SQLITE_OPTIONS))
	if err != nil {
		return fmt.Errorf("error opening assessment db %s: %w", assessmentDBPath, err)
	}

	cmds := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			schema_name		TEXT,
			object_name		TEXT,
			object_type		TEXT,
			seq_reads		INTEGER,
			row_writes		INTEGER,
			measurement_type TEXT,
			PRIMARY KEY (schema_name, object_name, measurement_type));`, TABLE_INDEX_IOPS),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			schema_name		TEXT,
			object_name		TEXT,
			object_type		TEXT,
			size_in_bytes	INTEGER,
			PRIMARY KEY (schema_name, object_name));`, TABLE_INDEX_SIZES),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			schema_name		TEXT,
			table_name		TEXT,
			row_count		INTEGER,
			PRIMARY KEY (schema_name, table_name));`, TABLE_ROW_COUNTS),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			schema_name		TEXT,
			object_name		TEXT,
			object_type		TEXT,
			column_count	INTEGER,
			PRIMARY KEY (schema_name, object_name));`, TABLE_COLUMNS_COUNT),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			index_schema	TEXT,
			index_name		TEXT,
			table_schema	TEXT,
			table_name		TEXT,
			PRIMARY KEY (index_schema, index_name));`, INDEX_TO_TABLE_MAPPING),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			schema_name     TEXT,
			object_name     TEXT,
			object_type     TEXT,
			PRIMARY KEY (schema_name, object_name));`, OBJECT_TYPE_MAPPING),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			schema_name		TEXT,
			table_name		TEXT,
			column_name		TEXT,
			data_type		TEXT,
			PRIMARY KEY (schema_name, table_name, column_name));`, TABLE_COLUMNS_DATA_TYPES),
		// derived from the above metric tables
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			schema_name         TEXT,
			object_name         TEXT,
			row_count           INTEGER,
			column_count		INTEGER,
			reads               INTEGER,
			writes              INTEGER,
			reads_per_second	INTEGER,
			writes_per_second	INTEGER,
			is_index            BOOLEAN,
			object_type			TEXT,
			parent_table_name   TEXT,
			size_in_bytes       INTEGER,
			PRIMARY KEY(schema_name, object_name));`, TABLE_INDEX_STATS),
	}

	for _, cmd := range cmds {
		_, err = conn.Exec(cmd)
		if err != nil {
			return fmt.Errorf("error while initializing assessment db with query-%s: %w", cmd, err)
		}
	}

	err = conn.Close()
	if err != nil {
		return fmt.Errorf("error closing assessment db %s: %w", assessmentDBPath, err)
	}
	return nil
}

type AssessmentDB struct {
	db *sql.DB
}

func NewAssessmentDB(sourceDBType string) (*AssessmentDB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("%s%s", GetSourceMetadataDBFilePath(), metadb.SQLITE_OPTIONS))
	if err != nil {
		return nil, fmt.Errorf("error opening assessment db %s: %w", GetSourceMetadataDBFilePath(), err)
	}

	return &AssessmentDB{db: db}, nil
}

func (adb *AssessmentDB) BulkInsert(table string, records [][]string) error {
	if len(records) == 0 {
		return nil
	}
	ctx := context.Background()
	tx, err := adb.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("error starting transaction for bulk insert into %s: %w", table, err)
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && errors.Is(err, sql.ErrTxDone) {
			log.Warnf("error while rollback the BulkInsert txn: %v", err)
		}
	}()

	columnNames := records[0]
	stmtStr := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, table,
		strings.Join(columnNames, ", "), strings.Repeat("?, ", len(columnNames)-1)+"?")

	stmt, err := tx.PrepareContext(ctx, stmtStr)
	if err != nil {
		return fmt.Errorf("error preparing statement for bulk insert into %s: %w", table, err)
	}

	for rowNum := 1; rowNum < len(records); rowNum++ {
		row := utils.ConvertStringSliceToInterface(records[rowNum])
		_, err = stmt.ExecContext(ctx, row...)
		if err != nil {
			return fmt.Errorf("error inserting record for bulk insert into %s: %w", table, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction for bulk insert into %s: %w", table, err)
	}

	return nil
}

const (
	InsertTableStats = `INSERT INTO %s (schema_name, object_name, row_count, column_count, reads, writes, reads_per_second, writes_per_second, is_index, object_type, parent_table_name, size_in_bytes)
	SELECT 
		trc.schema_name,
		trc.table_name AS object_name,
		trc.row_count,
		tcc.column_count,
		tii.seq_reads AS reads,
		tii.row_writes AS writes,
		NULL AS reads_per_second,
		NULL as writes_per_second,
		0 AS is_index,
		otm.object_type,
		NULL AS parent_table_name,
		tis.size_in_bytes
	FROM %s trc
	LEFT JOIN %s tii ON trc.schema_name = tii.schema_name AND trc.table_name = tii.object_name and tii.measurement_type='initial'
	LEFT JOIN %s tis ON trc.schema_name = tis.schema_name AND trc.table_name = tis.object_name
	LEFT JOIN %s tcc ON trc.schema_name = tcc.schema_name AND trc.table_name = tcc.object_name
	LEFT JOIN %s otm ON trc.schema_name = otm.schema_name AND trc.table_name = otm.object_name
	WHERE otm.object_type NOT IN ('%s', '%s');`

	// No insertion into 'column_count' for indexes
	InsertIndexStats = `INSERT INTO %s (schema_name, object_name, row_count, column_count, reads, writes, reads_per_second, writes_per_second, is_index, object_type, parent_table_name, size_in_bytes)
	SELECT 
		itm.index_schema AS schema_name,
		itm.index_name AS object_name,
		NULL AS row_count,
		tcc.column_count,
		tii.seq_reads AS reads,
		tii.row_writes AS writes,
		NULL AS reads_per_second,
		NULL as writes_per_second,
		1 AS is_index,
		otm.object_type,
		itm.table_schema || '.' || itm.table_name AS parent_table_name,
		tis.size_in_bytes
	FROM %s itm
	LEFT JOIN %s tii ON itm.index_schema = tii.schema_name AND itm.index_name = tii.object_name and tii.measurement_type='initial'
	LEFT JOIN %s tis ON itm.index_schema = tis.schema_name AND itm.index_name = tis.object_name
	LEFT JOIN %s tcc ON itm.index_schema = tcc.schema_name AND itm.index_name = tcc.object_name
	LEFT JOIN %s otm ON itm.index_schema = otm.schema_name AND itm.index_name = otm.object_name
	WHERE otm.object_type NOT IN ('%s', '%s');`

	CreateTempTable = `CREATE TEMP TABLE read_write_rates AS
	SELECT
		initial.schema_name,
		initial.object_name,
		initial.object_type,
		(final.seq_reads - initial.seq_reads) / 120 AS seq_reads_per_second,
		(final.row_writes - initial.row_writes) / 120 AS row_writes_per_second
	FROM
		%s AS initial
	JOIN
		%s AS final ON initial.schema_name = final.schema_name
								  AND initial.object_name = final.object_name
								  AND final.measurement_type = 'final'
	WHERE
		initial.measurement_type = 'initial';`

	UpdateStatsWithRates = `UPDATE table_index_stats
	SET
		reads_per_second = (SELECT seq_reads_per_second
							FROM read_write_rates
							WHERE read_write_rates.schema_name = table_index_stats.schema_name
							  AND read_write_rates.object_name = table_index_stats.object_name),
		writes_per_second = (SELECT row_writes_per_second
							 FROM read_write_rates
							 WHERE read_write_rates.schema_name = table_index_stats.schema_name
							   AND read_write_rates.object_name = table_index_stats.object_name)
	WHERE EXISTS (
		SELECT 1
		FROM read_write_rates
		WHERE read_write_rates.schema_name = table_index_stats.schema_name
		  AND read_write_rates.object_name = table_index_stats.object_name
	);`
)

// populate table_index_stats table using the data from other tables
func (adb *AssessmentDB) PopulateMigrationAssessmentStats() error {
	statements := []string{
		fmt.Sprintf(InsertTableStats, TABLE_INDEX_STATS, TABLE_ROW_COUNTS, TABLE_INDEX_IOPS, TABLE_INDEX_SIZES,
			TABLE_COLUMNS_COUNT, OBJECT_TYPE_MAPPING, PARTITIONED_TABLE_OBJECT_TYPE, PARTITIONED_INDEX_OBJECT_TYPE),
		fmt.Sprintf(InsertIndexStats, TABLE_INDEX_STATS, INDEX_TO_TABLE_MAPPING, TABLE_INDEX_IOPS, TABLE_INDEX_SIZES,
			TABLE_COLUMNS_COUNT, OBJECT_TYPE_MAPPING, PARTITIONED_TABLE_OBJECT_TYPE, PARTITIONED_INDEX_OBJECT_TYPE),
	}

	switch SourceDBType {
	case "postgresql":
		statements = append(statements,
			fmt.Sprintf(CreateTempTable, TABLE_INDEX_IOPS, TABLE_INDEX_IOPS),
			UpdateStatsWithRates)
	case "oracle":
		// already accounted
	default:
		panic("invalid source db type")
	}

	for _, stmt := range statements {
		log.Infof("executing query for populating migration assessment stats- %s", stmt)
		if _, err := adb.db.Exec(stmt); err != nil {
			return fmt.Errorf("error executing statement-%s: %w", stmt, err)
		}
	}

	return nil
}

func (adb *AssessmentDB) FetchAllStats() (*[]TableIndexStats, error) {
	log.Infof("fetching all stats info from %q table", TABLE_INDEX_STATS)
	query := fmt.Sprintf(`SELECT schema_name, object_name, row_count, column_count, reads, writes, 
       reads_per_second, writes_per_second, is_index, parent_table_name, size_in_bytes FROM %s;`, TABLE_INDEX_STATS)
	rows, err := adb.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying all stats-%s: %w", query, err)
	}
	defer rows.Close()

	var stats []TableIndexStats
	for rows.Next() {
		var stat TableIndexStats
		var rowCount, columnCount, reads, writes, readsPerSecond, writesPerSecond, sizeInBytes sql.NullInt64
		var parentTableName sql.NullString
		if err := rows.Scan(&stat.SchemaName, &stat.ObjectName, &rowCount, &columnCount, &reads, &writes,
			&readsPerSecond, &writesPerSecond, &stat.IsIndex, &parentTableName, &sizeInBytes); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		stat.RowCount = lo.Ternary(rowCount.Valid, &rowCount.Int64, nil)
		stat.ColumnCount = lo.Ternary(columnCount.Valid, &columnCount.Int64, nil)
		stat.Reads = lo.Ternary(reads.Valid, &reads.Int64, nil)
		stat.Writes = lo.Ternary(writes.Valid, &writes.Int64, nil)
		stat.ReadsPerSecond = lo.Ternary(readsPerSecond.Valid, &readsPerSecond.Int64, nil)
		stat.WritesPerSecond = lo.Ternary(writesPerSecond.Valid, &writesPerSecond.Int64, nil)
		stat.ParentTableName = lo.Ternary(parentTableName.Valid, &parentTableName.String, nil)
		stat.SizeInBytes = lo.Ternary(sizeInBytes.Valid, &sizeInBytes.Int64, nil)
		stats = append(stats, stat)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading rows: %w", err)
	}

	return &stats, nil
}

func (adb *AssessmentDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := adb.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("error executing query-%s: %w", query, err)
	}
	return rows, nil
}
