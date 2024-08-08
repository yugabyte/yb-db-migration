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
	"database/sql"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

var AssessmentDbSelectQuery = fmt.Sprintf("(?i)SELECT schema_name,.* FROM %v ORDER BY .* ASC", TABLE_INDEX_STATS)
var AssessmentDBColumns = []string{"schema_name", "object_name", "row_count", "reads_per_second", "writes_per_second",
	"is_index", "parent_table_name", "size_in_bytes"}

var colocatedThroughput = []ExpDataThroughput{
	{
		numCores:                   sql.NullFloat64{Float64: 4, Valid: true},
		memPerCore:                 sql.NullFloat64{Float64: 4, Valid: true},
		maxSupportedSelectsPerCore: sql.NullFloat64{Float64: 1230, Valid: true},
		maxSupportedInsertsPerCore: sql.NullFloat64{Float64: 400, Valid: true},
		selectConnPerNode:          sql.NullInt64{Int64: 8, Valid: true},
		insertConnPerNode:          sql.NullInt64{Int64: 12, Valid: true},
	},
	{
		numCores:                   sql.NullFloat64{Float64: 8, Valid: true},
		memPerCore:                 sql.NullFloat64{Float64: 4, Valid: true},
		maxSupportedSelectsPerCore: sql.NullFloat64{Float64: 1246, Valid: true},
		maxSupportedInsertsPerCore: sql.NullFloat64{Float64: 608, Valid: true},
		selectConnPerNode:          sql.NullInt64{Int64: 16, Valid: true},
		insertConnPerNode:          sql.NullInt64{Int64: 24, Valid: true},
	},
	{
		numCores:                   sql.NullFloat64{Float64: 16, Valid: true},
		memPerCore:                 sql.NullFloat64{Float64: 4, Valid: true},
		maxSupportedSelectsPerCore: sql.NullFloat64{Float64: 1220, Valid: true},
		maxSupportedInsertsPerCore: sql.NullFloat64{Float64: 755, Valid: true},
		selectConnPerNode:          sql.NullInt64{Int64: 32, Valid: true},
		insertConnPerNode:          sql.NullInt64{Int64: 48, Valid: true},
	},
}
var colocatedLimits = []ExpDataColocatedLimit{
	{
		maxColocatedSizeSupported: sql.NullFloat64{Float64: 113, Valid: true},
		numCores:                  sql.NullFloat64{Float64: 4, Valid: true},
		memPerCore:                sql.NullFloat64{Float64: 4, Valid: true},
		maxSupportedNumTables:     sql.NullInt64{Int64: 2000, Valid: true},
		minSupportedNumTables:     sql.NullFloat64{Float64: 1, Valid: true},
	},
	{
		maxColocatedSizeSupported: sql.NullFloat64{Float64: 113, Valid: true},
		numCores:                  sql.NullFloat64{Float64: 8, Valid: true},
		memPerCore:                sql.NullFloat64{Float64: 4, Valid: true},
		maxSupportedNumTables:     sql.NullInt64{Int64: 5000, Valid: true},
		minSupportedNumTables:     sql.NullFloat64{Float64: 1, Valid: true},
	},
	{
		maxColocatedSizeSupported: sql.NullFloat64{Float64: 113, Valid: true},
		numCores:                  sql.NullFloat64{Float64: 16, Valid: true},
		memPerCore:                sql.NullFloat64{Float64: 4, Valid: true},
		maxSupportedNumTables:     sql.NullInt64{Int64: 5000, Valid: true},
		minSupportedNumTables:     sql.NullFloat64{Float64: 1, Valid: true},
	},
}

/*
===== 	Test functions to test getSourceMetadata function	=====
*/
// verify successfully able to connect and load the source metadata
func TestGetSourceMetadata_SuccessReadingSourceMetadata(t *testing.T) {
	db, mock := createMockDB(t)
	rows := sqlmock.NewRows(AssessmentDBColumns).
		AddRow("public", "table1", 1000, 10, 5, false, "", 1048576000).
		AddRow("public", "index1", 0, 0, 0, true, "table1", 104857600)

	mock.ExpectQuery(AssessmentDbSelectQuery).WillReturnRows(rows)

	sourceTableMetadata, sourceIndexMetadata, totalSourceDBSize, err := getSourceMetadata(db)
	// assert if there are errors
	assert.NoError(t, err)
	// check if the total tables are equal to expected tables
	assert.Len(t, sourceTableMetadata, 1)
	// check if the total indexes are equal to expected indexes
	assert.Len(t, sourceIndexMetadata, 1)
	// check if the total size of the source database is equal to the expected size
	assert.Equal(t, 1.07, Round(totalSourceDBSize, 2))
	// check if the values of the source table metadata are equal to the expected values
	assert.Equal(t, "public", sourceTableMetadata[0].SchemaName)
	assert.Equal(t, "table1", sourceTableMetadata[0].ObjectName)
	assert.Equal(t, "public", sourceIndexMetadata[0].SchemaName)
	assert.Equal(t, "index1", sourceIndexMetadata[0].ObjectName)
}

// if table_index_stat does not exist in the assessment.db or one of the required column does not exist in the table
func TestGetSourceMetadata_QueryErrorIfTableDoesNotExistOrColumnsUnavailable(t *testing.T) {
	db, mock := createMockDB(t)
	mock.ExpectQuery(AssessmentDbSelectQuery).WillReturnError(errors.New("query error"))

	_, _, _, err := getSourceMetadata(db)
	assert.Error(t, err)
	// check if the error returned contains the expected string
	assert.Contains(t, err.Error(), "failed to query source metadata")
}

// verify if the all columns datatypes are correct. Expecting failure in case of unsupported data type. Example:
// expected column value is int but in assessment.db, float value is provided. Hence, it will fail to read/typecast
func TestGetSourceMetadata_RowScanError(t *testing.T) {
	db, mock := createMockDB(t)
	// 4th column is expected to be int, but as it is float, it will throw an error
	rows := sqlmock.NewRows(AssessmentDBColumns).AddRow("public", "table1", 1000, 10.5, 5, false, "", 1048576000).
		RowError(1, errors.New("row scan error"))
	mock.ExpectQuery(AssessmentDbSelectQuery).WillReturnRows(rows)

	_, _, _, err := getSourceMetadata(db)
	assert.Error(t, err)
	// check if the error is as expected
	assert.Contains(t, err.Error(), "failed to read from result set of query source metadata")
}

// verify if there are no rows in the source metadata
func TestGetSourceMetadata_NoRows(t *testing.T) {
	db, mock := createMockDB(t)
	rows := sqlmock.NewRows(AssessmentDBColumns)
	mock.ExpectQuery(AssessmentDbSelectQuery).WillReturnRows(rows)
	sourceTableMetadata, sourceIndexMetadata, totalSourceDBSize, err := getSourceMetadata(db)

	assert.NoError(t, err)
	//  since there is no mock data, all the fields are expected to be empty
	assert.Empty(t, sourceTableMetadata)
	assert.Empty(t, sourceIndexMetadata)
	assert.Equal(t, 0.0, totalSourceDBSize)
}

/*
===== 	Test functions to test shardingBasedOnTableSizeAndCount function	=====
*/
// validate if the source table of size in colocated limit is correctly placed in colocated table recommendation
func TestShardingBasedOnTableSizeAndCount_TableWithSizePlacedInColocated(t *testing.T) {
	sourceTableMetadata := []SourceDBMetadata{
		{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 100, Valid: true}},
	}
	var sourceIndexMetadata []SourceDBMetadata
	recommendation := map[int]IntermediateRecommendation{4: {}, 8: {}, 16: {}}

	expectedRecommendation := map[int]IntermediateRecommendation{
		4: {
			ColocatedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 100, Valid: true}},
			},
			ShardedTables: nil,
			ColocatedSize: 100,
			ShardedSize:   0,
		},
		8: {
			ColocatedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 100, Valid: true}},
			},
			ShardedTables: nil,
			ColocatedSize: 100,
			ShardedSize:   0,
		},
		16: {
			ColocatedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 100, Valid: true}},
			},
			ShardedTables: nil,
			ColocatedSize: 100,
			ShardedSize:   0,
		},
	}

	actualRecommendation :=
		shardingBasedOnTableSizeAndCount(sourceTableMetadata, sourceIndexMetadata, colocatedLimits, recommendation)
	assert.Equal(t, expectedRecommendation, actualRecommendation)
}

// validate if the source table and index of size in colocated limit is correctly placed in colocated table
// recommendation
func TestShardingBasedOnTableSizeAndCount_WithIndexes_ColocateAll(t *testing.T) {
	sourceTableMetadata := []SourceDBMetadata{
		{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 100, Valid: true}},
	}
	sourceIndexMetadata := []SourceDBMetadata{
		{
			SchemaName: "public", ObjectName: "index1", Size: sql.NullFloat64{Float64: 10, Valid: true}, IsIndex: true,
			ParentTableName: sql.NullString{String: "public.table1", Valid: true},
		},
	}
	recommendation := map[int]IntermediateRecommendation{4: {}, 8: {}, 16: {}}

	expectedRecommendation := map[int]IntermediateRecommendation{
		4: {
			ColocatedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 100, Valid: true}},
			},
			ShardedTables: nil,
			ColocatedSize: 110, // Table size + index size
		},
		8: {
			ColocatedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 100, Valid: true}},
			},
			ShardedTables: nil,
			ColocatedSize: 110, // Table size + index size
		},
		16: {
			ColocatedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 100, Valid: true}},
			},
			ShardedTables: nil,
			ColocatedSize: 110, // Table size + index size
		},
	}

	result :=
		shardingBasedOnTableSizeAndCount(sourceTableMetadata, sourceIndexMetadata, colocatedLimits, recommendation)
	assert.Equal(t, expectedRecommendation, result)
}

// validate if the source table and index of size in colocated limit exceeds the size limit and respective tables need
// to be put in sharded
func TestShardingBasedOnTableSizeAndCount_ColocatedLimitExceededBySize(t *testing.T) {
	sourceTableMetadata := []SourceDBMetadata{
		{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 110, Valid: true}},
		{SchemaName: "public", ObjectName: "table2", Size: sql.NullFloat64{Float64: 500, Valid: true}},
	}
	var sourceIndexMetadata []SourceDBMetadata
	recommendation := map[int]IntermediateRecommendation{4: {}, 8: {}, 16: {}}

	expectedRecommendation := map[int]IntermediateRecommendation{
		4: {
			ColocatedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 110, Valid: true}},
			},
			ShardedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table2", Size: sql.NullFloat64{Float64: 500, Valid: true}},
			},
			ColocatedSize: 110,
			ShardedSize:   500,
		},
		8: {
			ColocatedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 110, Valid: true}},
			},
			ShardedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table2", Size: sql.NullFloat64{Float64: 500, Valid: true}},
			},
			ColocatedSize: 110,
			ShardedSize:   500,
		},
		16: {
			ColocatedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 110, Valid: true}},
			},
			ShardedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table2", Size: sql.NullFloat64{Float64: 500, Valid: true}},
			},
			ColocatedSize: 110,
			ShardedSize:   500,
		},
	}

	result :=
		shardingBasedOnTableSizeAndCount(sourceTableMetadata, sourceIndexMetadata, colocatedLimits, recommendation)
	assert.Equal(t, expectedRecommendation, result)
}

// validate if the source table and index of size in colocated limit exceeds limit of maximum supported num objects
// and respective tables need to be put in sharded
func TestShardingBasedOnTableSizeAndCount_ColocatedLimitExceededByCount(t *testing.T) {
	const numTables = 16000
	sourceTableMetadata := make([]SourceDBMetadata, numTables)
	for i := 0; i < numTables; i++ {
		sourceTableMetadata[i] =
			SourceDBMetadata{SchemaName: "public", ObjectName: fmt.Sprintf("table%v", i), Size: sql.NullFloat64{Float64: 0.0001, Valid: true}}
	}

	var sourceIndexMetadata []SourceDBMetadata
	recommendation := map[int]IntermediateRecommendation{4: {}, 8: {}, 16: {}}

	expectedResults := make(map[int]map[string]int)
	expectedResults[4] = map[string]int{
		"lenColocatedTables": 2000,
		"lenShardedTables":   14000,
	}
	expectedResults[8] = map[string]int{
		"lenColocatedTables": 5000,
		"lenShardedTables":   11000,
	}
	expectedResults[16] = map[string]int{
		"lenColocatedTables": 5000,
		"lenShardedTables":   11000,
	}

	actualRecommendationsResult :=
		shardingBasedOnTableSizeAndCount(sourceTableMetadata, sourceIndexMetadata, colocatedLimits, recommendation)
	for key, rec := range actualRecommendationsResult {
		assert.Equal(t, expectedResults[key]["lenColocatedTables"], len(rec.ColocatedTables))
		assert.Equal(t, expectedResults[key]["lenShardedTables"], len(rec.ShardedTables))
	}
}

// validate if the tables of size more than max colocated size supported are put in the sharded tables
func TestShardingBasedOnTableSizeAndCount_NoColocatedTables(t *testing.T) {
	sourceTableMetadata := []SourceDBMetadata{
		{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 600, Valid: true}},
		{SchemaName: "public", ObjectName: "table2", Size: sql.NullFloat64{Float64: 500, Valid: true}},
	}
	var sourceIndexMetadata []SourceDBMetadata
	recommendation := map[int]IntermediateRecommendation{4: {}, 8: {}, 16: {}}

	expectedResults := make(map[int]map[string]int)
	expectedResults[4] = map[string]int{
		"lenColocatedTables": 0,
		"lenShardedTables":   2,
	}
	expectedResults[8] = map[string]int{
		"lenColocatedTables": 0,
		"lenShardedTables":   2,
	}
	expectedResults[16] = map[string]int{
		"lenColocatedTables": 0,
		"lenShardedTables":   2,
	}

	result :=
		shardingBasedOnTableSizeAndCount(sourceTableMetadata, sourceIndexMetadata, colocatedLimits, recommendation)
	for key, rec := range result {
		// assert that there are no colocated tables
		assert.Equal(t, expectedResults[key]["lenColocatedTables"], len(rec.ColocatedTables))
		// assert that there are 2 sharded tables
		assert.Equal(t, expectedResults[key]["lenShardedTables"], len(rec.ShardedTables))
	}
}

/*
===== 	Test functions to test shardingBasedOnOperations function	=====
*/
// validate that table throughput requirements are supported and sharding is not needed
func TestShardingBasedOnOperations_CanSupportOpsRequirement(t *testing.T) {
	// Define test data
	sourceIndexMetadata := []SourceDBMetadata{
		{
			ObjectName: "table1", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
			ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 100},
			WritesPerSec: sql.NullInt64{Valid: true, Int64: 50},
		},
	}
	recommendation := map[int]IntermediateRecommendation{
		4: {
			ColocatedTables: []SourceDBMetadata{
				{
					ObjectName: "table1", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
					ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 100},
					WritesPerSec: sql.NullInt64{Valid: true, Int64: 50},
				},
			},
			ShardedTables: []SourceDBMetadata{},
			ColocatedSize: 10.0,
			ShardedSize:   0.0,
		},
		8: {
			ColocatedTables: []SourceDBMetadata{
				{
					ObjectName: "table1", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
					ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 100},
					WritesPerSec: sql.NullInt64{Valid: true, Int64: 50},
				},
			},
			ShardedTables: []SourceDBMetadata{},
			ColocatedSize: 10.0,
			ShardedSize:   0.0,
		},
		16: {
			ColocatedTables: []SourceDBMetadata{
				{
					ObjectName: "table1", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
					ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 100},
					WritesPerSec: sql.NullInt64{Valid: true, Int64: 50},
				},
			},
			ShardedTables: []SourceDBMetadata{},
			ColocatedSize: 10.0,
			ShardedSize:   0.0,
		},
	}

	// Run the function
	updatedRecommendation := shardingBasedOnOperations(sourceIndexMetadata, colocatedThroughput, recommendation)

	// expected is that the table should be removed from colocated and added to sharded as the ops requirement is high
	for _, rec := range updatedRecommendation {
		assert.Empty(t, rec.ShardedTables)
		assert.Len(t, rec.ColocatedTables, 1)
		assert.Equal(t, "table1", rec.ColocatedTables[0].ObjectName)
	}
}

// validate that the tables should be removed from colocated and added to sharded as the ops requirement is higher than
// supported
func TestShardingBasedOnOperations_CannotSupportOpsAndNeedsSharding(t *testing.T) {
	// Define test data
	sourceIndexMetadata := []SourceDBMetadata{
		{
			ObjectName: "table1", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
			ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 1000000},
			WritesPerSec: sql.NullInt64{Valid: true, Int64: 50000000},
		},
	}
	recommendation := map[int]IntermediateRecommendation{
		4: {
			ColocatedTables: []SourceDBMetadata{
				{
					ObjectName: "table1", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
					ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 1000000},
					WritesPerSec: sql.NullInt64{Valid: true, Int64: 50000000},
				},
			},
			ShardedTables: []SourceDBMetadata{},
			ColocatedSize: 10.0,
			ShardedSize:   0.0,
		},
		8: {
			ColocatedTables: []SourceDBMetadata{
				{
					ObjectName: "table1", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
					ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 1000000},
					WritesPerSec: sql.NullInt64{Valid: true, Int64: 50000000},
				},
			},
			ShardedTables: []SourceDBMetadata{},
			ColocatedSize: 10.0,
			ShardedSize:   0.0,
		},
		16: {
			ColocatedTables: []SourceDBMetadata{
				{
					ObjectName: "table1", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
					ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 1000000},
					WritesPerSec: sql.NullInt64{Valid: true, Int64: 50000000},
				},
			},
			ShardedTables: []SourceDBMetadata{},
			ColocatedSize: 10.0,
			ShardedSize:   0.0,
		},
	}

	// Run the function
	updatedRecommendation := shardingBasedOnOperations(sourceIndexMetadata, colocatedThroughput, recommendation)

	// expected is that the table should be removed from colocated and added to sharded as the ops requirement is high
	for _, rec := range updatedRecommendation {
		assert.Empty(t, rec.ColocatedTables)
		assert.Len(t, rec.ShardedTables, 1)
		assert.Equal(t, "table1", rec.ShardedTables[0].ObjectName)
	}
}

/*
===== 	Test functions to test checkShardedTableLimit function	=====
*/
// validate that the sharded table limits is not exceeded and failure reasoning should be empty
func TestCheckShardedTableLimit_WithinLimit(t *testing.T) {
	// Define test data
	var sourceIndexMetadata []SourceDBMetadata
	shardedLimits := []ExpDataShardedLimit{
		{
			numCores:              sql.NullFloat64{Float64: 16, Valid: true},
			maxSupportedNumTables: sql.NullInt64{Int64: 100, Valid: true},
		},
	}
	recommendation := map[int]IntermediateRecommendation{
		16: {
			ShardedTables: []SourceDBMetadata{
				{
					SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
					ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 100},
					WritesPerSec: sql.NullInt64{Valid: true, Int64: 50},
				},
				{
					SchemaName: "public", ObjectName: "table2", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
					ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 100},
					WritesPerSec: sql.NullInt64{Valid: true, Int64: 50},
				},
			},
			VCPUsPerInstance: 16,
			MemoryPerCore:    4,
		},
	}

	// Run the function
	updatedRecommendation := checkShardedTableLimit(sourceIndexMetadata, shardedLimits, recommendation)
	for _, rec := range updatedRecommendation {
		// failure reasoning should be empty
		assert.Empty(t, rec.FailureReasoning)
	}
}

// validate that the sharded table limits is exceeded and failure reasoning is generated as expected
func TestCheckShardedTableLimit_LimitExceeded(t *testing.T) {
	// Define test data
	var sourceIndexMetadata []SourceDBMetadata
	shardedLimits := []ExpDataShardedLimit{
		{
			numCores:              sql.NullFloat64{Float64: 16, Valid: true},
			maxSupportedNumTables: sql.NullInt64{Int64: 1, Valid: true},
		},
	}
	recommendation := map[int]IntermediateRecommendation{
		16: {
			ShardedTables: []SourceDBMetadata{
				{
					SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
					ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 100},
					WritesPerSec: sql.NullInt64{Valid: true, Int64: 50},
				},
				{
					SchemaName: "public", ObjectName: "table2", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
					ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 100},
					WritesPerSec: sql.NullInt64{Valid: true, Int64: 50},
				},
			},
			VCPUsPerInstance: 16,
			MemoryPerCore:    4,
		},
	}

	// Run the function
	updatedRecommendation := checkShardedTableLimit(sourceIndexMetadata, shardedLimits, recommendation)

	// check if the Failure reasoning matches with the one generated
	for _, rec := range updatedRecommendation {
		// establish expected reasoning
		expectedFailureReasoning := fmt.Sprintf("Cannot support 2 sharded objects on a machine with %d cores"+
			" and %dGiB memory.", rec.VCPUsPerInstance, rec.VCPUsPerInstance*rec.MemoryPerCore)
		assert.Equal(t, expectedFailureReasoning, rec.FailureReasoning)
	}
}

/*
===== 	Test functions to test findNumNodesNeededBasedOnThroughputRequirement function	=====
*/
// validate that the throughput requirements are supported and no scaling is needed
func TestFindNumNodesNeededBasedOnThroughputRequirement_CanSupportOps(t *testing.T) {
	// Define test data
	sourceIndexMetadata := []SourceDBMetadata{
		{
			ObjectName: "table1", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
			ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 100},
			WritesPerSec: sql.NullInt64{Valid: true, Int64: 50},
		},
		{
			ObjectName: "table2", Size: sql.NullFloat64{Float64: 20.0, Valid: true},
			ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 200},
			WritesPerSec: sql.NullInt64{Valid: true, Int64: 100},
		},
	}

	shardedThroughput := []ExpDataThroughput{
		{
			numCores:                   sql.NullFloat64{Float64: 4},
			maxSupportedSelectsPerCore: sql.NullFloat64{Float64: 200},
			maxSupportedInsertsPerCore: sql.NullFloat64{Float64: 100},
			selectConnPerNode:          sql.NullInt64{Int64: 50},
			insertConnPerNode:          sql.NullInt64{Int64: 25},
		},
	}

	recommendation := map[int]IntermediateRecommendation{
		4: {
			ColocatedTables: []SourceDBMetadata{},
			ShardedTables: []SourceDBMetadata{
				{
					ObjectName: "table1", Size: sql.NullFloat64{Float64: 10.0, Valid: true},
					ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 100},
					WritesPerSec: sql.NullInt64{Valid: true, Int64: 50},
				},
			},
			OptimalSelectConnectionsPerNode: 50,
			OptimalInsertConnectionsPerNode: 25,
		},
	}

	// Run the function
	updatedRecommendation :=
		findNumNodesNeededBasedOnThroughputRequirement(sourceIndexMetadata, shardedThroughput, recommendation)

	// for 4 cores data, expected results are
	var expectedOptimalSelectConnectionsPerNode int64 = 50
	var expectedOptimalInsertConnectionsPerNode int64 = 25
	var expectedNumNodesNeeded float64 = 3

	// Check the results
	recommendationToVerify := updatedRecommendation[4]
	assert.Equal(t, expectedOptimalSelectConnectionsPerNode, recommendationToVerify.OptimalSelectConnectionsPerNode)
	assert.Equal(t, expectedOptimalInsertConnectionsPerNode, recommendationToVerify.OptimalInsertConnectionsPerNode)
	assert.Equal(t, expectedNumNodesNeeded, recommendationToVerify.NumNodes)
}

// validate that the throughput requirements cannot be supported by existing nodes and scaling is needed
func TestFindNumNodesNeededBasedOnThroughputRequirement_NeedMoreNodes(t *testing.T) {
	// Define test data
	var sourceIndexMetadata []SourceDBMetadata

	shardedLimits := []ExpDataThroughput{
		{
			numCores:                   sql.NullFloat64{Float64: 4},
			maxSupportedSelectsPerCore: sql.NullFloat64{Float64: 200},
			maxSupportedInsertsPerCore: sql.NullFloat64{Float64: 100},
		},
	}

	recommendation := map[int]IntermediateRecommendation{
		4: {
			ShardedTables: []SourceDBMetadata{
				{
					ObjectName: "table2", Size: sql.NullFloat64{Float64: 20.0, Valid: true},
					ReadsPerSec:  sql.NullInt64{Valid: true, Int64: 2000},
					WritesPerSec: sql.NullInt64{Valid: true, Int64: 5000},
				},
			},
			VCPUsPerInstance: 4,
			MemoryPerCore:    4,
		},
	}

	// Run the function
	updatedRecommendation :=
		findNumNodesNeededBasedOnThroughputRequirement(sourceIndexMetadata, shardedLimits, recommendation)

	// validate the expected number of nodes
	assert.Equal(t, updatedRecommendation[4].NumNodes, float64(15))
}

/*
===== 	Test functions to test findNumNodesNeededBasedOnTabletsRequired function	=====
*/
// validate that the tablets requirements are supported and no scaling is needed
func TestFindNumNodesNeededBasedOnTabletsRequired_CanSupportTablets(t *testing.T) {
	// Define test data
	var sourceIndexMetadata []SourceDBMetadata
	shardedLimits := []ExpDataShardedLimit{
		{
			numCores:              sql.NullFloat64{Float64: 4, Valid: true},
			maxSupportedNumTables: sql.NullInt64{Int64: 20, Valid: true},
		},
	}
	recommendation := map[int]IntermediateRecommendation{
		4: {
			ColocatedTables: []SourceDBMetadata{},
			ShardedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 10, Valid: true}},
				{SchemaName: "public", ObjectName: "table2", Size: sql.NullFloat64{Float64: 60, Valid: true}},
			},
			VCPUsPerInstance: 4,
			NumNodes:         3,
		},
	}

	// Run the function
	updatedRecommendation :=
		findNumNodesNeededBasedOnTabletsRequired(sourceIndexMetadata, shardedLimits, recommendation)

	// check if the num nodes in updated recommendation is same as before(3) meaning no scaling is required
	assert.Equal(t, float64(3), updatedRecommendation[4].NumNodes)
}

// validate that the tablets cannot be supported by existing nodes and scaling is needed
func TestFindNumNodesNeededBasedOnTabletsRequired_NeedMoreNodes(t *testing.T) {
	// Define test data
	sourceIndexMetadata := []SourceDBMetadata{
		{
			SchemaName: "public", ObjectName: "index1", Size: sql.NullFloat64{Float64: 7, Valid: true},
			ParentTableName: sql.NullString{String: "public.table1"},
		},
		{
			SchemaName: "public", ObjectName: "index2", Size: sql.NullFloat64{Float64: 3, Valid: true},
			ParentTableName: sql.NullString{String: "public.table2"},
		},
	}
	shardedLimits := []ExpDataShardedLimit{
		{
			numCores:              sql.NullFloat64{Float64: 4, Valid: true},
			maxSupportedNumTables: sql.NullInt64{Int64: 20, Valid: true},
		},
	}
	recommendation := map[int]IntermediateRecommendation{
		4: {
			ColocatedTables: []SourceDBMetadata{},
			ShardedTables: []SourceDBMetadata{
				{SchemaName: "public", ObjectName: "table1", Size: sql.NullFloat64{Float64: 125, Valid: true}},
				{SchemaName: "public", ObjectName: "table2", Size: sql.NullFloat64{Float64: 60, Valid: true}},
			},
			VCPUsPerInstance: 4,
			NumNodes:         3,
		},
	}

	// Run the function
	updatedRecommendation :=
		findNumNodesNeededBasedOnTabletsRequired(sourceIndexMetadata, shardedLimits, recommendation)

	// check if the num nodes in updated recommendation has increased. Meaning scaling is required.
	assert.Equal(t, float64(6), updatedRecommendation[4].NumNodes)
}

/*
===== 	Test functions to test pickBestRecommendation function	=====
*/
// validate if the recommendation with optimal nodes and cores is picked up
func TestPickBestRecommendation_PickOneWithOptimalNodesAndCores(t *testing.T) {
	recommendations := map[int]IntermediateRecommendation{
		4: {
			VCPUsPerInstance: 4,
			NumNodes:         10,
			FailureReasoning: "",
		},
		8: {
			VCPUsPerInstance: 8,
			NumNodes:         3,
			FailureReasoning: "",
		},
	}
	bestPick := pickBestRecommendation(recommendations)
	// validate the best recommendation which is 8 vcpus per instance is picked up
	assert.Equal(t, 8, bestPick.VCPUsPerInstance)
}

// validate if the recommendation with optimal nodes is picked up when some of the recommendation has failure reasoning
func TestPickBestRecommendation_PickOneWithOptimalNodesAndCoresWhenSomeHasFailures(t *testing.T) {
	recommendations := map[int]IntermediateRecommendation{
		4: {
			VCPUsPerInstance: 4,
			NumNodes:         10,
			FailureReasoning: "has some failure",
		},
		8: {
			VCPUsPerInstance: 8,
			NumNodes:         3,
			FailureReasoning: "has some failure as well",
		},
		16: {
			VCPUsPerInstance: 16,
			NumNodes:         3,
			FailureReasoning: "",
		},
	}
	bestPick := pickBestRecommendation(recommendations)
	// validate the best recommendation which is 16 vcpus per instance is picked up
	assert.Equal(t, 16, bestPick.VCPUsPerInstance)
}

// validate if the recommendation with optimal nodes and cores is picked up for showing failure reasoning
func TestPickBestRecommendation_PickLastMaxCoreRecommendationWhenNoneCanSupport(t *testing.T) {
	recommendations := map[int]IntermediateRecommendation{
		4: {
			VCPUsPerInstance: 4,
			NumNodes:         10,
			FailureReasoning: "has some failure reasoning",
		},
		8: {
			VCPUsPerInstance: 8,
			NumNodes:         3,
			FailureReasoning: "has some failure reasoning",
		},
		16: {
			VCPUsPerInstance: 16,
			NumNodes:         3,
			FailureReasoning: "has some failure reasoning",
		},
	}
	bestPick := pickBestRecommendation(recommendations)
	// validate the best recommendation which is 16 vcpus per instance is picked up
	assert.Equal(t, 16, bestPick.VCPUsPerInstance)
}

/*
===== 	Test functions to test calculateTimeTakenAndParallelJobsForImportColocatedObjects function	=====
*/
// validate the formula to calculate the import time for Colocated Objects
func TestCalculateTimeTakenAndParallelJobsForImportColocatedObjects_ValidateFormulaToCalculateImportTime(t *testing.T) {
	db, mock := createMockDB(t)
	// Define the mock response for the query
	rows := sqlmock.NewRows([]string{"csv_size_gb", "migration_time_secs", "parallel_threads"}).
		AddRow(100, 6000, 4)
	mock.ExpectQuery(
		"(?i)SELECT csv_size_gb, migration_time_secs, parallel_threads FROM .* WHERE num_cores = .*").
		WithArgs(4, 4, 50.0, 4).
		WillReturnRows(rows)

	// Define test data
	dbObjects := []SourceDBMetadata{
		{Size: sql.NullFloat64{Float64: 30.0, Valid: true}},
		{Size: sql.NullFloat64{Float64: 20.0, Valid: true}},
	}

	// Call the function
	estimatedTime, parallelJobs, err :=
		calculateTimeTakenAndParallelJobsForImportColocatedObjects(dbObjects, 4, 4, db)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Define expected results
	expectedTime := 50.0 // Calculated as ((6000 * 50) / 100) / 60
	expectedJobs := int64(4)
	if estimatedTime != expectedTime || parallelJobs != expectedJobs {
		t.Errorf("calculateTimeTakenAndParallelJobsForImport() = (%v, %v), want (%v, %v)",
			estimatedTime, parallelJobs, expectedTime, expectedJobs)
	}

	// Ensure all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

/*
===== 	Test functions to test calculateTimeTakenAndParallelJobsForImportShardedObjects function	=====
*/
// validate the formula to calculate the import time for sharded table without index
func TestCalculateTimeTakenAndParallelJobsForImportShardedObjects_ValidateImportTimeTableWithoutIndex(t *testing.T) {
	// Define test data
	shardedTables := []SourceDBMetadata{
		{ObjectName: "table0", SchemaName: "public", Size: sql.NullFloat64{Float64: 23.0, Valid: true}},
	}
	var sourceIndexMetadata []SourceDBMetadata
	shardedLoadTimes := []ExpDataLoadTime{
		{csvSizeGB: sql.NullFloat64{Float64: 19}, migrationTimeSecs: sql.NullFloat64{Float64: 1134}, parallelThreads: sql.NullInt64{Int64: 1}},
		{csvSizeGB: sql.NullFloat64{Float64: 29}, migrationTimeSecs: sql.NullFloat64{Float64: 1657}, parallelThreads: sql.NullInt64{Int64: 1}},
	}
	var indexImpacts []ExpDataLoadTimeIndexImpact
	// Call the function
	estimatedTime, parallelJobs, err :=
		calculateTimeTakenAndParallelJobsForImport(shardedTables, sourceIndexMetadata, shardedLoadTimes,
			indexImpacts, SHARDED)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Define expected results
	// Calculated as table0: 1 * ((1134 * 23) / 19) / 60
	expectedTime := 23.0
	expectedJobs := int64(1)
	if estimatedTime != expectedTime || parallelJobs != expectedJobs {
		t.Errorf("calculateTimeTakenAndParallelJobsForImport() = (%v, %v), want (%v, %v)",
			estimatedTime, parallelJobs, expectedTime, expectedJobs)
	}

}

// validate the formula to calculate the import time for sharded table with one index
func TestCalculateTimeTakenAndParallelJobsForImportShardedObjects_ValidateImportTimeTableWithOneIndex(t *testing.T) {
	// Define test data
	shardedTables := []SourceDBMetadata{
		{ObjectName: "table0", SchemaName: "public", Size: sql.NullFloat64{Float64: 23.0, Valid: true}},
	}
	sourceIndexMetadata := []SourceDBMetadata{
		{ObjectName: "table0_idx1", ParentTableName: sql.NullString{Valid: true, String: "public.table0"}},
	}
	shardedLoadTimes := []ExpDataLoadTime{
		{csvSizeGB: sql.NullFloat64{Float64: 19}, migrationTimeSecs: sql.NullFloat64{Float64: 1134}, parallelThreads: sql.NullInt64{Int64: 1}},
		{csvSizeGB: sql.NullFloat64{Float64: 29}, migrationTimeSecs: sql.NullFloat64{Float64: 1657}, parallelThreads: sql.NullInt64{Int64: 1}},
	}
	indexImpacts := []ExpDataLoadTimeIndexImpact{
		{numIndexes: sql.NullFloat64{Float64: 1}, multiplicationFactorSharded: sql.NullFloat64{Float64: 1.76}},
	}

	// Call the function
	estimatedTime, parallelJobs, err :=
		calculateTimeTakenAndParallelJobsForImport(shardedTables, sourceIndexMetadata, shardedLoadTimes,
			indexImpacts, SHARDED)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Define expected results
	// Calculated as table0: 1.76 * ((1134 * 23) / 19) / 60
	expectedTime := 41.0 // double the time required when there are no indexes.
	expectedJobs := int64(1)
	if estimatedTime != expectedTime || parallelJobs != expectedJobs {
		t.Errorf("calculateTimeTakenAndParallelJobsForImport() = (%v, %v), want (%v, %v)",
			estimatedTime, parallelJobs, expectedTime, expectedJobs)
	}

}

// validate the formula to calculate the import time for sharded table with 5 indexes
func TestCalculateTimeTakenAndParallelJobsForImportShardedObjects_ValidateImportTimeTableWithFiveIndexes(t *testing.T) {
	// Define test data
	shardedTables := []SourceDBMetadata{
		{ObjectName: "table0", SchemaName: "public", Size: sql.NullFloat64{Float64: 23.0, Valid: true}},
	}
	sourceIndexMetadata := []SourceDBMetadata{
		{ObjectName: "table0_idx1", ParentTableName: sql.NullString{Valid: true, String: "public.table0"}},
		{ObjectName: "table0_idx2", ParentTableName: sql.NullString{Valid: true, String: "public.table0"}},
		{ObjectName: "table0_idx3", ParentTableName: sql.NullString{Valid: true, String: "public.table0"}},
		{ObjectName: "table0_idx4", ParentTableName: sql.NullString{Valid: true, String: "public.table0"}},
		{ObjectName: "table0_idx5", ParentTableName: sql.NullString{Valid: true, String: "public.table0"}},
	}
	shardedLoadTimes := []ExpDataLoadTime{
		{csvSizeGB: sql.NullFloat64{Float64: 19}, migrationTimeSecs: sql.NullFloat64{Float64: 1134}, parallelThreads: sql.NullInt64{Int64: 1}},
		{csvSizeGB: sql.NullFloat64{Float64: 29}, migrationTimeSecs: sql.NullFloat64{Float64: 1657}, parallelThreads: sql.NullInt64{Int64: 1}},
	}

	indexImpacts := []ExpDataLoadTimeIndexImpact{
		{numIndexes: sql.NullFloat64{Float64: 1}, multiplicationFactorSharded: sql.NullFloat64{Float64: 1.76}},
		{numIndexes: sql.NullFloat64{Float64: 5}, multiplicationFactorSharded: sql.NullFloat64{Float64: 4.6}},
	}

	// Call the function
	estimatedTime, parallelJobs, err :=
		calculateTimeTakenAndParallelJobsForImport(shardedTables, sourceIndexMetadata, shardedLoadTimes,
			indexImpacts, SHARDED)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Define expected results
	// Calculated as table0: 4.6 * ((1134 * 23) / 19) / 60
	expectedTime := 106.0
	expectedJobs := int64(1)
	if estimatedTime != expectedTime || parallelJobs != expectedJobs {
		t.Errorf("calculateTimeTakenAndParallelJobsForImport() = (%v, %v), want (%v, %v)",
			estimatedTime, parallelJobs, expectedTime, expectedJobs)
	}

}

/*
===== 	Test functions to test getReasoning function	=====
*/
// validate reasoning when there are no objects(empty colocated and sharded objects)
func TestGetReasoning_NoObjects(t *testing.T) {
	recommendation := IntermediateRecommendation{VCPUsPerInstance: 4, MemoryPerCore: 16}
	var shardedObjects []SourceDBMetadata
	var colocatedObjects []SourceDBMetadata
	expected := "Recommended instance type with 4 vCPU and 64 GiB memory could fit  Non leaf partition tables/indexes and unsupported tables/indexes were not considered."

	result := getReasoning(recommendation, shardedObjects, 0, colocatedObjects, 0)
	if result != expected {
		t.Errorf("Expected %q but got %q", expected, result)
	}
}

// validate reasoning when there are only colocated objects
func TestGetReasoning_OnlyColocatedObjects(t *testing.T) {
	recommendation := IntermediateRecommendation{VCPUsPerInstance: 8, MemoryPerCore: 8}
	var shardedObjects []SourceDBMetadata
	colocatedObjects := []SourceDBMetadata{
		{Size: sql.NullFloat64{Valid: true, Float64: 50.0}, ReadsPerSec: sql.NullInt64{Valid: true, Int64: 1000}, WritesPerSec: sql.NullInt64{Valid: true, Int64: 500}},
		{Size: sql.NullFloat64{Valid: true, Float64: 30.0}, ReadsPerSec: sql.NullInt64{Valid: true, Int64: 2000}, WritesPerSec: sql.NullInt64{Valid: true, Int64: 1500}},
	}
	expected := "Recommended instance type with 8 vCPU and 64 GiB memory could fit 2 objects (2 tables and 0 " +
		"explicit/implicit indexes) with 80.00 GB size and throughput requirement of 3000 reads/sec and " +
		"2000 writes/sec as colocated. Non leaf partition tables/indexes and unsupported tables/indexes were not considered."

	result := getReasoning(recommendation, shardedObjects, 0, colocatedObjects, 0)
	if result != expected {
		t.Errorf("Expected %q but got %q", expected, result)
	}
}

// validate reasoning when there are only sharded objects
func TestGetReasoning_OnlyShardedObjects(t *testing.T) {
	recommendation := IntermediateRecommendation{VCPUsPerInstance: 16, MemoryPerCore: 4}
	shardedObjects := []SourceDBMetadata{
		{Size: sql.NullFloat64{Valid: true, Float64: 100.0}, ReadsPerSec: sql.NullInt64{Valid: true, Int64: 4000}, WritesPerSec: sql.NullInt64{Valid: true, Int64: 3000}},
		{Size: sql.NullFloat64{Valid: true, Float64: 200.0}, ReadsPerSec: sql.NullInt64{Valid: true, Int64: 5000}, WritesPerSec: sql.NullInt64{Valid: true, Int64: 4000}},
	}
	var colocatedObjects []SourceDBMetadata
	expected := "Recommended instance type with 16 vCPU and 64 GiB memory could fit 2 objects (2 tables and 0 " +
		"explicit/implicit indexes) with 300.00 GB size and throughput requirement of 9000 reads/sec and " +
		"7000 writes/sec as sharded. Non leaf partition tables/indexes and unsupported tables/indexes were not considered."

	result := getReasoning(recommendation, shardedObjects, 0, colocatedObjects, 0)
	if result != expected {
		t.Errorf("Expected %q but got %q", expected, result)
	}
}

// validate reasoning when there are colocated and sharded objects
func TestGetReasoning_ColocatedAndShardedObjects(t *testing.T) {
	recommendation := IntermediateRecommendation{VCPUsPerInstance: 32, MemoryPerCore: 2}
	shardedObjects := []SourceDBMetadata{
		{Size: sql.NullFloat64{Valid: true, Float64: 150.0}, ReadsPerSec: sql.NullInt64{Valid: true, Int64: 7000}, WritesPerSec: sql.NullInt64{Valid: true, Int64: 6000}},
	}
	colocatedObjects := []SourceDBMetadata{
		{Size: sql.NullFloat64{Valid: true, Float64: 70.0}, ReadsPerSec: sql.NullInt64{Valid: true, Int64: 3000}, WritesPerSec: sql.NullInt64{Valid: true, Int64: 2000}},
		{Size: sql.NullFloat64{Valid: true, Float64: 50.0}, ReadsPerSec: sql.NullInt64{Valid: true, Int64: 2000}, WritesPerSec: sql.NullInt64{Valid: true, Int64: 1000}},
	}
	expected := "Recommended instance type with 32 vCPU and 64 GiB memory could fit 2 objects (2 tables and 0 " +
		"explicit/implicit indexes) with 120.00 GB size and throughput requirement of 5000 reads/sec and " +
		"3000 writes/sec as colocated. Rest 1 objects (1 tables and 0 explicit/implicit indexes) with 150.00 GB " +
		"size and throughput requirement of 7000 reads/sec and 6000 writes/sec need to be migrated as range " +
		"partitioned tables. Non leaf partition tables/indexes and unsupported tables/indexes were not considered."

	result := getReasoning(recommendation, shardedObjects, 0, colocatedObjects, 0)
	if result != expected {
		t.Errorf("Expected %q but got %q", expected, result)
	}
}

// validate reasoning when there are colocated and sharded objects with indexes
func TestGetReasoning_Indexes(t *testing.T) {
	recommendation := IntermediateRecommendation{VCPUsPerInstance: 4, MemoryPerCore: 16}
	shardedObjects := []SourceDBMetadata{
		{Size: sql.NullFloat64{Valid: true, Float64: 200.0}, ReadsPerSec: sql.NullInt64{Valid: true, Int64: 6000}, WritesPerSec: sql.NullInt64{Valid: true, Int64: 4000}},
	}
	colocatedObjects := []SourceDBMetadata{
		{Size: sql.NullFloat64{Valid: true, Float64: 100.0}, ReadsPerSec: sql.NullInt64{Valid: true, Int64: 3000}, WritesPerSec: sql.NullInt64{Valid: true, Int64: 2000}},
	}
	expected := "Recommended instance type with 4 vCPU and 64 GiB memory could fit 1 objects (0 tables and " +
		"1 explicit/implicit indexes) with 100.00 GB size and throughput requirement of 3000 reads/sec and " +
		"2000 writes/sec as colocated. Rest 1 objects (0 tables and 1 explicit/implicit indexes) with 200.00 GB size " +
		"and throughput requirement of 6000 reads/sec and 4000 writes/sec need to be migrated as range " +
		"partitioned tables. Non leaf partition tables/indexes and unsupported tables/indexes were not considered."

	result := getReasoning(recommendation, shardedObjects, 1, colocatedObjects, 1)
	if result != expected {
		t.Errorf("Expected %q but got %q", expected, result)
	}
}

/*
===== 	Test functions to test getThresholdAndTablets function	=====
*/
func TestGetThresholdAndTablets(t *testing.T) {
	tests := []struct {
		previousNumNodes  float64
		sizeGB            float64
		expectedThreshold float64
		expectedTablets   int
	}{
		// test data where previousNumNodes is 3
		{3, 0.512, LOW_PHASE_SIZE_THRESHOLD_GB, 1},
		{3, 1, LOW_PHASE_SIZE_THRESHOLD_GB, 2},
		{3, 1.1, LOW_PHASE_SIZE_THRESHOLD_GB, 3},
		{3, 2, HIGH_PHASE_SIZE_THRESHOLD_GB, 3},
		{3, 10, HIGH_PHASE_SIZE_THRESHOLD_GB, 3},
		{3, 229, HIGH_PHASE_SIZE_THRESHOLD_GB, 23},
		{3, 241, HIGH_PHASE_SIZE_THRESHOLD_GB, 25},
		{3, 711, HIGH_PHASE_SIZE_THRESHOLD_GB, 72},
		{3, 7180, FINAL_PHASE_SIZE_THRESHOLD_GB, 72},
		{3, 7201, FINAL_PHASE_SIZE_THRESHOLD_GB, 73},
		// test data where previousNumNodes is 4
		{4, 0.512, LOW_PHASE_SIZE_THRESHOLD_GB, 1},
		{4, 1, LOW_PHASE_SIZE_THRESHOLD_GB, 2},
		{4, 1.1, LOW_PHASE_SIZE_THRESHOLD_GB, 3},
		{4, 2, LOW_PHASE_SIZE_THRESHOLD_GB, 4},
		{4, 10, HIGH_PHASE_SIZE_THRESHOLD_GB, 4},
		{4, 229, HIGH_PHASE_SIZE_THRESHOLD_GB, 23},
		{4, 241, HIGH_PHASE_SIZE_THRESHOLD_GB, 25},
		{4, 949, HIGH_PHASE_SIZE_THRESHOLD_GB, 95},
		{4, 951, HIGH_PHASE_SIZE_THRESHOLD_GB, 96},
		{4, 961, FINAL_PHASE_SIZE_THRESHOLD_GB, 96},
	}

	for _, test := range tests {
		threshold, tablets := getThresholdAndTablets(test.previousNumNodes, test.sizeGB)
		if threshold != test.expectedThreshold || tablets != test.expectedTablets {
			t.Errorf("getThresholdAndTablets(%v, %v) = (%v, %v); want (%v, %v)",
				test.previousNumNodes, test.sizeGB, threshold, tablets, test.expectedThreshold, test.expectedTablets)
		}
	}

}

/*
====================HELPER FUNCTIONS====================
*/

// Round rounds a float64 to n decimal places.
func Round(val float64, precision int) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}

func createMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	return db, mock
}
