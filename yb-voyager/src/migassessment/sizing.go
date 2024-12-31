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
	_ "embed"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/samber/lo"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"

	log "github.com/sirupsen/logrus"
)

type SourceDBMetadata struct {
	SchemaName      string          `db:"schema_name"`
	ObjectName      string          `db:"object_name"`
	RowCount        sql.NullFloat64 `db:"row_count,string"`
	ColumnCount     sql.NullInt64   `db:"column_count,string"`
	Reads           sql.NullInt64   `db:"reads,string"`
	Writes          sql.NullInt64   `db:"writes,string"`
	ReadsPerSec     sql.NullInt64   `db:"reads_per_second,string"`
	WritesPerSec    sql.NullInt64   `db:"writes_per_second,string"`
	IsIndex         bool            `db:"is_index,string"`
	ParentTableName sql.NullString  `db:"parent_table_name"`
	Size            sql.NullFloat64 `db:"size_in_bytes,string"`
}

type ExpDataShardedLimit struct {
	numCores              sql.NullFloat64 `db:"num_cores,string"`
	memPerCore            sql.NullFloat64 `db:"mem_per_core,string"`
	maxSupportedNumTables sql.NullInt64   `db:"max_num_tables,string"`
}

type ExpDataColocatedLimit struct {
	maxColocatedSizeSupported sql.NullFloat64 `db:"max_colocated_db_size_gb,string"`
	numCores                  sql.NullFloat64 `db:"num_cores,string"`
	memPerCore                sql.NullFloat64 `db:"mem_per_core,string"`
	maxSupportedNumTables     sql.NullInt64   `db:"max_num_tables,string"`
	minSupportedNumTables     sql.NullFloat64 `db:"min_num_tables,string"`
}

type ExpDataThroughput struct {
	numCores                   sql.NullFloat64 `db:"num_cores,string"`
	memPerCore                 sql.NullFloat64 `db:"mem_per_core,string"`
	maxSupportedSelectsPerCore sql.NullFloat64 `db:"max_selects_per_core,string"`
	maxSupportedInsertsPerCore sql.NullFloat64 `db:"max_inserts_per_core,string"`
	selectConnPerNode          sql.NullInt64   `db:"select_conn_per_node,string"`
	insertConnPerNode          sql.NullInt64   `db:"insert_conn_per_node,string"`
}

type ExpDataLoadTime struct {
	csvSizeGB         sql.NullFloat64 `db:"csv_size_gb,string"`
	migrationTimeSecs sql.NullFloat64 `db:"migration_time_secs,string"`
	rowCount          sql.NullFloat64 `db:"row_count,string"`
}

type ExpDataLoadTimeIndexImpact struct {
	numIndexes                    sql.NullFloat64 `db:"num_indexes,string"`
	multiplicationFactorSharded   sql.NullFloat64 `db:"multiplication_factor_sharded,string"`
	multiplicationFactorColocated sql.NullFloat64 `db:"multiplication_factor_colocated,string"`
}

type ExpDataLoadTimeColumnsImpact struct {
	numColumns                    sql.NullInt64   `db:"number_of_columns,string"`
	multiplicationFactorSharded   sql.NullFloat64 `db:"multiplication_factor_sharded,string"`
	multiplicationFactorColocated sql.NullFloat64 `db:"multiplication_factor_colocated,string"`
}

type IntermediateRecommendation struct {
	ColocatedTables                 []SourceDBMetadata
	ShardedTables                   []SourceDBMetadata
	ColocatedSize                   float64
	ShardedSize                     float64
	NumNodes                        float64
	VCPUsPerInstance                int
	MemoryPerCore                   int
	OptimalSelectConnectionsPerNode int64
	OptimalInsertConnectionsPerNode int64
	EstimatedTimeInMinForImport     float64
	FailureReasoning                string
}

const (
	COLOCATED_LIMITS_TABLE         = "colocated_limits"
	COLOCATED_SIZING_TABLE         = "colocated_sizing"
	SHARDED_SIZING_TABLE           = "sharded_sizing"
	COLOCATED_LOAD_TIME_TABLE      = "colocated_load_time"
	SHARDED_LOAD_TIME_TABLE        = "sharded_load_time"
	LOAD_TIME_INDEX_IMPACT_TABLE   = "load_time_index_impact"
	LOAD_TIME_COLUMNS_IMPACT_TABLE = "load_time_columns_impact"
	// GITHUB_RAW_LINK use raw github link to fetch the file from repository using the api:
	// https://raw.githubusercontent.com/{username-or-organization}/{repository}/{branch}/{path-to-file}
	GITHUB_RAW_LINK                 = "https://raw.githubusercontent.com/yugabyte/yb-voyager/main/yb-voyager/src/migassessment/resources"
	EXPERIMENT_DATA_FILENAME        = "yb_2024_0_source.db"
	DBS_DIR                         = "dbs"
	SIZE_UNIT_GB                    = "GB"
	SIZE_UNIT_MB                    = "MB"
	LOW_PHASE_SHARD_COUNT           = 1
	LOW_PHASE_SIZE_THRESHOLD_GB     = 0.512
	HIGH_PHASE_SHARD_COUNT          = 24
	HIGH_PHASE_SIZE_THRESHOLD_GB    = 10
	FINAL_PHASE_SIZE_THRESHOLD_GB   = 100
	MAX_TABLETS_PER_TABLE           = 256
	PREFER_REMOTE_EXPERIMENT_DB     = false
	COLOCATED_MAX_INDEXES_THRESHOLD = 2
	// COLOCATED / SHARDED Object types
	COLOCATED = "colocated"
	SHARDED   = "sharded"
)

func getExperimentDBPath() string {
	return filepath.Join(AssessmentDir, DBS_DIR, EXPERIMENT_DATA_FILENAME)
}

//go:embed resources/yb_2024_0_source.db
var experimentData20240 []byte

var SourceMetadataObjectTypesToUse = []string{
	"%table%",
	"%index%",
	"materialized view",
}

func SizingAssessment() error {

	log.Infof("loading metadata files for sharding assessment")
	sourceTableMetadata, sourceIndexMetadata, _, err := loadSourceMetadata(GetSourceMetadataDBFilePath())
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("failed to load source metadata: %v", err)
		return fmt.Errorf("failed to load source metadata: %w", err)
	}

	experimentDB, err := createConnectionToExperimentData()
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("failed to connect to experiment data: %v", err)
		return fmt.Errorf("failed to connect to experiment data: %w", err)
	}

	colocatedLimits, err := loadColocatedLimit(experimentDB)
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("error fetching the colocated limits: %v", err)
		return fmt.Errorf("error fetching the colocated limits: %w", err)
	}

	colocatedThroughput, err := loadExpDataThroughput(experimentDB, COLOCATED_SIZING_TABLE)
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("error fetching the colocated throughput: %v", err)
		return fmt.Errorf("error fetching the colocated throughput: %w", err)
	}

	shardedLimits, err := loadShardedTableLimits(experimentDB)
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("error fetching the sharded limits: %v", err)
		return fmt.Errorf("error fetching the colocated limits: %w", err)
	}

	shardedThroughput, err := loadExpDataThroughput(experimentDB, SHARDED_SIZING_TABLE)
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("error fetching the sharded throughput: %v", err)
		return fmt.Errorf("error fetching the sharded throughput: %w", err)
	}

	sizingRecommendationPerCore := createSizingRecommendationStructure(colocatedLimits)

	sizingRecommendationPerCore = shardingBasedOnTableSizeAndCount(sourceTableMetadata, sourceIndexMetadata,
		colocatedLimits, sizingRecommendationPerCore)

	sizingRecommendationPerCore = shardingBasedOnOperations(sourceIndexMetadata, colocatedThroughput, sizingRecommendationPerCore)

	sizingRecommendationPerCore = checkShardedTableLimit(sourceIndexMetadata, shardedLimits, sizingRecommendationPerCore)

	sizingRecommendationPerCore = findNumNodesNeededBasedOnThroughputRequirement(sourceIndexMetadata, shardedThroughput, sizingRecommendationPerCore)

	sizingRecommendationPerCore = findNumNodesNeededBasedOnTabletsRequired(sourceIndexMetadata, shardedLimits, sizingRecommendationPerCore)
	finalSizingRecommendation := pickBestRecommendation(sizingRecommendationPerCore)

	if finalSizingRecommendation.FailureReasoning != "" {
		SizingReport.FailureReasoning = finalSizingRecommendation.FailureReasoning
		return fmt.Errorf("error picking best recommendation: %v", finalSizingRecommendation.FailureReasoning)
	}

	colocatedObjects, cumulativeIndexCountColocated :=
		getListOfIndexesAlongWithObjects(finalSizingRecommendation.ColocatedTables, sourceIndexMetadata)
	shardedObjects, cumulativeIndexCountSharded :=
		getListOfIndexesAlongWithObjects(finalSizingRecommendation.ShardedTables, sourceIndexMetadata)

	// get load times data from experimental database for colocated Tables
	colocatedLoadTimes, err := getExpDataLoadTime(experimentDB, finalSizingRecommendation.VCPUsPerInstance,
		finalSizingRecommendation.MemoryPerCore, COLOCATED_LOAD_TIME_TABLE)
	if err != nil {
		return fmt.Errorf("error while fetching colocated load time info: %w", err)
	}

	// get load times data from experimental database for sharded Tables
	shardedLoadTimes, err := getExpDataLoadTime(experimentDB, finalSizingRecommendation.VCPUsPerInstance,
		finalSizingRecommendation.MemoryPerCore, SHARDED_LOAD_TIME_TABLE)
	if err != nil {
		return fmt.Errorf("error while fetching sharded load time info: %w", err)
	}

	// get experimental data for impact of indexes on import time
	indexImpactOnLoadTimeCommon, err := getExpDataIndexImpactOnLoadTime(experimentDB,
		finalSizingRecommendation.VCPUsPerInstance, finalSizingRecommendation.MemoryPerCore)
	if err != nil {
		return fmt.Errorf("error while fetching experiment data for impact of index on load time: %w", err)
	}

	// get experimental data for impact of number of columns on import time
	columnsImpactOnLoadTimeCommon, err := getExpDataNumColumnsImpactOnLoadTime(experimentDB,
		finalSizingRecommendation.VCPUsPerInstance, finalSizingRecommendation.MemoryPerCore)
	if err != nil {
		return fmt.Errorf("error while fetching experiment data for impact of number of columns on load time: %w", err)
	}

	// calculate time taken for colocated import
	importTimeForColocatedObjects, err := calculateTimeTakenForImport(
		finalSizingRecommendation.ColocatedTables, sourceIndexMetadata, colocatedLoadTimes,
		indexImpactOnLoadTimeCommon, columnsImpactOnLoadTimeCommon, COLOCATED)
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("calculate time taken for colocated data import: %v", err)
		return fmt.Errorf("calculate time taken for colocated data import: %w", err)
	}

	// calculate time taken for sharded import
	importTimeForShardedObjects, err := calculateTimeTakenForImport(
		finalSizingRecommendation.ShardedTables, sourceIndexMetadata, shardedLoadTimes,
		indexImpactOnLoadTimeCommon, columnsImpactOnLoadTimeCommon, SHARDED)
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("calculate time taken for sharded data import: %v", err)
		return fmt.Errorf("calculate time taken for sharded data import: %w", err)
	}
	reasoning := getReasoning(finalSizingRecommendation, shardedObjects, cumulativeIndexCountSharded, colocatedObjects,
		cumulativeIndexCountColocated)

	sizingRecommendation := &SizingRecommendation{
		ColocatedTables:                 fetchObjectNames(finalSizingRecommendation.ColocatedTables),
		ShardedTables:                   fetchObjectNames(finalSizingRecommendation.ShardedTables),
		VCPUsPerInstance:                finalSizingRecommendation.VCPUsPerInstance,
		MemoryPerInstance:               finalSizingRecommendation.VCPUsPerInstance * finalSizingRecommendation.MemoryPerCore,
		NumNodes:                        finalSizingRecommendation.NumNodes,
		OptimalSelectConnectionsPerNode: finalSizingRecommendation.OptimalSelectConnectionsPerNode,
		OptimalInsertConnectionsPerNode: finalSizingRecommendation.OptimalInsertConnectionsPerNode,
		ColocatedReasoning:              reasoning,
		EstimatedTimeInMinForImport:     importTimeForColocatedObjects + importTimeForShardedObjects,
	}
	SizingReport.SizingRecommendation = *sizingRecommendation

	return nil
}

/*
pickBestRecommendation selects the best recommendation from a map of recommendations by optimizing for the cores. Hence,
we chose the setup where the number of cores is less.
Parameters:
  - recommendation: A map where the key is the number of vCPUs per instance and the value is an IntermediateRecommendation struct.

Returns:
  - The best IntermediateRecommendation based on the defined criteria.
*/
func pickBestRecommendation(recommendation map[int]IntermediateRecommendation) IntermediateRecommendation {
	// find the one with least number of nodes
	var minCores int = math.MaxUint32
	var finalRecommendation IntermediateRecommendation
	var foundRecommendation bool = false
	var maxCores int = math.MinInt32

	// Iterate over each recommendation
	for _, rec := range recommendation {
		// Update maxCores with the maximum number of vCPUs per instance across recommendations. If none of the cores
		// abe to satisfy the criteria, recommendation with maxCores will be used as final recommendation
		if maxCores < rec.VCPUsPerInstance {
			maxCores = rec.VCPUsPerInstance
		}
		// Check if the recommendation has no failure reasoning (i.e., it's a valid recommendation)
		if rec.FailureReasoning == "" {
			foundRecommendation = true
			// Update finalRecommendation if the current recommendation has fewer cores
			if minCores > int(rec.NumNodes)*rec.VCPUsPerInstance {
				finalRecommendation = rec
				minCores = int(rec.NumNodes) * rec.VCPUsPerInstance
			} else if minCores == int(rec.NumNodes)*rec.VCPUsPerInstance {
				// If the number of cores is the same across machines, recommend the machine with higher core count
				if rec.VCPUsPerInstance > finalRecommendation.VCPUsPerInstance {
					finalRecommendation = rec
				}
			}
		}
	}
	// If no valid recommendation was found, select the recommendation with the maximum number of cores
	if !foundRecommendation {
		finalRecommendation = recommendation[maxCores]
		// notify customers to reach out to the Yugabyte customer support team for further assistance
		finalRecommendation.FailureReasoning = "Unable to determine appropriate sizing recommendation. Reach out to the Yugabyte customer support team at https://support.yugabyte.com for further assistance."
	}

	// Return the best recommendation
	return finalRecommendation
}

/*
findNumNodesNeededBasedOnThroughputRequirement calculates the number of nodes needed based on sharded throughput limits and updates the recommendation accordingly.
Parameters:
  - sourceIndexMetadata: A slice of SourceDBMetadata structs representing source indexes.
  - shardedThroughputSlice: A slice of ExpDataShardedThroughput structs representing sharded throughput limits.
  - recommendation: A map where the key is the number of vCPUs per instance and the value is an IntermediateRecommendation struct.

Returns:
  - An updated map of recommendations with the number of nodes needed.
*/
func findNumNodesNeededBasedOnThroughputRequirement(sourceIndexMetadata []SourceDBMetadata, shardedThroughputSlice []ExpDataThroughput,
	recommendation map[int]IntermediateRecommendation) map[int]IntermediateRecommendation {
	// Iterate over sharded throughput limits
	for _, shardedThroughput := range shardedThroughputSlice {
		// Get previous recommendation for the current num of cores
		previousRecommendation := recommendation[int(shardedThroughput.numCores.Float64)]
		var cumulativeSelectOpsPerSec int64 = 0
		var cumulativeInsertOpsPerSec int64 = 0

		// Calculate cumulative operations per second for sharded tables
		for _, table := range previousRecommendation.ShardedTables {
			// Check and fetch indexes for the current table
			_, _, indexReads, indexWrites := checkAndFetchIndexes(table, sourceIndexMetadata)
			cumulativeSelectOpsPerSec += lo.Ternary(table.ReadsPerSec.Valid, table.ReadsPerSec.Int64, 0) + indexReads
			cumulativeInsertOpsPerSec += lo.Ternary(table.WritesPerSec.Valid, table.WritesPerSec.Int64, 0) + indexWrites
		}

		// Calculate needed cores based on cumulative operations per second
		neededCores :=
			math.Ceil(float64(cumulativeSelectOpsPerSec)/shardedThroughput.maxSupportedSelectsPerCore.Float64 +
				float64(cumulativeInsertOpsPerSec)/shardedThroughput.maxSupportedInsertsPerCore.Float64)

		nodesNeeded := math.Ceil(neededCores / shardedThroughput.numCores.Float64)
		// Assumption: If there are any colocated objects - one node will be utilized as colocated tablet leader.
		// Add it explicitly.
		if len(previousRecommendation.ColocatedTables) > 0 {
			nodesNeeded += 1
		}

		// Assumption: minimum required replication is 3, so minimum nodes recommended would be 3.
		// Choose max of nodes needed and 3
		nodesNeeded = math.Max(nodesNeeded, 3)

		// Update recommendation with the number of nodes needed
		recommendation[int(shardedThroughput.numCores.Float64)] = IntermediateRecommendation{
			ColocatedTables:                 previousRecommendation.ColocatedTables,
			ShardedTables:                   previousRecommendation.ShardedTables,
			VCPUsPerInstance:                previousRecommendation.VCPUsPerInstance,
			MemoryPerCore:                   previousRecommendation.MemoryPerCore,
			NumNodes:                        nodesNeeded,
			OptimalSelectConnectionsPerNode: int64(math.Min(float64(previousRecommendation.OptimalSelectConnectionsPerNode), float64(shardedThroughput.selectConnPerNode.Int64))),
			OptimalInsertConnectionsPerNode: int64(math.Min(float64(previousRecommendation.OptimalInsertConnectionsPerNode), float64(shardedThroughput.insertConnPerNode.Int64))),
			ColocatedSize:                   previousRecommendation.ColocatedSize,
			ShardedSize:                     previousRecommendation.ShardedSize,
			EstimatedTimeInMinForImport:     previousRecommendation.EstimatedTimeInMinForImport,
			FailureReasoning:                previousRecommendation.FailureReasoning,
		}
	}
	// Return updated recommendation map
	return recommendation
}

/*
findNumNodesNeededBasedOnTabletsRequired calculates the number of nodes needed based on tablets required by each
table and its indexes and updates the recommendation accordingly.
Parameters:
  - sourceIndexMetadata: A slice of SourceDBMetadata structs representing source indexes.
  - shardedLimits: A slice of ExpDataShardedThroughput structs representing sharded throughput limits.
  - recommendation: A map where the key is the number of vCPUs per instance and the value is an IntermediateRecommendation struct.

Returns:
  - An updated map of recommendations with the number of nodes needed.
*/
func findNumNodesNeededBasedOnTabletsRequired(sourceIndexMetadata []SourceDBMetadata,
	shardedLimits []ExpDataShardedLimit,
	recommendation map[int]IntermediateRecommendation) map[int]IntermediateRecommendation {
	// Iterate over each intermediate recommendation where failureReasoning is empty
	for i, rec := range recommendation {
		totalTabletsRequired := 0
		if len(rec.ShardedTables) != 0 && rec.FailureReasoning == "" {
			// Iterate over each table and its indexes to find out how many tablets are needed
			for _, table := range rec.ShardedTables {
				_, tabletsRequired := getThresholdAndTablets(rec.NumNodes, lo.Ternary(table.Size.Valid, table.Size.Float64, 0))
				for _, index := range sourceIndexMetadata {
					if index.ParentTableName.Valid && (index.ParentTableName.String == (table.SchemaName + "." + table.ObjectName)) {
						// calculating tablets required for each of the index
						_, tabletsRequiredForIndex := getThresholdAndTablets(rec.NumNodes, lo.Ternary(index.Size.Valid, index.Size.Float64, 0))
						// tablets required for each table is the sum of tablets required for the table and its indexes
						tabletsRequired += tabletsRequiredForIndex
					}
				}
				// adding total tablets required across all tables
				totalTabletsRequired += tabletsRequired
			}
			// assuming table limits is also a tablet limit
			// get shardedLimit of current recommendation
			for _, record := range shardedLimits {
				if record.numCores.Valid && int(record.numCores.Float64) == rec.VCPUsPerInstance {
					// considering RF=3, hence total required tablets would be 3 times(1 tablet leader and 2 followers) the totalTabletsRequired
					// adding 100% buffer for the tablets required by multiplier of 2
					nodesRequired := math.Ceil(float64(totalTabletsRequired*3*2) / float64(record.maxSupportedNumTables.Int64))
					// update recommendation to use the maximum of the existing recommended nodes and nodes calculated based on tablets
					// Caveat: if new nodes required is more than the existing recommended nodes, we would need to
					// re-evaluate tablets required. Although, in this iteration we've skipping re-evaluation.
					rec.NumNodes = math.Max(rec.NumNodes, nodesRequired)
					recommendation[i] = rec
				}
			}
		}
	}
	// return updated recommendations
	return recommendation
}

/*
getThresholdAndTablets determines the size threshold and number of tablets needed for a given table size.

Parameters:
- previousNumNodes: float64 - The number of nodes in the previous recommendation.
- sizeGB: float64 - The size of the table in gigabytes.

Returns:
- float64: The size threshold in gigabytes for the table based on the phase.
- int: The number of tablets needed for the table.

Description:
This function calculates which size threshold applies to a table based on its size and determines the number of tablets required.
Following details/comments are with assumption that the previous recommended nodes is 3.
Similar works for other recommended nodes as well.:
  - For sizes up to the low phase limit (1*3 shards of 512 MB each, up to 1.5 GB), the low phase threshold is used. Where 1 is low phase shard count and 3 is the previous recommended nodes.
  - After 1*3 shards, the high phase threshold is used.
  - Intermediate phase upto 30 GB is calculated based on 3 tablets of 10 GB each.
  - For sizes up to the high phase limit (72(24*3) shards of 10 GB each, up to 720 GB), the high phase threshold is used.
  - For larger sizes, the final phase threshold (100 GB) is used.
*/
func getThresholdAndTablets(previousNumNodes float64, sizeGB float64) (float64, int) {
	var tablets = math.Ceil(sizeGB / LOW_PHASE_SIZE_THRESHOLD_GB)

	if tablets <= (LOW_PHASE_SHARD_COUNT * previousNumNodes) {
		// table size is less than 1.5GB, hence 1*3 tablets of 512MB each will be enough
		return LOW_PHASE_SIZE_THRESHOLD_GB, int(tablets)
	} else {
		// table size is more than 1.5GB.
		// find out the per tablet size if it is less than 10GB which is high phase threshold
		perTabletSize := sizeGB / (LOW_PHASE_SHARD_COUNT * previousNumNodes)
		if perTabletSize <= HIGH_PHASE_SIZE_THRESHOLD_GB {
			// tablet count is still 1*3 but the size of each tablet is less than 10GB(table size < 30GB).
			return HIGH_PHASE_SIZE_THRESHOLD_GB, int(LOW_PHASE_SHARD_COUNT * previousNumNodes)
		} else {
			// table size is > 30GB, hence we need to increase the tablet count
			tablets = math.Ceil(LOW_PHASE_SHARD_COUNT*previousNumNodes + (sizeGB-LOW_PHASE_SHARD_COUNT*previousNumNodes*HIGH_PHASE_SIZE_THRESHOLD_GB)/HIGH_PHASE_SIZE_THRESHOLD_GB)
			if tablets <= (HIGH_PHASE_SHARD_COUNT * previousNumNodes) {
				// this means that table size is less than 720GB, hence 72(24*3) tablets of 10GB each will be enough
				return HIGH_PHASE_SIZE_THRESHOLD_GB, int(tablets)
			} else {
				// table size is more than 720 GB.
				// find out the per tablet size if it is less than 100GB which is final phase threshold
				perTabletSize = sizeGB / HIGH_PHASE_SHARD_COUNT
				if perTabletSize <= FINAL_PHASE_SIZE_THRESHOLD_GB {
					// tablet count is still 72(24*3) but the size of each tablet is less than 100GB(table size < 7200GB).
					return FINAL_PHASE_SIZE_THRESHOLD_GB, int(HIGH_PHASE_SHARD_COUNT * previousNumNodes)
				} else {
					// table size is > 7200GB, hence we need to increase the tablet count
					tablets = math.Ceil(HIGH_PHASE_SHARD_COUNT*previousNumNodes + (sizeGB-HIGH_PHASE_SHARD_COUNT*previousNumNodes*FINAL_PHASE_SIZE_THRESHOLD_GB)/FINAL_PHASE_SIZE_THRESHOLD_GB)
					if tablets <= (MAX_TABLETS_PER_TABLE * previousNumNodes) {
						// this means that table size is less than 76800GB. So 768(256*3) tablets of 100GB each will be enough
						return FINAL_PHASE_SIZE_THRESHOLD_GB, int(tablets)
					} else {
						// to support table size > 76800GB, tablets per table limit in YugabyteDB needs to be
						// set to 0(meaning no limit). Refer doc:
						//https://docs.yugabyte.com/preview/architecture/docdb-sharding/tablet-splitting/#final-phase
						tablets = math.Ceil(MAX_TABLETS_PER_TABLE + (sizeGB-MAX_TABLETS_PER_TABLE*previousNumNodes*FINAL_PHASE_SIZE_THRESHOLD_GB)/FINAL_PHASE_SIZE_THRESHOLD_GB)
						return FINAL_PHASE_SIZE_THRESHOLD_GB, int(tablets)
					}
				}
			}
		}
	}
}

/*
checkShardedTableLimit checks if the total number of sharded tables exceeds the sharded limit for each core configuration.
If the limit is exceeded, it updates the recommendation with a failure reasoning.
Parameters:
  - sourceIndexMetadata: A slice of SourceDBMetadata structs representing source indexes.
  - shardedLimits: A slice of ExpDataShardedLimit structs representing sharded limits.
  - recommendation: A map where the key is the number of vCPUs per instance and the value is an IntermediateRecommendation struct.

Returns:
  - An updated map of recommendations with failure reasoning if the sharded table limit is exceeded.
*/
func checkShardedTableLimit(sourceIndexMetadata []SourceDBMetadata, shardedLimits []ExpDataShardedLimit, recommendation map[int]IntermediateRecommendation) map[int]IntermediateRecommendation {

	for _, shardedLimit := range shardedLimits {
		var totalObjectCount int64 = 0

		// Get previous recommendation for the current cores
		previousRecommendation := recommendation[int(shardedLimit.numCores.Float64)]

		// Calculate total object count for sharded tables
		for _, table := range previousRecommendation.ShardedTables {
			// Check and fetch indexes for the current table
			indexes, _, _, _ := checkAndFetchIndexes(table, sourceIndexMetadata)
			totalObjectCount += int64(len(indexes)) + 1

		}
		// Check if total object count exceeds the max supported num tables limit
		if totalObjectCount > shardedLimit.maxSupportedNumTables.Int64 {
			// Generate failure reasoning
			failureReasoning := fmt.Sprintf("Cannot support %v sharded objects on a machine with %v cores "+
				"and %vGiB memory.", totalObjectCount, previousRecommendation.VCPUsPerInstance,
				previousRecommendation.VCPUsPerInstance*previousRecommendation.MemoryPerCore)

			// Update recommendation with failure reasoning
			recommendation[int(shardedLimit.numCores.Float64)] = IntermediateRecommendation{
				ColocatedTables:                 []SourceDBMetadata{},
				ShardedTables:                   []SourceDBMetadata{},
				VCPUsPerInstance:                previousRecommendation.VCPUsPerInstance,
				MemoryPerCore:                   previousRecommendation.MemoryPerCore,
				NumNodes:                        previousRecommendation.NumNodes,
				OptimalSelectConnectionsPerNode: previousRecommendation.OptimalSelectConnectionsPerNode,
				OptimalInsertConnectionsPerNode: previousRecommendation.OptimalInsertConnectionsPerNode,
				ColocatedSize:                   0,
				ShardedSize:                     0,
				EstimatedTimeInMinForImport:     previousRecommendation.EstimatedTimeInMinForImport,
				FailureReasoning:                failureReasoning,
			}
		}
	}
	// Return updated recommendation map
	return recommendation
}

/*
shardingBasedOnOperations performs sharding based on operations (reads and writes) per second, taking into account colocated limits.
It updates the existing recommendations with information about colocated and sharded tables based on operations.
Parameters:
  - sourceIndexMetadata: A slice of SourceDBMetadata structs representing source indexes.
  - colocatedThroughput: A slice of ExpDataThroughput structs representing colocated limits.
  - recommendation: A map where the key is the number of vCPUs per instance and the value is an IntermediateRecommendation struct.

Returns:
  - An updated map of recommendations where sharding information based on operations has been incorporated.
*/
func shardingBasedOnOperations(sourceIndexMetadata []SourceDBMetadata,
	colocatedThroughputSlice []ExpDataThroughput, recommendation map[int]IntermediateRecommendation) map[int]IntermediateRecommendation {

	for _, colocatedThroughput := range colocatedThroughputSlice {
		var colocatedObjects []SourceDBMetadata
		var cumulativeColocatedSizeSum float64 = 0
		var numColocated int = 0
		var cumulativeSelectOpsPerSec int64 = 0
		var cumulativeInsertOpsPerSec int64 = 0

		// Get previous recommendation for the current num of cores
		previousRecommendation := recommendation[int(colocatedThroughput.numCores.Float64)]

		for _, table := range previousRecommendation.ColocatedTables {
			// Check and fetch indexes for the current table
			_, indexesSizeSum, indexReads, indexWrites := checkAndFetchIndexes(table, sourceIndexMetadata)

			// Calculate new operations per second
			newSelectOpsPerSec := cumulativeSelectOpsPerSec + lo.Ternary(table.ReadsPerSec.Valid, table.ReadsPerSec.Int64, 0) + indexReads
			newInsertOpsPerSec := cumulativeInsertOpsPerSec + lo.Ternary(table.WritesPerSec.Valid, table.WritesPerSec.Int64, 0) + indexWrites

			// Calculate total object size
			objectTotalSize := lo.Ternary(table.Size.Valid, table.Size.Float64, 0) + indexesSizeSum

			// Calculate needed cores based on operations
			neededCores :=
				math.Ceil(float64(newSelectOpsPerSec)/colocatedThroughput.maxSupportedSelectsPerCore.Float64 +
					float64(newInsertOpsPerSec)/colocatedThroughput.maxSupportedInsertsPerCore.Float64)

			if neededCores <= colocatedThroughput.numCores.Float64 {
				// Update cumulative counts and add table to colocated objects
				colocatedObjects = append(colocatedObjects, table)
				cumulativeSelectOpsPerSec = newSelectOpsPerSec
				cumulativeInsertOpsPerSec = newInsertOpsPerSec
				cumulativeColocatedSizeSum += objectTotalSize
				numColocated++
			} else {
				// Break the loop if needed cores are more than current
				break
			}
		}
		shardedObjects := previousRecommendation.ShardedTables
		var cumulativeSizeSharded float64 = 0

		// Iterate over remaining colocated tables for sharding
		for _, remainingTable := range previousRecommendation.ColocatedTables[numColocated:] {
			shardedObjects = append(shardedObjects, remainingTable)
			_, indexesSizeSumSharded, _, _ := checkAndFetchIndexes(remainingTable, sourceIndexMetadata)
			cumulativeSizeSharded += lo.Ternary(remainingTable.Size.Valid, remainingTable.Size.Float64, 0) + indexesSizeSumSharded
		}

		// Update recommendation for the current colocated limit
		recommendation[int(colocatedThroughput.numCores.Float64)] = IntermediateRecommendation{
			ColocatedTables:                 colocatedObjects,
			ShardedTables:                   shardedObjects,
			VCPUsPerInstance:                previousRecommendation.VCPUsPerInstance,
			MemoryPerCore:                   previousRecommendation.MemoryPerCore,
			NumNodes:                        previousRecommendation.NumNodes,
			OptimalSelectConnectionsPerNode: colocatedThroughput.selectConnPerNode.Int64,
			OptimalInsertConnectionsPerNode: colocatedThroughput.insertConnPerNode.Int64,
			ColocatedSize:                   cumulativeColocatedSizeSum,
			ShardedSize:                     cumulativeSizeSharded,
			EstimatedTimeInMinForImport:     previousRecommendation.EstimatedTimeInMinForImport,
		}
	}
	// Return updated recommendation map
	return recommendation
}

/*
shardingBasedOnTableSizeAndCount performs sharding based on table size and count, taking into account colocated limits.
It updates the existing recommendations with information about colocated and sharded tables.
Parameters:
  - sourceTableMetadata: A slice of SourceDBMetadata structs representing source tables.
  - sourceIndexMetadata: A slice of SourceDBMetadata structs representing source indexes.
  - colocatedLimits: A slice of ExpDataColocatedLimit structs representing colocated limits.
  - recommendation: A map where the key is the number of vCPUs per instance and the value is an IntermediateRecommendation struct.

Returns:
  - An updated map of recommendations where sharding information has been incorporated.
*/
func shardingBasedOnTableSizeAndCount(sourceTableMetadata []SourceDBMetadata,
	sourceIndexMetadata []SourceDBMetadata, colocatedLimits []ExpDataColocatedLimit,
	recommendation map[int]IntermediateRecommendation) map[int]IntermediateRecommendation {

	for _, colocatedLimit := range colocatedLimits {
		var cumulativeColocatedSizeSum float64 = 0
		var colocatedObjects []SourceDBMetadata
		var numColocated int = 0
		var cumulativeObjectCount int64 = 0

		var shardedObjects []SourceDBMetadata
		var cumulativeSizeSharded float64 = 0

		for _, table := range sourceTableMetadata {
			// Check and fetch indexes for the current table
			indexesOfTable, indexesSizeSum, _, _ := checkAndFetchIndexes(table, sourceIndexMetadata)
			// DB-12363: make tables having more than COLOCATED_MAX_INDEXES_THRESHOLD indexes as sharded
			// (irrespective of size or ops requirements)
			if len(indexesOfTable) > COLOCATED_MAX_INDEXES_THRESHOLD {
				shardedObjects = append(shardedObjects, table)
				cumulativeSizeSharded += lo.Ternary(table.Size.Valid, table.Size.Float64, 0) + indexesSizeSum
				// skip to next table
				continue
			}
			// Calculate new object count and total size
			newObjectCount := cumulativeObjectCount + int64(len(indexesOfTable)) + 1
			objectTotalSize := lo.Ternary(table.Size.Valid, table.Size.Float64, 0) + indexesSizeSum
			newCumulativeSize := cumulativeColocatedSizeSum + objectTotalSize

			// Check if adding the current table exceeds max colocated size supported or max supported num tables
			if newCumulativeSize <= colocatedLimit.maxColocatedSizeSupported.Float64 &&
				(newObjectCount <= colocatedLimit.maxSupportedNumTables.Int64) {
				cumulativeObjectCount = newObjectCount
				cumulativeColocatedSizeSum = newCumulativeSize
				colocatedObjects = append(colocatedObjects, table)
				numColocated++
			} else {
				// Break the loop if colocated limits are exceeded
				break
			}
		}

		// Iterate over remaining tables for sharding
		for _, remainingTable := range sourceTableMetadata[(len(shardedObjects) + numColocated):] {
			shardedObjects = append(shardedObjects, remainingTable)
			_, indexesSizeSumSharded, _, _ := checkAndFetchIndexes(remainingTable, sourceIndexMetadata)
			cumulativeSizeSharded += lo.Ternary(remainingTable.Size.Valid, remainingTable.Size.Float64, 0) + indexesSizeSumSharded
		}
		// Update recommendation for the current colocated limit
		previousRecommendation := recommendation[int(colocatedLimit.numCores.Float64)]
		recommendation[int(colocatedLimit.numCores.Float64)] = IntermediateRecommendation{
			ColocatedTables:                 colocatedObjects,
			ShardedTables:                   shardedObjects,
			VCPUsPerInstance:                previousRecommendation.VCPUsPerInstance,
			MemoryPerCore:                   previousRecommendation.MemoryPerCore,
			NumNodes:                        previousRecommendation.NumNodes,
			OptimalSelectConnectionsPerNode: previousRecommendation.OptimalSelectConnectionsPerNode,
			OptimalInsertConnectionsPerNode: previousRecommendation.OptimalInsertConnectionsPerNode,
			ColocatedSize:                   cumulativeColocatedSizeSum,
			ShardedSize:                     cumulativeSizeSharded,
			EstimatedTimeInMinForImport:     previousRecommendation.EstimatedTimeInMinForImport,
		}
	}
	// Return updated recommendation map
	return recommendation
}

/*
loadColocatedLimit fetches colocated limits from the experiment data table.
It retrieves various limits such as maximum colocated database size, number of cores, memory per core, etc.
Returns:
  - A slice of ExpDataColocatedLimit structs containing the fetched colocated limits.
  - An error if there was any issue during the data retrieval process.
*/
func loadColocatedLimit(experimentDB *sql.DB) ([]ExpDataColocatedLimit, error) {
	var colocatedLimits []ExpDataColocatedLimit
	query := fmt.Sprintf(`
		SELECT max_colocated_db_size_gb, 
			   num_cores, 
			   mem_per_core, 
			   max_num_tables, 
			   min_num_tables
		FROM %v 
		ORDER BY num_cores DESC
	`, COLOCATED_LIMITS_TABLE)
	rows, err := experimentDB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("cannot fetch data from experiment data table with query [%s]: %w", query, err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Warnf("failed to close the result set for query [%v]", query)
		}
	}()

	for rows.Next() {
		var r1 ExpDataColocatedLimit
		if err := rows.Scan(&r1.maxColocatedSizeSupported, &r1.numCores, &r1.memPerCore, &r1.maxSupportedNumTables,
			&r1.minSupportedNumTables); err != nil {
			return nil, fmt.Errorf("cannot fetch data from experiment data table with query [%s]: %w", query, err)
		}
		colocatedLimits = append(colocatedLimits, r1)
	}
	// Return fetched colocated limits
	return colocatedLimits, nil
}

/*
loadShardedTableLimits fetches sharded table limits from the experiment data table.
It retrieves information such as the number of cores, memory per core, and maximum number of tables.
Returns:
  - A slice of ExpDataShardedLimit structs containing the fetched sharded table limits.
  - An error if there was any issue during the data retrieval process.
*/
func loadShardedTableLimits(experimentDB *sql.DB) ([]ExpDataShardedLimit, error) {
	// added num_cores >= VCPUPerInstance from colo recommendation as that is the starting point
	selectQuery := fmt.Sprintf(`
			SELECT num_cores, memory_per_core, num_tables 
			FROM %s 
			WHERE dimension LIKE '%%TableLimits-3nodeRF=3%%' 
			ORDER BY num_cores
		`, SHARDED_SIZING_TABLE)
	rows, err := experimentDB.Query(selectQuery)

	if err != nil {
		return nil, fmt.Errorf("error while fetching cores info with query [%s]: %w", selectQuery, err)
	}

	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Warnf("failed to close result set for query: [%s]", selectQuery)
		}
	}()
	var shardedLimits []ExpDataShardedLimit

	for rows.Next() {
		var r1 ExpDataShardedLimit
		if err := rows.Scan(&r1.numCores, &r1.memPerCore, &r1.maxSupportedNumTables); err != nil {
			return nil, fmt.Errorf("cannot fetch data from experiment data table with query [%s]: %w", selectQuery, err)
		}
		shardedLimits = append(shardedLimits, r1)
	}
	// Return fetched sharded table limits
	return shardedLimits, nil
}

/*
loadExpDataThroughput fetches sharded throughput information from the experiment data table.
It retrieves data such as inserts per core, selects per core, number of cores, memory per core, etc.
Parameters:

	experimentDB: A pointer to the experiment database.
	tableName: colocated or sharded table

Returns:
  - A slice of ExpDataThroughput structs containing the fetched throughput information.
  - An error if there was any issue during the data retrieval process.
*/
func loadExpDataThroughput(experimentDB *sql.DB, tableName string) ([]ExpDataThroughput, error) {
	selectQuery := fmt.Sprintf(`
			SELECT inserts_per_core,
				   selects_per_core, 
				   num_cores, 
				   memory_per_core,
				   select_conn_per_node, 
			   	   insert_conn_per_node 
			FROM %s 
			WHERE dimension = 'MaxThroughput' 
			ORDER BY num_cores DESC;
	`, tableName)
	rows, err := experimentDB.Query(selectQuery)
	if err != nil {
		return nil, fmt.Errorf("error while fetching throughput info with query [%s]: %w", selectQuery, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Warnf("failed to close result set for query: [%s]", selectQuery)
		}
	}()

	var shardedThroughput []ExpDataThroughput
	for rows.Next() {
		var throughput ExpDataThroughput
		if err := rows.Scan(&throughput.maxSupportedInsertsPerCore,
			&throughput.maxSupportedSelectsPerCore, &throughput.numCores,
			&throughput.memPerCore, &throughput.selectConnPerNode, &throughput.insertConnPerNode); err != nil {
			return nil, fmt.Errorf("cannot fetch data from experiment data table with query [%s]: %w", selectQuery, err)
		}
		shardedThroughput = append(shardedThroughput, throughput)
	}
	// Return fetched sharded throughput information
	return shardedThroughput, nil
}

/*
loadSourceMetadata connects to the assessment metadata of the source database and generates the slice of objects
for tables and indexes. It also returns the total size of source db in GB. Primary key size is not considered in the
calculation.
Returns:

	[]SourceDBMetadata: all table objects from source db
	[]SourceDBMetadata: all index objects from source db
	float64: total size of source db
*/
func loadSourceMetadata(filePath string) ([]SourceDBMetadata, []SourceDBMetadata, float64, error) {
	SourceMetaDB, err := utils.ConnectToSqliteDatabase(filePath)
	if err != nil {
		return nil, nil, 0.0, fmt.Errorf("cannot connect to source metadata database: %w", err)
	}
	return getSourceMetadata(SourceMetaDB)
}

/*
createSizingRecommendationStructure generates sizing recommendations based on colocated limits.
It creates recommendations per core and returns them in a map.
Parameters:
  - colocatedLimits: A slice of ExpDataColocatedLimit structs representing colocated limits.

Returns:
  - A map where the key is the number of vCPUs per instance and the value is an IntermediateRecommendation struct.
*/
func createSizingRecommendationStructure(colocatedLimits []ExpDataColocatedLimit) map[int]IntermediateRecommendation {
	recommendationPerCore := make(map[int]IntermediateRecommendation)
	for _, colocatedLimit := range colocatedLimits {
		var sizingRecommendation IntermediateRecommendation
		sizingRecommendation.MemoryPerCore = int(colocatedLimit.memPerCore.Float64)
		sizingRecommendation.VCPUsPerInstance = int(colocatedLimit.numCores.Float64)
		// Store recommendation in the map with vCPUs per instance as the key
		recommendationPerCore[sizingRecommendation.VCPUsPerInstance] = sizingRecommendation
	}
	// Return map containing recommendations per core
	return recommendationPerCore
}

/*
calculateTimeTakenForImport estimates the time taken for import of tables.
It queries experimental data to find import time estimates for similar object sizes and configurations. For every table
, it tries to find out how much time it would table for importing that table. The function adjusts the
import time on that table by multiplying it by factor based on the indexes. The import time is also converted to
minutes and returned.
Parameters:

	tables: A slice containing metadata for the database objects to be migrated.
	sourceIndexMetadata: A slice containing metadata for the indexes of the database objects to be migrated.
	loadTimes: Experiment data for impact of load times on tables
	indexImpacts: Data containing impact of indexes on load time.
	objectType: COLOCATED or SHARDED

Returns:

	float64: The estimated time taken for import in minutes.
	error: Error if any
*/
func calculateTimeTakenForImport(tables []SourceDBMetadata,
	sourceIndexMetadata []SourceDBMetadata, loadTimes []ExpDataLoadTime,
	indexImpactData []ExpDataLoadTimeIndexImpact, numColumnImpactData []ExpDataLoadTimeColumnsImpact,
	objectType string) (float64, error) {
	var importTime float64

	// we need to calculate the time taken for import for every table.
	// For every index, the time taken for import increases.
	// find the rows in experiment data about the approx row matching the size
	for _, table := range tables {
		// find the closest record from experiment data for the size of the table
		tableSize := lo.Ternary(table.Size.Valid, table.Size.Float64, 0)
		rowsInTable := lo.Ternary(table.RowCount.Valid, table.RowCount.Float64, 0)

		// get multiplication factor for every table based on the number of indexes
		loadTimeMultiplicationFactorWrtIndexes := getMultiplicationFactorForImportTimeBasedOnIndexes(table,
			sourceIndexMetadata, indexImpactData, objectType)
		// get multiplication factor for every table based on the number of columns in the table
		loadTimeMultiplicationFactorWrtNumColumns := getMultiplicationFactorForImportTimeBasedOnNumColumns(table,
			numColumnImpactData, objectType)

		tableImportTimeSec := findImportTimeFromExpDataLoadTime(loadTimes, tableSize, rowsInTable)
		// add maximum import time to total import time by converting it to minutes
		importTime += (loadTimeMultiplicationFactorWrtIndexes * loadTimeMultiplicationFactorWrtNumColumns * tableImportTimeSec) / 60
	}

	return math.Ceil(importTime), nil
}

/*
getExpDataLoadTime fetches load time information from the experiment data table.
Parameters:

	experimentDB: Connection to the experiment database
	vCPUPerInstance: Number of virtual CPUs per instance.
	memPerCore: Memory per core.

Returns:

	[]ExpDataLoadTime: A slice containing the fetched load time information.
	error: Error if any.
*/
func getExpDataLoadTime(experimentDB *sql.DB, vCPUPerInstance int, memPerCore int, tableType string) ([]ExpDataLoadTime, error) {
	selectQuery := fmt.Sprintf(`
		SELECT csv_size_gb, 
			   migration_time_secs,
			   row_count
		FROM %v 
		WHERE num_cores = ? 
			AND mem_per_core = ?
		ORDER BY csv_size_gb;
	`, tableType)
	rows, err := experimentDB.Query(selectQuery, vCPUPerInstance, memPerCore)

	if err != nil {
		return nil, fmt.Errorf("error while fetching load time info with query [%s]: %w", selectQuery, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Warnf("failed to close result set for query: [%s]", selectQuery)
		}
	}()

	var loadTimes []ExpDataLoadTime
	for rows.Next() {
		var loadTime ExpDataLoadTime
		if err = rows.Scan(&loadTime.csvSizeGB, &loadTime.migrationTimeSecs, &loadTime.rowCount); err != nil {
			return nil, fmt.Errorf("cannot fetch data from experiment data table with query [%s]: %w",
				selectQuery, err)
		}
		loadTimes = append(loadTimes, loadTime)
	}
	return loadTimes, nil
}

/*
getExpDataIndexImpactOnLoadTime fetches data for impact of indexes on load time from the experiment data table.
Parameters:

	experimentDB: Connection to the experiment database
	vCPUPerInstance: Number of virtual CPUs per instance.
	memPerCore: Memory per core.

Returns:

	[]ExpDataShardedLoadTimeIndexImpact: A slice containing the fetched load time information based on number of indexes.
	error: Error if any.
*/
func getExpDataIndexImpactOnLoadTime(experimentDB *sql.DB, vCPUPerInstance int, memPerCore int) ([]ExpDataLoadTimeIndexImpact, error) {
	selectQuery := fmt.Sprintf(`
		SELECT number_of_indexes, 
			   multiplication_factor_sharded,
			   multiplication_factor_colocated
		FROM %v 
		WHERE num_cores = ? 
			AND mem_per_core = ?
		ORDER BY number_of_indexes;
	`, LOAD_TIME_INDEX_IMPACT_TABLE)
	rows, err := experimentDB.Query(selectQuery, vCPUPerInstance, memPerCore)

	if err != nil {
		return nil, fmt.Errorf("error while fetching index impact info with query [%s]: %w", selectQuery, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Warnf("failed to close result set for query: [%s]", selectQuery)
		}
	}()

	var loadTimeIndexImpacts []ExpDataLoadTimeIndexImpact
	for rows.Next() {
		var loadTimeIndexImpact ExpDataLoadTimeIndexImpact
		if err = rows.Scan(&loadTimeIndexImpact.numIndexes, &loadTimeIndexImpact.multiplicationFactorSharded,
			&loadTimeIndexImpact.multiplicationFactorColocated); err != nil {
			return nil, fmt.Errorf("cannot fetch data from experiment data table with query [%s]: %w", selectQuery, err)
		}
		loadTimeIndexImpacts = append(loadTimeIndexImpacts, loadTimeIndexImpact)
	}
	return loadTimeIndexImpacts, nil
}

/*
getExpDataNumColumnsImpactOnLoadTime fetches data for impact of number of columns on load time from the experiment
data table.
Parameters:

	experimentDB: Connection to the experiment database
	vCPUPerInstance: Number of virtual CPUs per instance.
	memPerCore: Memory per core.

Returns:

	[]ExpDataLoadTimeColumnsImpact: A slice containing the fetched load time information based on number of indexes.
	error: Error if any.
*/
func getExpDataNumColumnsImpactOnLoadTime(experimentDB *sql.DB, vCPUPerInstance int,
	memPerCore int) ([]ExpDataLoadTimeColumnsImpact, error) {
	selectQuery := fmt.Sprintf(`
		SELECT number_of_columns, 
			   multiplication_factor_sharded,
			   multiplication_factor_colocated
		FROM %v 
		WHERE num_cores = ? 
			AND mem_per_core = ?
		ORDER BY number_of_columns;
	`, LOAD_TIME_COLUMNS_IMPACT_TABLE)
	rows, err := experimentDB.Query(selectQuery, vCPUPerInstance, memPerCore)

	if err != nil {
		return nil, fmt.Errorf("error while fetching columns impact info with query [%s]: %w", selectQuery, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Warnf("failed to close result set for query: [%s]", selectQuery)
		}
	}()

	var loadTimeColumnsImpacts []ExpDataLoadTimeColumnsImpact
	for rows.Next() {
		var loadTimeColumnsImpact ExpDataLoadTimeColumnsImpact
		if err = rows.Scan(&loadTimeColumnsImpact.numColumns, &loadTimeColumnsImpact.multiplicationFactorSharded,
			&loadTimeColumnsImpact.multiplicationFactorColocated); err != nil {
			return nil, fmt.Errorf("cannot fetch data from experiment data table with query [%s]: %w", selectQuery, err)
		}
		loadTimeColumnsImpacts = append(loadTimeColumnsImpacts, loadTimeColumnsImpact)
	}
	return loadTimeColumnsImpacts, nil
}

/*
findImportTimeFromExpDataLoadTime finds the closest record from the experiment data based on row count or size
of the table. Out of objects close in terms of size and rows, prefer object having number of rows.
Parameters:

	loadTimes: A slice containing the fetched load time information.
	objectSize: The size of the table in gigabytes.
	rowsInTable: number of rows in the table.

Returns:

	float64: max load time wrt size or count.
*/
func findImportTimeFromExpDataLoadTime(loadTimes []ExpDataLoadTime, objectSize float64,
	rowsInTable float64) float64 {
	closestInSize := loadTimes[0]
	closestInRows := loadTimes[0]

	minSizeDiff := math.Abs(objectSize - closestInSize.csvSizeGB.Float64)
	minRowsDiff := math.Abs(rowsInTable - closestInRows.rowCount.Float64)

	for _, num := range loadTimes {
		sizeDiff := math.Abs(objectSize - num.csvSizeGB.Float64)
		rowsDiff := math.Abs(rowsInTable - num.rowCount.Float64)
		if sizeDiff < minSizeDiff {
			minSizeDiff = sizeDiff
			closestInSize = num
		}
		if rowsDiff < minRowsDiff {
			minRowsDiff = rowsDiff
			closestInRows = num
		}
	}

	// calculate the time taken for import based on csv size and row count
	importTimeWrtSize := (closestInSize.migrationTimeSecs.Float64 * objectSize) / closestInSize.csvSizeGB.Float64
	importTimeWrtRowCount := (closestInRows.migrationTimeSecs.Float64 * rowsInTable) / closestInRows.rowCount.Float64

	// return the load time which is maximum of the two
	return math.Ceil(math.Max(importTimeWrtSize, importTimeWrtRowCount))
}

/*
getMultiplicationFactorForImportTimeBasedOnIndexes calculates the multiplication factor for import time based on number
of indexes on the table.

Parameters:

	table: Metadata for the database table for which the multiplication factor is to be calculated.
	sourceIndexMetadata: A slice containing metadata for the indexes in the database.
	objectType: COLOCATED or SHARDED

Returns:

	float64: The multiplication factor for import time based on the number of indexes on the table.
*/
func getMultiplicationFactorForImportTimeBasedOnIndexes(table SourceDBMetadata, sourceIndexMetadata []SourceDBMetadata,
	indexImpacts []ExpDataLoadTimeIndexImpact, objectType string) float64 {
	var numberOfIndexesOnTable float64 = 0
	var multiplicationFactor float64 = 1

	for _, index := range sourceIndexMetadata {
		if index.ParentTableName.Valid && index.ParentTableName.String == (table.SchemaName+"."+table.ObjectName) {
			numberOfIndexesOnTable += 1
		}
	}
	if numberOfIndexesOnTable == 0 {
		// if there are no indexes on table, return 1 immediately
		return 1
	} else {
		closest := indexImpacts[0]
		minDiff := math.Abs(numberOfIndexesOnTable - closest.numIndexes.Float64)

		for _, indexImpactData := range indexImpacts {
			diff := math.Abs(numberOfIndexesOnTable - indexImpactData.numIndexes.Float64)
			if diff < minDiff {
				minDiff = diff
				closest = indexImpactData
			}
		}
		// impact on load time for given table would be relative to the closest record's impact
		if objectType == COLOCATED {
			multiplicationFactor = (closest.multiplicationFactorColocated.Float64 / closest.numIndexes.Float64) * numberOfIndexesOnTable
		} else if objectType == SHARDED {
			multiplicationFactor = (closest.multiplicationFactorSharded.Float64 / closest.numIndexes.Float64) * numberOfIndexesOnTable
		}

		return multiplicationFactor
	}
}

/*
getMultiplicationFactorForImportTimeBasedOnNumColumns calculates the multiplication factor for import time based on
number of columns on the table.

Parameters:

	table: Metadata for the database table for which the multiplication factor is to be calculated.
	objectType: COLOCATED or SHARDED

Returns:

	float64: The multiplication factor for import time based on the number of columns in the table.
*/
func getMultiplicationFactorForImportTimeBasedOnNumColumns(table SourceDBMetadata,
	columnImpacts []ExpDataLoadTimeColumnsImpact, objectType string) float64 {
	numOfColumnsInTable := lo.Ternary(table.ColumnCount.Valid, table.ColumnCount.Int64, 1)

	// Initialize the selectedImpact as nil and minDiff with a high value
	var selectedImpact ExpDataLoadTimeColumnsImpact
	minDiff := int64(math.MaxInt64)
	found := false

	for _, columnsImpactData := range columnImpacts {
		if columnsImpactData.numColumns.Int64 >= numOfColumnsInTable {
			diff := columnsImpactData.numColumns.Int64 - numOfColumnsInTable
			if diff < minDiff {
				minDiff = diff
				selectedImpact = columnsImpactData
				found = true
			}
		}
	}

	// If no suitable impact is found, use the one with the maximum ColumnCount
	if !found {
		for _, columnsImpactData := range columnImpacts {
			if columnsImpactData.numColumns.Int64 > selectedImpact.numColumns.Int64 {
				selectedImpact = columnsImpactData
			}
		}
	}

	var multiplicationFactor float64
	// multiplication factor is different for colocated and sharded tables.
	// multiplication factor would be maximum of the two:
	//	max of (mf of selected entry from experiment data,  mf for table wrt selected entry)
	if objectType == COLOCATED {
		multiplicationFactor = math.Max(selectedImpact.multiplicationFactorColocated.Float64,
			(selectedImpact.multiplicationFactorColocated.Float64/float64(selectedImpact.numColumns.Int64))*float64(numOfColumnsInTable))
	} else if objectType == SHARDED {
		multiplicationFactor = math.Max(selectedImpact.multiplicationFactorSharded.Float64,
			(selectedImpact.multiplicationFactorSharded.Float64/float64(selectedImpact.numColumns.Int64))*float64(numOfColumnsInTable))
	}

	return multiplicationFactor
}

/*
getSourceMetadata retrieves metadata for source database tables and indexes along with the total size of the source
database.
Returns:

	[]SourceDBMetadata: Metadata for source database tables.
	[]SourceDBMetadata: Metadata for source database indexes.
	float64: The total size of the source database in gigabytes.
*/
func getSourceMetadata(sourceDB *sql.DB) ([]SourceDBMetadata, []SourceDBMetadata, float64, error) {
	// Construct the WHERE clause dynamically using LIKE
	var likeConditions []string
	for _, pattern := range SourceMetadataObjectTypesToUse {
		likeConditions = append(likeConditions, fmt.Sprintf("object_type LIKE '%s'", pattern))
	}
	// Join the LIKE conditions with OR
	whereClause := strings.Join(likeConditions, " OR ")

	query := fmt.Sprintf(`
		SELECT schema_name, 
			   object_name, 
			   row_count, 
			   reads_per_second, 
			   writes_per_second, 
			   is_index, 
			   parent_table_name, 
			   size_in_bytes,
			   column_count 
		FROM %v 
		WHERE %s
		ORDER BY IFNULL(size_in_bytes, 0) ASC
	`, GetTableIndexStatName(), whereClause)
	rows, err := sourceDB.Query(query)
	if err != nil {
		return nil, nil, 0.0, fmt.Errorf("failed to query source metadata with query [%s]: %w", query, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Warnf("failed to close result set for query: [%s]", query)
		}
	}()

	// Iterate over the rows
	var sourceTableMetadata []SourceDBMetadata
	var sourceIndexMetadata []SourceDBMetadata

	var totalSourceDBSize float64 = 0
	for rows.Next() {
		var metadata SourceDBMetadata
		if err := rows.Scan(&metadata.SchemaName, &metadata.ObjectName, &metadata.RowCount, &metadata.ReadsPerSec, &metadata.WritesPerSec,
			&metadata.IsIndex, &metadata.ParentTableName, &metadata.Size, &metadata.ColumnCount); err != nil {
			return nil, nil, 0.0, fmt.Errorf("failed to read from result set of query source metadata [%s]: %w", query, err)
		}
		// convert bytes to GB
		metadata.Size.Float64 = utils.BytesToGB(lo.Ternary(metadata.Size.Valid, metadata.Size.Float64, 0))
		if metadata.IsIndex {
			sourceIndexMetadata = append(sourceIndexMetadata, metadata)
		} else {
			sourceTableMetadata = append(sourceTableMetadata, metadata)
		}
		totalSourceDBSize += metadata.Size.Float64
	}
	if err := rows.Err(); err != nil {
		return nil, nil, 0.0, fmt.Errorf("failed to query source metadata with query [%s]: %w", query, err)
	}
	if err := sourceDB.Close(); err != nil {
		log.Warnf("failed to close connection to sourceDB metadata")
	}
	return sourceTableMetadata, sourceIndexMetadata, totalSourceDBSize, nil
}

/*
checkAndFetchIndexes checks for indexes associated with a specific database table and fetches their metadata.
It iterates through a slice of index metadata and selects indexes that belong to the specified table by comparing
their parent table names. The function returns a slice containing metadata for indexes associated with the table
and the total size of those indexes.
Parameters:

	table: Metadata for the database table for which indexes are to be checked.
	indexes: A slice containing metadata for all indexes in the database.

Returns:

	[]SourceDBMetadata: Metadata for indexes associated with the specified table.
	float64: The total size of indexes associated with the specified table.
	int64 : sum of read ops per second for all indexes of the table
	int64 : sum of write ops per second for all indexes of the table
*/
func checkAndFetchIndexes(table SourceDBMetadata, indexes []SourceDBMetadata) ([]SourceDBMetadata, float64, int64, int64) {
	indexesOfTable := make([]SourceDBMetadata, 0)
	var indexesSizeSum float64 = 0
	var cumulativeSelectOpsPerSecIdx int64 = 0
	var cumulativeInsertOpsPerSecIdx int64 = 0
	for _, index := range indexes {
		if index.ParentTableName.Valid && (index.ParentTableName.String == (table.SchemaName + "." + table.ObjectName)) {
			indexesOfTable = append(indexesOfTable, index)
			indexesSizeSum += lo.Ternary(index.Size.Valid, index.Size.Float64, 0)
			cumulativeSelectOpsPerSecIdx += lo.Ternary(index.ReadsPerSec.Valid, index.ReadsPerSec.Int64, 0)
			cumulativeInsertOpsPerSecIdx += lo.Ternary(index.ReadsPerSec.Valid, index.ReadsPerSec.Int64, 0)
		}
	}

	return indexesOfTable, indexesSizeSum, cumulativeSelectOpsPerSecIdx, cumulativeInsertOpsPerSecIdx
}

/*
getReasoning generates a string describing the reasoning behind a recommendation for an instance type.
It considers the characteristics of colocated and sharded objects.
Parameters:
  - recommendation: An IntermediateRecommendation struct containing information about the recommended instance type.
  - shardedObjects: A slice of SourceDBMetadata structs representing objects that need to be sharded.
  - colocatedObjects: A slice of SourceDBMetadata structs representing objects that can be colocated.

Returns:
  - A string describing the reasoning behind the recommendation.
*/
func getReasoning(recommendation IntermediateRecommendation, shardedObjects []SourceDBMetadata,
	cumulativeIndexCountSharded int, colocatedObjects []SourceDBMetadata, cumulativeIndexCountColocated int) string {
	// Calculate size and throughput of colocated objects
	colocatedObjectsSize, colocatedReads, colocatedWrites, sizeUnitColocated := getObjectsSize(colocatedObjects)
	reasoning := fmt.Sprintf("Recommended instance type with %v vCPU and %v GiB memory could fit ",
		recommendation.VCPUsPerInstance, recommendation.VCPUsPerInstance*recommendation.MemoryPerCore)

	// Add information about colocated objects if they exist
	if len(colocatedObjects) > 0 {
		reasoning += fmt.Sprintf("%v objects (%v tables/materialized views and %v explicit/implicit indexes) with %0.2f %v size "+
			"and throughput requirement of %v reads/sec and %v writes/sec as colocated.", len(colocatedObjects),
			len(colocatedObjects)-cumulativeIndexCountColocated, cumulativeIndexCountColocated, colocatedObjectsSize,
			sizeUnitColocated, colocatedReads, colocatedWrites)
	}
	// Add information about sharded objects if they exist
	if len(shardedObjects) > 0 {
		// Calculate size and throughput of sharded objects
		shardedObjectsSize, shardedReads, shardedWrites, sizeUnitSharded := getObjectsSize(shardedObjects)
		// Construct reasoning for sharded objects
		shardedReasoning := fmt.Sprintf("%v objects (%v tables/materialized views and %v explicit/implicit indexes) with %0.2f %v "+
			"size and throughput requirement of %v reads/sec and %v writes/sec ", len(shardedObjects),
			len(shardedObjects)-cumulativeIndexCountSharded, cumulativeIndexCountSharded, shardedObjectsSize,
			sizeUnitSharded, shardedReads, shardedWrites)
		// If colocated objects exist, add sharded objects information as rest of the objects need to be migrated as sharded
		if len(colocatedObjects) > 0 {
			reasoning += " Rest " + shardedReasoning + "need to be migrated as range partitioned tables."
		} else {
			reasoning += shardedReasoning + "as sharded."
		}

	}
	reasoning += " Non leaf partition tables/indexes and unsupported tables/indexes were not considered."
	return reasoning
}

/*
getObjectsSize calculates the total size and throughput requirements of a slice of SourceDBMetadata objects.
Parameters:
  - objects: A slice of SourceDBMetadata structs representing the database objects.

Returns:
  - The total size of the objects (in float64 representing GB).
  - The total number of select operations per second across all objects (in int64).
  - The total number of insert operations per second across all objects (in int64).
  - The size unit for the total size
*/
func getObjectsSize(objects []SourceDBMetadata) (float64, int64, int64, string) {
	var objectsSize float64 = 0
	var objectSelectOps int64 = 0
	var objectInsertOps int64 = 0
	var sizeUnit = SIZE_UNIT_GB

	for _, object := range objects {
		// Accumulate size and throughput values
		objectsSize += lo.Ternary(object.Size.Valid, object.Size.Float64, 0)
		objectSelectOps += lo.Ternary(object.ReadsPerSec.Valid, object.ReadsPerSec.Int64, 0)
		objectInsertOps += lo.Ternary(object.WritesPerSec.Valid, object.WritesPerSec.Int64, 0)
	}
	// if object size is less than 1 GB, convert it to MB
	if objectsSize < 1 {
		sizeUnit = SIZE_UNIT_MB
		objectsSize = objectsSize * 1024
	}

	// Return the accumulated size and throughput values
	return objectsSize, objectSelectOps, objectInsertOps, sizeUnit
}

/*
getListOfIndexesAlongWithObjects generates a list of indexes along with their corresponding tables from the given tableList.
Parameters:
  - tableList: A slice of SourceDBMetadata structs representing tables.
  - sourceIndexMetadata: A slice of SourceDBMetadata structs representing index metadata.

Returns:
  - A slice of SourceDBMetadata structs containing both indexes and tables.
  - total indexes of the tables
*/
func getListOfIndexesAlongWithObjects(tableList []SourceDBMetadata,
	sourceIndexMetadata []SourceDBMetadata) ([]SourceDBMetadata, int) {
	var indexesAndObject []SourceDBMetadata
	var cumulativeIndexCount int = 0

	for _, table := range tableList {
		// Check and fetch indexes for the current table
		indexes, _, _, _ := checkAndFetchIndexes(table, sourceIndexMetadata)
		indexesAndObject = append(indexesAndObject, indexes...)
		indexesAndObject = append(indexesAndObject, table)
		cumulativeIndexCount += len(indexes)
	}
	// Return the slice containing both indexes and tables
	return indexesAndObject, cumulativeIndexCount
}

func createConnectionToExperimentData() (*sql.DB, error) {
	filePath, err := getExperimentFile()
	if err != nil {
		return nil, fmt.Errorf("failed to get experiment file: %w", err)
	}
	DbConnection, err := utils.ConnectToSqliteDatabase(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to experiment data database: %w", err)
	}
	return DbConnection, nil
}

func getExperimentFile() (string, error) {
	fetchedFromRemote := false
	if PREFER_REMOTE_EXPERIMENT_DB && checkInternetAccess() {
		existsOnRemote, err := checkAndDownloadFileExistsOnRemoteRepo()
		if err != nil {
			return "", err
		}
		if existsOnRemote {
			fetchedFromRemote = true
		}
	}
	if !fetchedFromRemote {
		err := os.WriteFile(getExperimentDBPath(), experimentData20240, 0644)
		if err != nil {
			return "", fmt.Errorf("failed to write experiment data file: %w", err)
		}
	}

	return getExperimentDBPath(), nil
}

func checkAndDownloadFileExistsOnRemoteRepo() (bool, error) {
	// check if the file exists on remote github repository using the raw link
	remotePath := GITHUB_RAW_LINK + "/" + EXPERIMENT_DATA_FILENAME
	resp, err := http.Get(remotePath)
	if err != nil {
		return false, fmt.Errorf("failed to make GET request: %w", err)
	}
	defer func() {
		if closingErr := resp.Body.Close(); closingErr != nil {
			log.Warnf("failed to close the response body for GET api")
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return false, nil
	}

	downloadPath := getExperimentDBPath()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("failed to read response body: %w", err)
	}

	err = os.WriteFile(downloadPath, bodyBytes, 0644)
	if err != nil {
		return false, fmt.Errorf("failed to write file: %w", err)
	}

	return true, nil
}

/*
fetchObjectNames extracts object names from the given database objects and returns them as a slice of strings.
Parameters:
  - dbObjects: A slice of SourceDBMetadata structs representing database objects.

Returns:
  - A slice of strings containing the names of the database objects in the format
*/
func fetchObjectNames(dbObjects []SourceDBMetadata) []string {
	var objectNames []string
	for _, dbObject := range dbObjects {
		objectNames = append(objectNames, dbObject.SchemaName+"."+dbObject.ObjectName)
	}
	return objectNames
}
