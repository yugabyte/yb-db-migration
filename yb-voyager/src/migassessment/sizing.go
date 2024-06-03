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

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"

	log "github.com/sirupsen/logrus"
)

type SourceDBMetadata struct {
	SchemaName      string         `db:"schema_name"`
	ObjectName      string         `db:"object_name"`
	RowCount        sql.NullInt64  `db:"row_count,string"`
	ColumnCount     sql.NullInt64  `db:"column_count,string"`
	Reads           int64          `db:"reads,string"`
	Writes          int64          `db:"writes,string"`
	ReadsPerSec     int64          `db:"reads_per_second,string"`
	WritesPerSec    int64          `db:"writes_per_second,string"`
	IsIndex         bool           `db:"is_index,string"`
	ParentTableName sql.NullString `db:"parent_table_name"`
	Size            float64        `db:"size_in_bytes,string"`
}

type ExpDataShardedLimit struct {
	numCores              sql.NullFloat64 `db:"num_cores,string"`
	memPerCore            sql.NullFloat64 `db:"mem_per_core,string"`
	maxSupportedNumTables sql.NullInt64   `db:"max_num_tables,string"`
}

type ExpDataColocatedLimit struct {
	maxColocatedSizeSupported  sql.NullFloat64 `db:"max_colocated_db_size_gb,string"`
	numCores                   sql.NullFloat64 `db:"num_cores,string"`
	memPerCore                 sql.NullFloat64 `db:"mem_per_core,string"`
	maxSupportedNumTables      sql.NullInt64   `db:"max_num_tables,string"`
	minSupportedNumTables      sql.NullFloat64 `db:"min_num_tables,string"`
	maxSupportedSelectsPerCore sql.NullFloat64 `db:"max_selects_per_core,string"`
	maxSupportedInsertsPerCore sql.NullFloat64 `db:"max_inserts_per_core,string"`
}

type ExpDataShardedThroughput struct {
	numCores                   sql.NullFloat64 `db:"num_cores,string"`
	memPerCore                 sql.NullFloat64 `db:"mem_per_core,string"`
	maxSupportedSelectsPerCore sql.NullFloat64 `db:"max_selects_per_core,string"`
	maxSupportedInsertsPerCore sql.NullFloat64 `db:"max_inserts_per_core,string"`
	selectConnPerNode          sql.NullInt64   `db:"select_conn_per_node,string"`
	insertConnPerNode          sql.NullInt64   `db:"insert_conn_per_node,string"`
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
	ParallelVoyagerJobs             float64
	FailureReasoning                string
}

const (
	COLOCATED_LIMITS_TABLE    = "colocated_limits"
	SHARDED_SIZING_TABLE      = "sharded_sizing"
	COLOCATED_LOAD_TIME_TABLE = "colocated_load_time"
	SHARDED_LOAD_TIME_TABLE   = "sharded_load_time"
	// GITHUB_RAW_LINK use raw github link to fetch the file from repository using the api:
	// https://raw.githubusercontent.com/{username-or-organization}/{repository}/{branch}/{path-to-file}
	GITHUB_RAW_LINK                       = "https://raw.githubusercontent.com/yugabyte/yb-voyager/main/yb-voyager/src/migassessment/resources"
	EXPERIMENT_DATA_FILENAME              = "yb_2024_0_source.db"
	DBS_DIR                               = "dbs"
	SIZE_UNIT_GB                          = "GB"
	SIZE_UNIT_MB                          = "MB"
	LOW_PHASE_SHARD_COUNT                 = 8
	LOW_PHASE_SIZE_THRESHOLD_GB   float64 = 0.512
	HIGH_PHASE_SHARD_COUNT                = 24
	HIGH_PHASE_SIZE_THRESHOLD_GB  float64 = 10
	FINAL_PHASE_SIZE_THRESHOLD_GB float64 = 100
	MAX_TABLETS_PER_TABLE                 = 256
)

var ExperimentDB *sql.DB

func getExperimentDBPath(assessmentDir string) string {
	if AssessmentDir == "" {
		return filepath.Join(assessmentDir, DBS_DIR, EXPERIMENT_DATA_FILENAME)
	} else {
		return filepath.Join(AssessmentDir, DBS_DIR, EXPERIMENT_DATA_FILENAME)
	}
}

//go:embed resources/yb_2024_0_source.db
var experimentData20240 []byte

func SizingAssessment(assessmentDir string) error {

	log.Infof("loading metadata files for sharding assessment")
	sourceTableMetadata, sourceIndexMetadata, _, err := loadSourceMetadata(assessmentDir)
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("failed to load source metadata: %v", err)
		return fmt.Errorf("failed to load source metadata: %w", err)
	}

	err = createConnectionToExperimentData(assessmentDir)
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("failed to connect to experiment data: %v", err)
		return fmt.Errorf("failed to connect to experiment data: %w", err)
	}

	colocatedLimits, err := loadColocatedLimit()
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("error fetching the colocated limits: %v", err)
		return fmt.Errorf("error fetching the colocated limits: %w", err)
	}

	shardedLimits, err := loadShardedTableLimits()
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("error fetching the sharded limits: %v", err)
		return fmt.Errorf("error fetching the colocated limits: %w", err)
	}

	shardedThroughput, err := loadShardedThroughput()
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("error fetching the sharded throughput: %v", err)
		return fmt.Errorf("error fetching the sharded throughput: %w", err)
	}

	sizingRecommendationPerCore := createSizingRecommendationStructure(colocatedLimits)

	sizingRecommendationPerCore = shardingBasedOnTableSizeAndCount(sourceTableMetadata, sourceIndexMetadata,
		colocatedLimits, sizingRecommendationPerCore)

	sizingRecommendationPerCore = shardingBasedOnOperations(sourceIndexMetadata, colocatedLimits, sizingRecommendationPerCore)

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

	// calculate time taken for colocated import
	importTimeForColocatedObjects, parallelVoyagerJobsColocated, err :=
		calculateTimeTakenAndParallelJobsForImport(COLOCATED_LOAD_TIME_TABLE, colocatedObjects,
			finalSizingRecommendation.VCPUsPerInstance, finalSizingRecommendation.MemoryPerCore)
	if err != nil {
		SizingReport.FailureReasoning = fmt.Sprintf("calculate time taken for colocated data import: %v", err)
		return fmt.Errorf("calculate time taken for colocated data import: %w", err)
	}

	// calculate time taken for sharded import
	importTimeForShardedObjects, parallelVoyagerJobsSharded, err :=
		calculateTimeTakenAndParallelJobsForImport(SHARDED_LOAD_TIME_TABLE, shardedObjects,
			finalSizingRecommendation.VCPUsPerInstance, finalSizingRecommendation.MemoryPerCore)
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
		ParallelVoyagerJobs:             math.Min(float64(parallelVoyagerJobsColocated), float64(parallelVoyagerJobsSharded)),
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
	}

	// Return the best recommendation
	return finalRecommendation
}

/*
findNumNodesNeededBasedOnThroughputRequirement calculates the number of nodes needed based on sharded throughput limits and updates the recommendation accordingly.
Parameters:
  - sourceIndexMetadata: A slice of SourceDBMetadata structs representing source indexes.
  - shardedLimits: A slice of ExpDataShardedThroughput structs representing sharded throughput limits.
  - recommendation: A map where the key is the number of vCPUs per instance and the value is an IntermediateRecommendation struct.

Returns:
  - An updated map of recommendations with the number of nodes needed.
*/
func findNumNodesNeededBasedOnThroughputRequirement(sourceIndexMetadata []SourceDBMetadata, shardedLimits []ExpDataShardedThroughput,
	recommendation map[int]IntermediateRecommendation) map[int]IntermediateRecommendation {
	// Iterate over sharded throughput limits
	for _, shardedLimit := range shardedLimits {
		// Get previous recommendation for the current num of cores
		previousRecommendation := recommendation[int(shardedLimit.numCores.Float64)]
		var cumulativeSelectOpsPerSec int64 = 0
		var cumulativeInsertOpsPerSec int64 = 0

		// Calculate cumulative operations per second for sharded tables
		for _, table := range previousRecommendation.ShardedTables {
			// Check and fetch indexes for the current table
			_, _, indexReads, indexWrites := checkAndFetchIndexes(table, sourceIndexMetadata)
			cumulativeSelectOpsPerSec += table.ReadsPerSec + indexReads
			cumulativeInsertOpsPerSec += table.WritesPerSec + indexWrites
		}

		// Calculate needed cores based on cumulative operations per second
		neededCores :=
			math.Ceil(float64(cumulativeSelectOpsPerSec)/shardedLimit.maxSupportedSelectsPerCore.Float64 +
				float64(cumulativeInsertOpsPerSec)/shardedLimit.maxSupportedInsertsPerCore.Float64)

		nodesNeeded := math.Ceil(neededCores / shardedLimit.numCores.Float64)
		// Assumption: If there are any colocated objects - one node will be utilized as colocated tablet leader.
		// Add it explicitly.
		if len(previousRecommendation.ColocatedTables) > 0 {
			nodesNeeded += 1
		}

		// Assumption: minimum required replication is 3, so minimum nodes recommended would be 3.
		// Choose max of nodes needed and 3
		nodesNeeded = math.Max(nodesNeeded, 3)

		// Update recommendation with the number of nodes needed
		recommendation[int(shardedLimit.numCores.Float64)] = IntermediateRecommendation{
			ColocatedTables:                 previousRecommendation.ColocatedTables,
			ShardedTables:                   previousRecommendation.ShardedTables,
			VCPUsPerInstance:                previousRecommendation.VCPUsPerInstance,
			MemoryPerCore:                   previousRecommendation.MemoryPerCore,
			NumNodes:                        nodesNeeded,
			OptimalSelectConnectionsPerNode: shardedLimit.selectConnPerNode.Int64,
			OptimalInsertConnectionsPerNode: shardedLimit.insertConnPerNode.Int64,
			ParallelVoyagerJobs:             previousRecommendation.ParallelVoyagerJobs,
			ColocatedSize:                   previousRecommendation.ColocatedSize,
			ShardedSize:                     previousRecommendation.ShardedSize,
			EstimatedTimeInMinForImport:     previousRecommendation.EstimatedTimeInMinForImport,
			FailureReasoning:                previousRecommendation.FailureReasoning,
		}
	}
	// Return updated recommendation map
	return recommendation
}

func findNumNodesNeededBasedOnTabletsRequired(sourceIndexMetadata []SourceDBMetadata,
	shardedLimits []ExpDataShardedLimit,
	recommendation map[int]IntermediateRecommendation) map[int]IntermediateRecommendation {
	// Iterate over each intermediate recommendation where failureReasoning is empty
	totalTabletsRequired := 0
	for index, rec := range recommendation {
		if len(rec.ShardedTables) != 0 && rec.FailureReasoning == "" {
			fmt.Printf("\nProcessing Recommendation for %d vCPUs per instance\n", rec.VCPUsPerInstance)
			// Iterate over each table and its indexes to find out how many tablets are needed
			for _, table := range rec.ShardedTables {
				// check and fetch indexes size data for table
				_, indexesSizeSum, _, _ := checkAndFetchIndexes(table, sourceIndexMetadata)
				threshold, tabletsRequired := getThresholdAndTablets(table.Size + indexesSizeSum)
				totalTabletsRequired += tabletsRequired
				fmt.Printf("\tTable %s with size %f GB requires %d tablets from threshold %0.4f\n", table.ObjectName, table.Size+indexesSizeSum, tabletsRequired, threshold)
			}
			// assuming table limits is also a tablet limit
			// get shardedLimit of current recommendation
			for _, record := range shardedLimits {
				if record.numCores.Valid && int(record.numCores.Float64) == rec.VCPUsPerInstance {
					nodesRequired := math.Ceil(float64(totalTabletsRequired) / float64(record.maxSupportedNumTables.Int64))
					// update recommendation to use the maximum of the existing recommended nodes and nodes calculated based on tablets
					if nodesRequired > rec.NumNodes {
						fmt.Printf("Tablets required %d, existing recommendation: %f new recommendation: %f\n", totalTabletsRequired, rec.NumNodes, nodesRequired)
					} else {
						fmt.Printf("Tablets required %d, existing recommendation: %f. Staying with current recommendation\n", totalTabletsRequired, rec.NumNodes)
					}
					rec.NumNodes = math.Max(rec.NumNodes, nodesRequired)
					recommendation[index] = rec
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
- sizeGB: float64 - The size of the table in gigabytes.

Returns:
- float64: The size threshold in gigabytes for the table based on the phase.
- int: The number of tablets needed for the table.

Description:
This function calculates which size threshold applies to a table based on its size and determines the number of tablets required.
- For sizes up to the low phase limit (8 shards of 512 MB each, up to 4 GB), the low phase threshold is used.
- For sizes up to the high phase limit (24 shards of 10 GB each, up to 240 GB), the high phase threshold is used.
- For larger sizes, the final phase threshold (100 GB) is used, with a maximum of 256 tablets per table.
*/
func getThresholdAndTablets(sizeGB float64) (float64, int) {
	var threshold float64
	var tablets float64

	if sizeGB <= LOW_PHASE_SIZE_THRESHOLD_GB*float64(LOW_PHASE_SHARD_COUNT) {
		threshold = LOW_PHASE_SIZE_THRESHOLD_GB
		tablets = math.Ceil(sizeGB / LOW_PHASE_SIZE_THRESHOLD_GB)
	} else if sizeGB <= HIGH_PHASE_SIZE_THRESHOLD_GB*float64(HIGH_PHASE_SHARD_COUNT) {
		threshold = HIGH_PHASE_SIZE_THRESHOLD_GB
		tablets = math.Ceil(sizeGB / HIGH_PHASE_SIZE_THRESHOLD_GB)
	} else {
		threshold = FINAL_PHASE_SIZE_THRESHOLD_GB
		tablets = math.Ceil(sizeGB / FINAL_PHASE_SIZE_THRESHOLD_GB)
		if tablets > MAX_TABLETS_PER_TABLE {
			tablets = MAX_TABLETS_PER_TABLE
		}
	}
	// considering each table will require at least 1 tablet
	tablets = math.Max(1, tablets)
	return threshold, int(tablets)
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
				ParallelVoyagerJobs:             previousRecommendation.ParallelVoyagerJobs,
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
  - colocatedLimits: A slice of ExpDataColocatedLimit structs representing colocated limits.
  - recommendation: A map where the key is the number of vCPUs per instance and the value is an IntermediateRecommendation struct.

Returns:
  - An updated map of recommendations where sharding information based on operations has been incorporated.
*/
func shardingBasedOnOperations(sourceIndexMetadata []SourceDBMetadata,
	colocatedLimits []ExpDataColocatedLimit, recommendation map[int]IntermediateRecommendation) map[int]IntermediateRecommendation {

	for _, colocatedLimit := range colocatedLimits {
		var colocatedObjects []SourceDBMetadata
		var cumulativeColocatedSizeSum float64 = 0
		var numColocated int = 0
		var cumulativeSelectOpsPerSec int64 = 0
		var cumulativeInsertOpsPerSec int64 = 0

		// Get previous recommendation for the current num of cores
		previousRecommendation := recommendation[int(colocatedLimit.numCores.Float64)]

		for _, table := range previousRecommendation.ColocatedTables {
			// Check and fetch indexes for the current table
			_, indexesSizeSum, indexReads, indexWrites := checkAndFetchIndexes(table, sourceIndexMetadata)

			// Calculate new operations per second
			newSelectOpsPerSec := cumulativeSelectOpsPerSec + table.ReadsPerSec + indexReads
			newInsertOpsPerSec := cumulativeInsertOpsPerSec + table.WritesPerSec + indexWrites

			// Calculate total object size
			objectTotalSize := table.Size + indexesSizeSum

			// Calculate needed cores based on operations
			neededCores :=
				math.Ceil(float64(newSelectOpsPerSec)/colocatedLimit.maxSupportedSelectsPerCore.Float64 +
					float64(newInsertOpsPerSec)/colocatedLimit.maxSupportedInsertsPerCore.Float64)

			if neededCores <= colocatedLimit.numCores.Float64 {
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
			cumulativeSizeSharded += remainingTable.Size + indexesSizeSumSharded
		}

		// Update recommendation for the current colocated limit
		recommendation[int(colocatedLimit.numCores.Float64)] = IntermediateRecommendation{
			ColocatedTables:                 colocatedObjects,
			ShardedTables:                   shardedObjects,
			VCPUsPerInstance:                previousRecommendation.VCPUsPerInstance,
			MemoryPerCore:                   previousRecommendation.MemoryPerCore,
			NumNodes:                        previousRecommendation.NumNodes,
			OptimalSelectConnectionsPerNode: previousRecommendation.OptimalSelectConnectionsPerNode,
			OptimalInsertConnectionsPerNode: previousRecommendation.OptimalInsertConnectionsPerNode,
			ParallelVoyagerJobs:             previousRecommendation.ParallelVoyagerJobs,
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

		for _, table := range sourceTableMetadata {
			// Check and fetch indexes for the current table
			indexesOfTable, indexesSizeSum, _, _ := checkAndFetchIndexes(table, sourceIndexMetadata)
			// Calculate new object count and total size
			newObjectCount := cumulativeObjectCount + int64(len(indexesOfTable)) + 1
			objectTotalSize := table.Size + indexesSizeSum
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
		var shardedObjects []SourceDBMetadata
		var cumulativeSizeSharded float64 = 0

		// Iterate over remaining tables for sharding
		for _, remainingTable := range sourceTableMetadata[numColocated:] {
			shardedObjects = append(shardedObjects, remainingTable)
			_, indexesSizeSumSharded, _, _ := checkAndFetchIndexes(remainingTable, sourceIndexMetadata)
			cumulativeSizeSharded += remainingTable.Size + indexesSizeSumSharded
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
			ParallelVoyagerJobs:             previousRecommendation.ParallelVoyagerJobs,
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
func loadColocatedLimit() ([]ExpDataColocatedLimit, error) {
	var colocatedLimits []ExpDataColocatedLimit
	query := fmt.Sprintf(`
		SELECT max_colocated_db_size_gb, 
			   num_cores, 
			   mem_per_core, 
			   max_num_tables, 
			   min_num_tables, 
			   max_selects_per_core, 
			   max_inserts_per_core 
		FROM %v 
		ORDER BY num_cores DESC
	`, COLOCATED_LIMITS_TABLE)
	rows, err := ExperimentDB.Query(query)
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
			&r1.minSupportedNumTables, &r1.maxSupportedSelectsPerCore, &r1.maxSupportedInsertsPerCore); err != nil {
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
func loadShardedTableLimits() ([]ExpDataShardedLimit, error) {
	// added num_cores >= VCPUPerInstance from colo recommendation as that is the starting point
	selectQuery := fmt.Sprintf(`
			SELECT num_cores, memory_per_core, num_tables 
			FROM %s 
			WHERE dimension LIKE '%%TableLimits-3nodeRF=3%%' 
			ORDER BY num_cores
		`, SHARDED_SIZING_TABLE)
	rows, err := ExperimentDB.Query(selectQuery)

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
loadShardedThroughput fetches sharded throughput information from the experiment data table.
It retrieves data such as inserts per core, selects per core, number of cores, memory per core, etc.
Returns:
  - A slice of ExpDataShardedThroughput structs containing the fetched sharded throughput information.
  - An error if there was any issue during the data retrieval process.
*/
func loadShardedThroughput() ([]ExpDataShardedThroughput, error) {
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
	`, SHARDED_SIZING_TABLE)
	rows, err := ExperimentDB.Query(selectQuery)
	if err != nil {
		return nil, fmt.Errorf("error while fetching throughput info with query [%s]: %w", selectQuery, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Warnf("failed to close result set for query: [%s]", selectQuery)
		}
	}()

	var shardedThroughput []ExpDataShardedThroughput
	for rows.Next() {
		var throughput ExpDataShardedThroughput
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
func loadSourceMetadata(assessmentDir string) ([]SourceDBMetadata, []SourceDBMetadata, float64, error) {
	filePath := GetSourceMetadataDBFilePath()
	if AssessmentDir == "" {
		filePath = filepath.Join(assessmentDir, filePath)
	} else {
		filePath = filepath.Join(AssessmentDir, filePath)
	}

	fmt.Println("source db file to connect to: ", filePath)
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
calculateTimeTakenAndParallelJobsForImport estimates the time taken for import of database objects based on their type, size,
and the specified CPU and memory configurations. It calculates the total size of the database objects to be migrated,
then queries experimental data to find import time estimates for similar object sizes and configurations. The
function adjusts the import time based on the ratio of the total size of the objects to be migrated to the maximum
size found in the experimental data. The import time is then converted from seconds to minutes and returned.
Parameters:

	tableName: A string indicating the type of database objects to be imported (e.g., "colocated" or "sharded").
	dbObjects: A slice containing metadata for the database objects to be migrated.
	vCPUPerInstance: The number of virtual CPUs per instance used for import.
	memPerCore: The memory allocated per CPU core used for import.

Returns:

	float64: The estimated time taken for import in minutes.
	int64: Total parallel jobs used for import.
*/
func calculateTimeTakenAndParallelJobsForImport(tableName string, dbObjects []SourceDBMetadata,
	vCPUPerInstance int, memPerCore int) (float64, int64, error) {
	// the total size of objects
	var size float64 = 0
	var timeTakenOfFetchedRow float64
	var maxSizeOfFetchedRow float64
	var parallelJobs int64
	for _, dbObject := range dbObjects {
		size += dbObject.Size
	}
	// find the rows in experiment data about the approx row matching the size
	selectQuery := fmt.Sprintf(`
		SELECT csv_size_gb, 
			   migration_time_secs, 
			   parallel_threads 
		FROM %v 
		WHERE num_cores = ? 
			AND mem_per_core = ? 
			AND csv_size_gb >= ? 
		UNION ALL 
		SELECT csv_size_gb, 
			   migration_time_secs, 
			   parallel_threads 
		FROM %v 
		WHERE csv_size_gb = (
			SELECT MAX(csv_size_gb) 
			FROM %v
		) 
		AND num_cores = ?
		LIMIT 1;
	`, tableName, tableName, tableName)
	row := ExperimentDB.QueryRow(selectQuery, vCPUPerInstance, memPerCore, size, vCPUPerInstance)

	if err := row.Scan(&maxSizeOfFetchedRow, &timeTakenOfFetchedRow, &parallelJobs); err != nil {
		if err == sql.ErrNoRows {
			log.Errorf("No rows were returned by the query to experiment table: %v", tableName)
		} else {
			return 0.0, 0, fmt.Errorf("error while fetching import time info with query [%s]: %w", selectQuery, err)
		}
	}

	importTime := ((timeTakenOfFetchedRow * size) / maxSizeOfFetchedRow) / 60
	return math.Ceil(importTime), parallelJobs, nil
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
	query := fmt.Sprintf(`
		SELECT schema_name, 
			   object_name, 
			   row_count, 
			   reads_per_second, 
			   writes_per_second, 
			   is_index, 
			   parent_table_name, 
			   size_in_bytes 
		FROM %v 
		ORDER BY size_in_bytes ASC
	`, GetTableIndexStatName())
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
			&metadata.IsIndex, &metadata.ParentTableName, &metadata.Size); err != nil {
			return nil, nil, 0.0, fmt.Errorf("failed to read from result set of query source metadata [%s]: %w", query, err)
		}
		// convert bytes to GB
		metadata.Size = utils.BytesToGB(metadata.Size)
		if metadata.IsIndex {
			sourceIndexMetadata = append(sourceIndexMetadata, metadata)
		} else {
			sourceTableMetadata = append(sourceTableMetadata, metadata)
		}
		totalSourceDBSize += metadata.Size
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
			indexesSizeSum += index.Size
			cumulativeSelectOpsPerSecIdx += index.ReadsPerSec
			cumulativeInsertOpsPerSecIdx += index.WritesPerSec
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
		reasoning += fmt.Sprintf("%v objects(%v tables and %v explicit/implicit indexes) with %0.2f %v size "+
			"and throughput requirement of %v reads/sec and %v writes/sec as colocated.", len(colocatedObjects),
			len(colocatedObjects)-cumulativeIndexCountColocated, cumulativeIndexCountColocated, colocatedObjectsSize,
			sizeUnitColocated, colocatedReads, colocatedWrites)
	}
	// Add information about sharded objects if they exist
	if len(shardedObjects) > 0 {
		// Calculate size and throughput of sharded objects
		shardedObjectsSize, shardedReads, shardedWrites, sizeUnitSharded := getObjectsSize(shardedObjects)
		// Construct reasoning for sharded objects
		shardedReasoning := fmt.Sprintf("%v objects(%v tables and %v explicit/implicit indexes) with %0.2f %v "+
			"size and throughput requirement of %v reads/sec and %v writes/sec ", len(shardedObjects),
			len(shardedObjects)-cumulativeIndexCountSharded, cumulativeIndexCountSharded, shardedObjectsSize,
			sizeUnitSharded, shardedReads, shardedWrites)
		// If colocated objects exist, add sharded objects information as rest of the objects need to be migrated as sharded
		if len(colocatedObjects) > 0 {
			reasoning += " Rest " + shardedReasoning + " need to be migrated as range partitioned tables"
		} else {
			reasoning += shardedReasoning + "as sharded."
		}

	}
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
		objectsSize += object.Size
		objectSelectOps += object.ReadsPerSec
		objectInsertOps += object.WritesPerSec
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

func createConnectionToExperimentData(assessmentDir string) error {
	filePath, err := getExperimentFile(assessmentDir)
	if err != nil {
		return fmt.Errorf("failed to get experiment file: %w", err)
	}
	DbConnection, err := utils.ConnectToSqliteDatabase(filePath)
	if err != nil {
		return fmt.Errorf("failed to connect to experiment data database: %w", err)
	}
	ExperimentDB = DbConnection
	return nil
}

func getExperimentFile(assessmentDir string) (string, error) {
	fetchedFromRemote := false
	if checkInternetAccess() {
		existsOnRemote, err := checkAndDownloadFileExistsOnRemoteRepo(assessmentDir)
		if err != nil {
			return "", err
		}
		if existsOnRemote {
			fetchedFromRemote = true
		}
	}
	if !fetchedFromRemote {
		err := os.WriteFile(getExperimentDBPath(assessmentDir), experimentData20240, 0644)
		if err != nil {
			return "", fmt.Errorf("failed to write experiment data file: %w", err)
		}
	}

	return getExperimentDBPath(assessmentDir), nil
}

func checkAndDownloadFileExistsOnRemoteRepo(assessmentDir string) (bool, error) {
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

	downloadPath := getExperimentDBPath(assessmentDir)
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
