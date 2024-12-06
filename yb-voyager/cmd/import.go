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
package cmd

import (
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

var sourceDBType string
var enableOrafce utils.BoolStr
var importType string

// tconf struct will be populated by CLI arguments parsing
var tconf tgtdb.TargetConf

var tdb tgtdb.TargetDB

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import schema and data from compatible source database to target database. ",
	Long:  `Import has various sub-commands i.e. import schema, import data to import into YugabyteDB from various compatible source databases(Oracle, MySQL, PostgreSQL). Also import data(snapshot + changes from target) into source-replica/source in case of live migration with fall-back/fall-forward worflows.`,
}

func init() {
	rootCmd.AddCommand(importCmd)
}

// If any changes are made to this function, verify if the change is also needed for importDataFileCommand.go
func validateImportFlags(cmd *cobra.Command, importerRole string) error {
	checkOrSetDefaultTargetSSLMode()
	validateTargetPortRange()

	validateConflictsBetweenTableListFlags(tconf.TableList, tconf.ExcludeTableList)

	validateTableListFlag(tconf.TableList, "table-list")
	validateTableListFlag(tconf.ExcludeTableList, "exclude-table-list")

	var err error
	if tconf.TableList == "" {
		tconf.TableList, err = validateAndExtractTableNamesFromFile(tableListFilePath, "table-list-file-path")
		if err != nil {
			return err
		}
	}

	if tconf.ExcludeTableList == "" {
		tconf.ExcludeTableList, err = validateAndExtractTableNamesFromFile(excludeTableListFilePath, "exclude-table-list-file-path")
		if err != nil {
			return err
		}
	}

	if tconf.ImportObjects != "" && tconf.ExcludeImportObjects != "" {
		return fmt.Errorf("only one of --object-type-list and --exclude-object-type-list are allowed")
	}
	validateImportObjectsFlag(tconf.ImportObjects, "object-type-list")
	validateImportObjectsFlag(tconf.ExcludeImportObjects, "exclude-object-type-list")
	validateTargetSchemaFlag()
	// For beta2.0 release (and onwards until further notice)
	if tconf.DisableTransactionalWrites {
		fmt.Println("WARNING: The --disable-transactional-writes feature is in the experimental phase, not for production use case.")
	}
	validateBatchSizeFlag(batchSizeInNumRows)
	switch importerRole {
	case TARGET_DB_IMPORTER_ROLE:
		getTargetPassword(cmd)
	case SOURCE_REPLICA_DB_IMPORTER_ROLE:
		getSourceReplicaDBPassword(cmd)
	case SOURCE_DB_IMPORTER_ROLE:
		getSourceDBPassword(cmd)
	}
	validateParallelismFlags()
	validateTruncateTablesFlag()
	return nil
}

func registerCommonImportFlags(cmd *cobra.Command) {
	BoolVar(cmd.Flags(), &tconf.ContinueOnError, "continue-on-error", false,
		"Ignore errors and continue with the import")

	BoolVar(cmd.Flags(), &tconf.RunGuardrailsChecks, "run-guardrails-checks", true, "Run guardrails checks during import")
}

func registerTargetDBConnFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&tconf.Host, "target-db-host", "127.0.0.1",
		"host on which the YugabyteDB server is running")

	cmd.Flags().IntVar(&tconf.Port, "target-db-port", 0,
		"port on which the YugabyteDB YSQL API is running (Default: 5433)")

	cmd.Flags().StringVar(&tconf.User, "target-db-user", "",
		"username with which to connect to the target YugabyteDB server")
	cmd.MarkFlagRequired("target-db-user")

	cmd.Flags().StringVar(&tconf.Password, "target-db-password", "",
		"password with which to connect to the target YugabyteDB server. Alternatively, you can also specify the password by setting the environment variable TARGET_DB_PASSWORD. If you don't provide a password via the CLI, yb-voyager will prompt you at runtime for a password. If the password contains special characters that are interpreted by the shell (for example, # and $), enclose the password in single quotes.")

	cmd.Flags().StringVar(&tconf.DBName, "target-db-name", "",
		"name of the database on the target YugabyteDB server on which import needs to be done")

	cmd.Flags().StringVar(&tconf.Schema, "target-db-schema", "",
		"target schema name in YugabyteDB (Note: works only for source as Oracle and MySQL, in case of PostgreSQL you can ALTER schema name post import)")

	// TODO: SSL related more args might come. Need to explore SSL part completely.
	cmd.Flags().StringVar(&tconf.SSLCertPath, "target-ssl-cert", "",
		"Path of file containing target SSL Certificate")

	cmd.Flags().StringVar(&tconf.SSLMode, "target-ssl-mode", "prefer",
		"specify the target SSL mode: (disable, allow, prefer, require, verify-ca, verify-full)")

	cmd.Flags().StringVar(&tconf.SSLKey, "target-ssl-key", "",
		"Path of file containing target SSL Key")

	cmd.Flags().StringVar(&tconf.SSLRootCert, "target-ssl-root-cert", "",
		"Path of file containing target SSL Root Certificate")

	cmd.Flags().StringVar(&tconf.SSLCRL, "target-ssl-crl", "",
		"Path of file containing target SSL Root Certificate Revocation List (CRL)")
}

func registerSourceDBAsTargetConnFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&tconf.Password, "source-db-password", "",
		"source password to connect as the specified user on the source DB server. Alternatively, you can also specify the password by setting the environment variable SOURCE_DB_PASSWORD. If you don't provide a password via the CLI, yb-voyager will prompt you at runtime for a password. If the password contains special characters that are interpreted by the shell (for example, # and $), enclose the password in single quotes.")
}

func registerSourceReplicaDBAsTargetConnFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&tconf.Host, "source-replica-db-host", "127.0.0.1",
		"host on which the Source-Replica DB server is running")

	cmd.Flags().IntVar(&tconf.Port, "source-replica-db-port", 0,
		"port on which the Source-Replica DB server is running Default: ORACLE(1521), POSTGRESQL(5432)")

	cmd.Flags().StringVar(&tconf.User, "source-replica-db-user", "",
		"username with which to connect to the Source-Replica DB server")
	cmd.MarkFlagRequired("source-replica-db-user")

	cmd.Flags().StringVar(&tconf.Password, "source-replica-db-password", "",
		"password with which to connect to the Source-Replica DB server. Alternatively, you can also specify the password by setting the environment variable SOURCE_REPLICA_DB_PASSWORD. If you don't provide a password via the CLI, yb-voyager will prompt you at runtime for a password. If the password contains special characters that are interpreted by the shell (for example, # and $), enclose the password in single quotes.")

	cmd.Flags().StringVar(&tconf.DBName, "source-replica-db-name", "",
		"name of the database on the Source-Replica DB server on which import needs to be done")

	cmd.Flags().StringVar(&tconf.DBSid, "source-replica-db-sid", "",
		"[For Oracle Only] Oracle System Identifier (SID) that you wish to use while importing data to Oracle instances")

	cmd.Flags().StringVar(&tconf.OracleHome, "oracle-home", "",
		"[For Oracle Only] Path to set $ORACLE_HOME environment variable. tnsnames.ora is found in $ORACLE_HOME/network/admin")

	cmd.Flags().StringVar(&tconf.TNSAlias, "oracle-tns-alias", "",
		"[For Oracle Only] Name of TNS Alias you wish to use to connect to Oracle instance. Refer to documentation to learn more about configuring tnsnames.ora and aliases")

	cmd.Flags().StringVar(&tconf.Schema, "source-replica-db-schema", "",
		"schema name in Source-Replica DB (Note: works only for source as Oracle, in case of PostgreSQL schemas remain same as of source)")

	// TODO: SSL related more args might come. Need to explore SSL part completely.
	cmd.Flags().StringVar(&tconf.SSLCertPath, "source-replica-ssl-cert", "",
		"Path of the file containing Source-Replica DB SSL Certificate Path")

	cmd.Flags().StringVar(&tconf.SSLMode, "source-replica-ssl-mode", "prefer",
		"specify the Source-Replica DB SSL mode out of - disable, allow, prefer, require, verify-ca, verify-full")

	cmd.Flags().StringVar(&tconf.SSLKey, "source-replica-ssl-key", "",
		"Path of the file containing Source-Replica DB SSL Key")

	cmd.Flags().StringVar(&tconf.SSLRootCert, "source-replica-ssl-root-cert", "",
		"Path of the file containing Source-Replica DB SSL Root Certificate")

	cmd.Flags().StringVar(&tconf.SSLCRL, "source-replica-ssl-crl", "",
		"Path of the file containing Source-Replica DB SSL Root Certificate Revocation List (CRL)")
}

func registerImportDataCommonFlags(cmd *cobra.Command) {
	BoolVar(cmd.Flags(), &disablePb, "disable-pb", false,
		"Disable progress bar/stats during data import (default false)")

	cmd.Flags().IntVar(&EVENT_BATCH_MAX_RETRY_COUNT, "max-retries", 50, "Maximum number of retries for failed event batch in live migration")
	cmd.Flags().MarkHidden("max-retries") // majorly for automation as we don't want any retries to happen in automation for even retryable errors

	cmd.Flags().StringVar(&tconf.ExcludeTableList, "exclude-table-list", "",
		"comma-separated list of the source db table names to exclude while import data.\n"+
			"Table names can include glob wildcard characters ? (matches one character) and * (matches zero or more characters) \n"+
			`In case the table names are case sensitive, double-quote them. For example --exclude-table-list 'orders,"Products",items'`)
	cmd.Flags().StringVar(&tconf.TableList, "table-list", "",
		"comma-separated list of the source db table names to include while importing data.\n"+
			"Table names can include glob wildcard characters ? (matches one character) and * (matches zero or more characters) \n"+
			`In case the table names are case sensitive, double-quote them. For example --table-list 'orders,"Products",items'`)

	cmd.Flags().StringVar(&excludeTableListFilePath, "exclude-table-list-file-path", "",
		"path of the file containing for list of the source db table names to exclude while importing data")
	cmd.Flags().StringVar(&tableListFilePath, "table-list-file-path", "",
		"path of the file containing the list of the source db table names to import data")

	BoolVar(cmd.Flags(), &tconf.EnableUpsert, "enable-upsert", false,
		"Enable UPSERT mode on target tables. WARNING: Ensure that tables on target YugabyteDB do not have secondary indexes. If a table has secondary indexes, setting this flag to true may lead to corruption of the indexes. (default false)")
	BoolVar(cmd.Flags(), &tconf.UsePublicIP, "use-public-ip", false,
		"Use the public IPs of the nodes to distribute --parallel-jobs uniformly for data import (default false)\n"+
			"Note: you might need to configure database to have public_ip available by setting server-broadcast-addresses.\n"+
			"Refer: https://docs.yugabyte.com/preview/reference/configuration/yb-tserver/#server-broadcast-addresses")
	cmd.Flags().StringVar(&tconf.TargetEndpoints, "target-endpoints", "",
		"comma separated list of node's endpoint to use for parallel import of data(default is to use all the nodes in the cluster).\n"+
			"For example: \"host1:port1,host2:port2\" or \"host1,host2\"\n"+
			"Note: use-public-ip flag will be ignored if this is used.")
	// flag existence depends on fix of this gh issue: https://github.com/yugabyte/yugabyte-db/issues/12464
	BoolVar(cmd.Flags(), &tconf.DisableTransactionalWrites, "disable-transactional-writes", false,
		"Disable transactional writes in tables for faster data ingestion (default false)\n"+
			"(Note: this is a interim flag until the issues related to 'yb_disable_transactional_writes' session variable are fixed. Refer: https://github.com/yugabyte/yugabyte-db/issues/12464)")
	// Hidden for beta2.0 release (and onwards until further notice).
	cmd.Flags().MarkHidden("disable-transactional-writes")

	BoolVar(cmd.Flags(), &truncateSplits, "truncate-splits", true,
		"Truncate splits after importing")
	cmd.Flags().MarkHidden("truncate-splits")
}

func registerImportDataToTargetFlags(cmd *cobra.Command) {
	BoolVar(cmd.Flags(), &startClean, "start-clean", false,
		`Starts a fresh import with exported data files present in the export-dir/data directory. 
If any table on YugabyteDB database is non-empty, it prompts whether you want to continue the import without truncating those tables; 
If you go ahead without truncating, then yb-voyager starts ingesting the data present in the data files with upsert mode.
Note that for the cases where a table doesn't have a primary key, this may lead to insertion of duplicate data. To avoid this, exclude the table using the --exclude-file-list or truncate those tables manually before using the start-clean flag (default false)`)
	BoolVar(cmd.Flags(), &truncateTables, "truncate-tables", false, "Truncate tables on target YugabyteDB before importing data. Only applicable along with --start-clean true (default false)")
}

func registerImportSchemaFlags(cmd *cobra.Command) {
	BoolVar(cmd.Flags(), &startClean, "start-clean", false,
		"Delete all schema objects and start a fresh import (default false)")
	cmd.Flags().StringVar(&tconf.ImportObjects, "object-type-list", "",
		"comma separated list of schema object types to include while importing schema")
	cmd.Flags().StringVar(&tconf.ExcludeImportObjects, "exclude-object-type-list", "",
		"comma separated list of schema object types to exclude while importing schema (ignored if --object-type-list is used)")
	BoolVar(cmd.Flags(), &importObjectsInStraightOrder, "straight-order", false,
		"Imports the schema objects in the order specified via the --object-type-list flag (default false)")
	BoolVar(cmd.Flags(), &flagPostSnapshotImport, "post-snapshot-import", false,
		"Perform schema related tasks on target YugabyteDB after data import is complete. Use --refresh-mviews along with this flag to refresh materialized views.")
	BoolVar(cmd.Flags(), &tconf.IgnoreIfExists, "ignore-exist", false,
		"ignore errors if object already exists (default false)")
	BoolVar(cmd.Flags(), &flagRefreshMViews, "refresh-mviews", false,
		"Refreshes the materialised views on target during post snapshot import phase (default false)")
	BoolVar(cmd.Flags(), &enableOrafce, "enable-orafce", true,
		"enable Orafce extension on target(if source db type is Oracle)")
}

func validateTargetPortRange() {
	if tconf.Port == 0 {
		if tconf.TargetDBType == ORACLE {
			tconf.Port = ORACLE_DEFAULT_PORT
		} else if tconf.TargetDBType == YUGABYTEDB {
			tconf.Port = YUGABYTEDB_YSQL_DEFAULT_PORT
		} else if tconf.TargetDBType == POSTGRESQL {
			tconf.Port = POSTGRES_DEFAULT_PORT
		}
		return
	}

	if tconf.Port < 0 || tconf.Port > 65535 {
		utils.ErrExit("Invalid port number %d. Valid range is 0-65535", tconf.Port)
	}
}

func validateTargetSchemaFlag() {
	// do not check for source and source-replica imports
	if importerRole == TARGET_DB_IMPORTER_ROLE {
		if tconf.Schema != "" && sourceDBType == "postgresql" {
			utils.ErrExit("Error: --target-db-schema flag is not valid for export from 'postgresql' db type")
		}
	}

	if tconf.Schema == "" {
		if tconf.TargetDBType == YUGABYTEDB {
			tconf.Schema = YUGABYTEDB_DEFAULT_SCHEMA
		} else if tconf.TargetDBType == ORACLE {
			tconf.Schema = tconf.User
		}
		return
	}
}

func getTargetPassword(cmd *cobra.Command) {
	var err error
	tconf.Password, err = getPassword(cmd, "target-db-password", "TARGET_DB_PASSWORD")
	if err != nil {
		utils.ErrExit("error in getting target-db-password: %v", err)
	}
}

func getSourceReplicaDBPassword(cmd *cobra.Command) {
	var err error
	tconf.Password, err = getPassword(cmd, "source-replica-db-password", "SOURCE_REPLICA_DB_PASSWORD")
	if err != nil {
		utils.ErrExit("error while getting source-replica-db-password: %w", err)
	}
}

func getSourceDBPassword(cmd *cobra.Command) {
	var err error
	tconf.Password, err = getPassword(cmd, "source-db-password", "SOURCE_DB_PASSWORD")
	if err != nil {
		utils.ErrExit("error while getting source-db-password: %w", err)
	}
}

func validateImportObjectsFlag(importObjectsString string, flagName string) {
	if importObjectsString == "" {
		return
	}

	availableObjects := utils.GetSchemaObjectList(GetSourceDBTypeFromMSR())
	objectList := utils.CsvStringToSlice(importObjectsString)
	for _, object := range objectList {
		if !slices.Contains(availableObjects, strings.ToUpper(object)) {
			utils.ErrExit("Error: Invalid object type '%v' specified wtih --%s flag. Supported object types are: %v", object, flagName, availableObjects)
		}
	}
}

func checkOrSetDefaultTargetSSLMode() {
	if tconf.SSLMode == "" {
		tconf.SSLMode = "prefer"
	} else if tconf.SSLMode != "disable" && tconf.SSLMode != "prefer" && tconf.SSLMode != "require" && tconf.SSLMode != "verify-ca" && tconf.SSLMode != "verify-full" {
		utils.ErrExit("Invalid sslmode %q. Required one of [disable, allow, prefer, require, verify-ca, verify-full]", tconf.SSLMode)
	}
}

func registerFlagsForTarget(cmd *cobra.Command) {
	cmd.Flags().Int64Var(&batchSizeInNumRows, "batch-size", 0,
		fmt.Sprintf("Size of batches in the number of rows generated for ingestion during import. default(%d)", DEFAULT_BATCH_SIZE_YUGABYTEDB))
	cmd.Flags().IntVar(&tconf.Parallelism, "parallel-jobs", 0,
		"number of parallel jobs to use while importing data. By default, voyager will try if it can determine the total "+
			"number of cores N and use N/4 as parallel jobs. "+
			"Otherwise, it fall back to using twice the number of nodes in the cluster. "+
			"Any value less than 1 reverts to the default calculation.")
	BoolVar(cmd.Flags(), &tconf.EnableYBAdaptiveParallelism, "enable-adaptive-parallelism", true,
		"Adapt parallelism based on the resource usage (CPU, memory) of the target YugabyteDB cluster.")
	cmd.Flags().IntVar(&tconf.MaxParallelism, "adaptive-parallelism-max", 0,
		"number of max parallel jobs to use while importing data when adaptive parallelism is enabled."+
			"By default, voyager will try if it can determine the total number of cores N and use N/2 as the max parallel jobs. ")
}

func registerFlagsForSourceReplica(cmd *cobra.Command) {
	cmd.Flags().Int64Var(&batchSizeInNumRows, "batch-size", 0,
		fmt.Sprintf("Size of batches in the number of rows generated for ingestion during import. default: ORACLE(%d), POSTGRESQL(%d)", DEFAULT_BATCH_SIZE_ORACLE, DEFAULT_BATCH_SIZE_POSTGRESQL))
	cmd.Flags().IntVar(&tconf.Parallelism, "parallel-jobs", 0,
		"number of parallel jobs to use while importing data. default: For PostgreSQL(voyager will try if it can determine the total "+
			"number of cores N and use N/2 as parallel jobs else it will fall back to 8) and Oracle(16). "+
			"Any value less than 1 reverts to the default calculation.")
}

func validateBatchSizeFlag(numLinesInASplit int64) {
	if batchSizeInNumRows <= 0 {
		if tconf.TargetDBType == ORACLE {
			batchSizeInNumRows = DEFAULT_BATCH_SIZE_ORACLE
		} else if tconf.TargetDBType == POSTGRESQL {
			batchSizeInNumRows = DEFAULT_BATCH_SIZE_POSTGRESQL
		} else {
			batchSizeInNumRows = DEFAULT_BATCH_SIZE_YUGABYTEDB
		}
		return
	}

	var defaultBatchSize int64
	if tconf.TargetDBType == ORACLE {
		defaultBatchSize = DEFAULT_BATCH_SIZE_ORACLE
	} else if tconf.TargetDBType == POSTGRESQL {
		defaultBatchSize = DEFAULT_BATCH_SIZE_POSTGRESQL
	} else {
		defaultBatchSize = DEFAULT_BATCH_SIZE_YUGABYTEDB
	}

	if numLinesInASplit > defaultBatchSize {
		utils.PrintAndLog(color.YellowString("Increasing the --batch-size to a very high value is not recommended as the whole batch is executed in one transaction."))
	}
}

func validateFFDBSchemaFlag() {
	if tconf.Schema == "" && tconf.TargetDBType == ORACLE {
		utils.ErrExit("Error: --source-replica-db-schema flag is mandatory for import data to source-replica")
	}
}

func validateParallelismFlags() {
	if tconf.EnableYBAdaptiveParallelism {
		if tconf.Parallelism > 0 {
			utils.ErrExit("Error: --parallel-jobs flag cannot be used with --enable-adaptive-parallelism true. If you wish to set the number of parallel jobs explicitly, disable adaptive parallelism using --enable-adaptive-parallelism false")
		}
	}
	if tconf.MaxParallelism > 0 {
		if !tconf.EnableYBAdaptiveParallelism {
			utils.ErrExit("Error: --adaptive-parallelism-max flag can only be used with --enable-adaptive-parallelism true")
		}
	}

}

func validateTruncateTablesFlag() {
	if truncateTables && !startClean {
		utils.ErrExit("Error: --truncate-tables true can only be specified along with --start-clean true")
	}
}
