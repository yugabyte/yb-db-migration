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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/cp"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/srcdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

var importSchemaCmd = &cobra.Command{
	Use: "schema",
	Short: "Import schema into the target YugabyteDB database\n" +
		"For more details and examples, visit https://docs.yugabyte.com/preview/yugabyte-voyager/reference/schema-migration/import-schema/",

	PreRun: func(cmd *cobra.Command, args []string) {
		if !schemaIsExported() {
			utils.ErrExit("Error: schema is not exported yet.")
		}
		if tconf.TargetDBType == "" {
			tconf.TargetDBType = YUGABYTEDB
		}
		err := retrieveMigrationUUID()
		if err != nil {
			utils.ErrExit("failed to get migration UUID: %w", err)
		}
		sourceDBType = GetSourceDBTypeFromMSR()
		err = validateImportFlags(cmd, TARGET_DB_IMPORTER_ROLE)
		if err != nil {
			utils.ErrExit("Error: %s", err.Error())
		}
	},

	Run: func(cmd *cobra.Command, args []string) {
		tconf.ImportMode = true
		err := importSchema()
		if err != nil {
			packAndSendImportSchemaPayload(ERROR, err.Error())
			utils.ErrExit("error in importing schema: %s", err)
		}
		packAndSendImportSchemaPayload(COMPLETE, "")
	},
}

func init() {
	importCmd.AddCommand(importSchemaCmd)
	registerCommonGlobalFlags(importSchemaCmd)
	registerCommonImportFlags(importSchemaCmd)
	registerTargetDBConnFlags(importSchemaCmd)
	registerImportSchemaFlags(importSchemaCmd)
}

var flagPostSnapshotImport utils.BoolStr
var importObjectsInStraightOrder utils.BoolStr
var flagRefreshMViews utils.BoolStr
var invalidTargetIndexesCache map[string]bool

func importSchema() error {

	tconf.Schema = strings.ToLower(tconf.Schema)

	if callhome.SendDiagnostics || getControlPlaneType() == YUGABYTED {
		tconfSchema := tconf.Schema
		// setting the tconf schema to public here for initalisation to handle cases where non-public target schema
		// is not created as it will be created with `createTargetSchemas` func, so not a problem in using public as it will be
		// available always and this is just for initialisation of tdb and marking it nil again back.
		tconf.Schema = "public"
		tdb = tgtdb.NewTargetDB(&tconf)
		err := tdb.Init()
		if err != nil {
			utils.ErrExit("Failed to initialize the target DB: %s", err)
		}
		targetDBDetails = tdb.GetCallhomeTargetDBInfo()
		//Marking tdb as nil back to not allow others to use it as this is just dummy initialisation of tdb
		//with public schema so Reintialise tdb if required with proper configs when it is available.
		tdb.Finalize()
		tdb = nil
		tconf.Schema = tconfSchema
	}

	if tconf.RunGuardrailsChecks {
		// Check import schema permissions
		missingPermissions, err := getMissingImportSchemaPermissions()
		if err != nil {
			utils.ErrExit("Failed to get missing import schema permissions: %s", err)
		}
		if len(missingPermissions) > 0 {
			output := strings.Join(missingPermissions, "\n")
			utils.PrintAndLog(output)

			link := "https://docs.yugabyte.com/preview/yugabyte-voyager/migrate/migrate-steps/#prepare-the-target-database"
			fmt.Println("\nCheck the documentation to prepare the database for migration:", color.BlueString(link))

			// Prompt user to continue if missing permissions
			if !utils.AskPrompt("Do you want to continue anyway") {
				utils.ErrExit("Grant the required permissions and try again.")
			}
		} else {
			log.Info("The target database has the required permissions for importing schema.")
		}
	}

	importSchemaStartEvent := createImportSchemaStartedEvent()
	controlPlane.ImportSchemaStarted(&importSchemaStartEvent)

	conn, err := pgx.Connect(context.Background(), tconf.GetConnectionUri())
	if err != nil {
		return fmt.Errorf("unable to connect to target YugabyteDB database: %v", err)
	}
	defer conn.Close(context.Background())

	targetDBVersion := ""
	query := "SELECT setting FROM pg_settings WHERE name = 'server_version'"
	err = conn.QueryRow(context.Background(), query).Scan(&targetDBVersion)
	if err != nil {
		return fmt.Errorf("get target db version: %s", err)
	}
	utils.PrintAndLog("YugabyteDB version: %s\n", targetDBVersion)

	migrationAssessmentDoneAndApplied, err := MigrationAssessmentDoneAndApplied()
	if err != nil {
		return err
	}

	if migrationAssessmentDoneAndApplied && !isYBDatabaseIsColocated(conn) && !utils.AskPrompt(fmt.Sprintf("\nWarning: Target DB '%s' is a non-colocated database, colocated tables can't be created in a non-colocated database.\n", tconf.DBName),
		"Use a colocated database if your schema contains colocated tables. Do you still want to continue") {
		utils.ErrExit("Exiting...")
	}

	if !flagPostSnapshotImport {
		filePath := filepath.Join(exportDir, "schema", "uncategorized.sql")
		if utils.FileOrFolderExists(filePath) {
			color.Red("\nIMPORTANT NOTE: Please, review and manually import the DDL statements from the %q\n", filePath)
		}

		createTargetSchemas(conn)
		installOrafceIfRequired(conn)
	}

	var objectList []string
	// Pre data load.
	// This list also has defined the order to create object type in target YugabyteDB.
	// if post snapshot import, no objects should be imported.
	if !flagPostSnapshotImport {
		objectList = utils.GetSchemaObjectList(sourceDBType)
		if len(objectList) == 0 {
			utils.ErrExit("No schema objects to import! Must import at least 1 of the supported schema object types: %v", utils.GetSchemaObjectList(sourceDBType))
		}

		objectList = applySchemaObjectFilterFlags(objectList)
		log.Infof("list of schema objects to import: %v", objectList)
		// Import some statements only after importing everything else
		isSkipStatement := func(objType, stmt string) bool {
			stmt = strings.ToUpper(strings.TrimSpace(stmt))
			switch objType {
			case "SEQUENCE":
				// ALTER TABLE table_name ALTER COLUMN column_name ... ('sequence_name');
				// ALTER SEQUENCE sequence_name OWNED BY table_name.column_name;
				return strings.HasPrefix(stmt, "ALTER TABLE") || strings.HasPrefix(stmt, "ALTER SEQUENCE")
			case "TABLE":
				// skips the ALTER TABLE table_name ADD CONSTRAINT constraint_name FOREIGN KEY (column_name) REFERENCES another_table_name(another_column_name);
				return strings.Contains(stmt, "ALTER TABLE") && strings.Contains(stmt, "FOREIGN KEY")
			case "UNIQUE INDEX":
				// skips all the INDEX DDLs, Except CREATE UNIQUE INDEX index_name ON table ... (column_name);
				return !strings.Contains(stmt, objType)
			case "INDEX":
				// skips all the CREATE UNIQUE INDEX index_name ON table ... (column_name);
				return strings.Contains(stmt, "UNIQUE INDEX")
			}
			return false
		}
		skipFn := isSkipStatement
		err = importSchemaInternal(exportDir, objectList, skipFn)
		if err != nil {
			return err
		}

		// Import the skipped ALTER TABLE statements from sequence.sql and table.sql if it exists
		skipFn = func(objType, stmt string) bool {
			return !isSkipStatement(objType, stmt)
		}
		if slices.Contains(objectList, "SEQUENCE") {
			err = importSchemaInternal(exportDir, []string{"SEQUENCE"}, skipFn)
			if err != nil {
				return err
			}
		}
		if slices.Contains(objectList, "TABLE") {
			err = importSchemaInternal(exportDir, []string{"TABLE"}, skipFn)
			if err != nil {
				return err
			}
		}

		importDeferredStatements()
		log.Info("Schema import is complete.")

		dumpStatements(finalFailedSqlStmts, filepath.Join(exportDir, "schema", "failed.sql"))
	}

	if flagPostSnapshotImport {
		err = importSchemaInternal(exportDir, []string{"TABLE"}, nil)
		if err != nil {
			return err
		}
		if flagRefreshMViews {
			refreshMViews(conn)
		}
	} else {
		utils.PrintAndLog("\nNOTE: Materialized Views are not populated by default. To populate them, pass --refresh-mviews while executing `import schema --post-snapshot-import`.")
	}

	importSchemaCompleteEvent := createImportSchemaCompletedEvent()
	controlPlane.ImportSchemaCompleted(&importSchemaCompleteEvent)

	return nil
}

func getMissingImportSchemaPermissions() ([]string, error) {
	var missingPermissions []string

	// Check if the user has superuser privileges
	isSuperUser, err := tgtdb.IsCurrentUserSuperUser(&tconf)
	if err != nil {
		return nil, fmt.Errorf("failed to check if the current user has superuser privileges: %w", err)
	}

	if !isSuperUser {
		msg := fmt.Sprintf("The current user %q does not have superuser privileges", tconf.User)
		missingPermissions = append(missingPermissions, msg)
	}

	return missingPermissions, nil
}

func packAndSendImportSchemaPayload(status string, errMsg string) {
	if !shouldSendCallhome() {
		return
	}
	//Basic details in the payload
	payload := createCallhomePayload()
	payload.MigrationPhase = IMPORT_SCHEMA_PHASE
	payload.Status = status
	payload.TargetDBDetails = callhome.MarshalledJsonString(targetDBDetails)

	//Handling the error cases in import schema with/without continue-on-error
	var errorsList []string
	//e.g for finalFailedSqlStmts - [`/*\nERROR: changing primary key of a partitioned table is not yet implemented (SQLSTATE XX000)*/\n
	//	ALTER TABLE ONLY public.customers\n ADD CONSTRAINT customers_pkey PRIMARY KEY (id, statuses, arr);`]
	for _, stmt := range finalFailedSqlStmts {
		//parts - ["/*\nERROR: changing primary key of a partitioned table is not yet implemented (SQLSTATE XX000)" "ALTER TABLE ONLY public.customers\n ADD CONSTRAINT customers_pkey PRIMARY KEY (id, statuses, arr);"]
		parts := strings.Split(stmt, "*/\n")
		errorsList = append(errorsList, strings.Trim(parts[0], "/*\n")) //trimming the prefix of `/*\n` from parts[0] (the error msg)
	}

	if len(errorsList) > 0 && status != EXIT {
		payload.Status = COMPLETE_WITH_ERRORS
	}

	//import-schema specific payload details
	importSchemaPayload := callhome.ImportSchemaPhasePayload{
		ContinueOnError:    bool(tconf.ContinueOnError),
		EnableOrafce:       bool(enableOrafce),
		IgnoreExist:        bool(tconf.IgnoreIfExists),
		RefreshMviews:      bool(flagRefreshMViews),
		ErrorCount:         len(errorsList),
		PostSnapshotImport: bool(flagPostSnapshotImport),
		StartClean:         bool(startClean),
		Error:              callhome.SanitizeErrorMsg(errMsg),
	}
	payload.PhasePayload = callhome.MarshalledJsonString(importSchemaPayload)
	err := callhome.SendPayload(&payload)
	if err == nil && (status == COMPLETE || status == ERROR) {
		callHomeErrorOrCompletePayloadSent = true
	}
}

func isYBDatabaseIsColocated(conn *pgx.Conn) bool {
	var isColocated bool
	query := "SELECT yb_is_database_colocated();"
	err := conn.QueryRow(context.Background(), query).Scan(&isColocated)
	if err != nil {
		utils.ErrExit("failed to check if Target DB  is colocated or not: %q: %v", tconf.DBName, err)
	}
	log.Infof("target DB '%s' colocoated='%t'", tconf.DBName, isColocated)
	return isColocated
}

func dumpStatements(stmts []string, filePath string) {
	if len(stmts) == 0 {
		if flagPostSnapshotImport {
			// nothing
		} else if utils.FileOrFolderExists(filePath) {
			err := os.Remove(filePath)
			if err != nil {
				utils.ErrExit("remove file: %v", err)
			}
		}
		log.Infof("no failed sql statements to dump")
		return
	}

	var fileMode int
	if flagPostSnapshotImport {
		fileMode = os.O_WRONLY | os.O_CREATE | os.O_APPEND
	} else {
		fileMode = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	file, err := os.OpenFile(filePath, fileMode, 0644)
	if err != nil {
		utils.ErrExit("open file: %v", err)
	}

	for i := 0; i < len(stmts); i++ {
		_, err = file.WriteString(stmts[i] + "\n\n")
		if err != nil {
			utils.ErrExit("failed writing in file: %s: %v", filePath, err)
		}
	}

	msg := fmt.Sprintf("\nSQL statements failed during migration are present in %q file\n", filePath)
	color.Red(msg)
	log.Info(msg)
}

// installs Orafce extension in target YugabyteDB.
func installOrafceIfRequired(conn *pgx.Conn) {
	if sourceDBType != ORACLE || !enableOrafce {
		return
	}

	utils.PrintAndLog("Installing Orafce extension in target YugabyteDB")
	_, err := conn.Exec(context.Background(), "CREATE EXTENSION IF NOT EXISTS orafce")
	if err != nil {
		utils.ErrExit("failed to install Orafce extension: %v", err)
	}
}

func refreshMViews(conn *pgx.Conn) {
	utils.PrintAndLog("\nRefreshing Materialized Views..\n\n")
	var mViewNames []string
	mViewsSqlInfoArr := getDDLStmts("MVIEW")
	for _, eachMviewSql := range mViewsSqlInfoArr {
		if strings.Contains(strings.ToUpper(eachMviewSql.stmt), "CREATE MATERIALIZED VIEW") {
			mViewNames = append(mViewNames, eachMviewSql.objName)
		}
	}
	log.Infof("List of Mviews Imported to refresh - %v", mViewNames)
	for _, mViewName := range mViewNames {
		query := fmt.Sprintf("REFRESH MATERIALIZED VIEW %s", mViewName)
		_, err := conn.Exec(context.Background(), query)
		if err != nil && !strings.Contains(strings.ToLower(err.Error()), "has not been populated") {
			utils.ErrExit("error in refreshing the materialized view: %s: %v", mViewName, err)
		}
	}
	log.Infof("Checking if mviews are refreshed or not - %v", mViewNames)
	var mviewsNotRefreshed []string
	for _, mViewName := range mViewNames {
		query := fmt.Sprintf("SELECT * from %s LIMIT 1;", mViewName)
		rows, err := conn.Query(context.Background(), query)
		if err != nil {
			utils.ErrExit("error in checking whether mview  is refreshed or not: %q: %v", mViewName, err)
		}
		if !rows.Next() {
			mviewsNotRefreshed = append(mviewsNotRefreshed, mViewName)
		}
		rows.Close()
	}
	if len(mviewsNotRefreshed) > 0 {
		utils.PrintAndLog("\nNOTE: Following Materialized Views might not be refreshed - %v, Please verify and refresh them manually if required!", mviewsNotRefreshed)
	}
}

func getDDLStmts(objType string) []sqlInfo {
	var sqlInfoArr []sqlInfo
	schemaDir := filepath.Join(exportDir, "schema")
	importMViewFilePath := utils.GetObjectFilePath(schemaDir, objType)
	if utils.FileOrFolderExists(importMViewFilePath) {
		sqlInfoArr = parseSqlFileForObjectType(importMViewFilePath, objType)
	}
	return sqlInfoArr
}

func createTargetSchemas(conn *pgx.Conn) {
	var targetSchemas []string
	tconf.Schema = strings.ToLower(strings.Trim(tconf.Schema, "\"")) //trim case sensitivity quotes if needed, convert to lowercase

	schemaAnalysisReport := analyzeSchemaInternal(
		&srcdb.Source{
			DBType: sourceDBType,
		}, false)

	switch sourceDBType {
	case "postgresql": // in case of postgreSQL as source, there can be multiple schemas present in a database
		source = srcdb.Source{DBType: sourceDBType}
		targetSchemas = utils.GetObjectNameListFromReport(schemaAnalysisReport, "SCHEMA")
	case "oracle": // ORACLE PACKAGEs are exported as SCHEMAs
		source = srcdb.Source{DBType: sourceDBType}
		targetSchemas = append(targetSchemas, tconf.Schema)
		targetSchemas = append(targetSchemas, utils.GetObjectNameListFromReport(schemaAnalysisReport, "PACKAGE")...)
	case "mysql":
		source = srcdb.Source{DBType: sourceDBType}
		targetSchemas = append(targetSchemas, tconf.Schema)

	}
	targetSchemas = utils.ToCaseInsensitiveNames(targetSchemas)

	utils.PrintAndLog("schemas to be present in target database %q: %v\n", tconf.DBName, targetSchemas)
	for _, targetSchema := range targetSchemas {
		//check if target schema exists or not
		schemaExists := checkIfTargetSchemaExists(conn, targetSchema)
		dropSchemaQuery := fmt.Sprintf("DROP SCHEMA %s CASCADE", targetSchema)

		if schemaExists {
			if startClean {
				promptMsg := fmt.Sprintf("do you really want to drop the '%s' schema", targetSchema)
				if !utils.AskPrompt(promptMsg) {
					continue
				}

				utils.PrintAndLog("dropping schema '%s' in target database", targetSchema)
				_, err := conn.Exec(context.Background(), dropSchemaQuery)
				if err != nil {
					utils.ErrExit("Failed to drop schema: %q: %s", targetSchema, err)
				}
			} else {
				utils.PrintAndLog("schema '%s' already present in target database, continuing with it..\n", targetSchema)
			}
		}
	}

	if sourceDBType != POSTGRESQL { // with the new schema list flag, pg_dump takes care of all schema creation DDLs
		schemaExists := checkIfTargetSchemaExists(conn, tconf.Schema)
		createSchemaQuery := fmt.Sprintf("CREATE SCHEMA %s", tconf.Schema)
		/* --target-db-schema(or target.Schema) flag valid for Oracle & MySQL
		only create target.Schema, other required schemas are created via .sql files */
		if !schemaExists {
			utils.PrintAndLog("creating schema '%s' in target database...", tconf.Schema)
			_, err := conn.Exec(context.Background(), createSchemaQuery)
			if err != nil {
				utils.ErrExit("Failed to create schema in the target DB: %q: %s", tconf.Schema, err)
			}
		}

		if tconf.Schema == YUGABYTEDB_DEFAULT_SCHEMA &&
			!utils.AskPrompt("do you really want to import into 'public' schema") {
			utils.ErrExit("User selected not to import in the `public` schema. Exiting.")
		}
	}
}

func checkIfTargetSchemaExists(conn *pgx.Conn, targetSchema string) bool {
	checkSchemaExistQuery := fmt.Sprintf("select nspname from pg_namespace n where n.nspname = '%s'", targetSchema)

	var fetchedSchema string
	err := conn.QueryRow(context.Background(), checkSchemaExistQuery).Scan(&fetchedSchema)
	log.Infof("check if schema %q exists: fetchedSchema: %q, err: %s", targetSchema, fetchedSchema, err)
	if err != nil && (strings.Contains(err.Error(), "no rows in result set") && fetchedSchema == "") {
		return false
	} else if err != nil {
		utils.ErrExit("Failed to check if schema exists: %q: %s", targetSchema, err)
	}

	return fetchedSchema == targetSchema
}

func missingRequiredSchemaObject(err error) bool {
	return strings.Contains(err.Error(), "does not exist")
}

func isAlreadyExists(errString string) bool {
	alreadyExistsErrors := []string{"already exists",
		"multiple primary keys",
		"already a partition"}
	for _, subStr := range alreadyExistsErrors {
		if strings.Contains(errString, subStr) {
			return true
		}
	}
	return false
}

func createImportSchemaStartedEvent() cp.ImportSchemaStartedEvent {
	result := cp.ImportSchemaStartedEvent{}
	initBaseTargetEvent(&result.BaseEvent, "IMPORT SCHEMA")
	return result
}

func createImportSchemaCompletedEvent() cp.ImportSchemaCompletedEvent {
	result := cp.ImportSchemaCompletedEvent{}
	initBaseTargetEvent(&result.BaseEvent, "IMPORT SCHEMA")
	return result
}

func MigrationAssessmentDoneAndApplied() (bool, error) {
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		return false, fmt.Errorf("failed to get migration status record for targetDB colocation check: %w", err)
	}

	return (msr.MigrationAssessmentDone && msr.AssessmentRecommendationsApplied), nil
}
