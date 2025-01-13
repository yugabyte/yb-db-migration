package cmd

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/dustin/go-humanize"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/config"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/dbzm"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/lockfile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/metadb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/srcdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

var (
	backupSchemaFiles       utils.BoolStr
	backupDataFiles         utils.BoolStr
	saveMigrationReports    utils.BoolStr
	backupLogFiles          utils.BoolStr
	backupDir               string
	targetDBPassword        string
	sourceReplicaDBPassword string
	sourceDBPassword        string
	streamChangesMode       bool
)

var endMigrationCmd = &cobra.Command{
	Use:   "migration",
	Short: "End the current migration and cleanup all metadata stored in databases(Target, Source-Replica and Source) and export-dir",
	Long:  "End the current migration and cleanup all metadata stored in databases(Target, Source-Replica and Source) and export-dir",

	PreRun: func(cmd *cobra.Command, args []string) {
		if utils.IsDirectoryEmpty(exportDir) {
			utils.ErrExit("export directory is empty, nothing to end")
		}

		err := retrieveMigrationUUID()
		if err != nil {
			utils.ErrExit("failed to get migration UUID: %w", err)
		}
		err = validateEndMigrationFlags(cmd)
		if err != nil {
			utils.ErrExit(err.Error())
		}

	},

	Run: endMigrationCommandFn,
}

func endMigrationCommandFn(cmd *cobra.Command, args []string) {
	if utils.AskPrompt("Migration can't be resumed or continued after this.", "Are you sure you want to end the migration") {
		log.Info("ending the migration")
	} else {
		utils.PrintAndLog("aborting the end migration command")
		return
	}

	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("getting migration status record: %v", err)
	} else if msr == nil {
		utils.ErrExit("migration status record not found. Is the migration initialized?")
	}

	streamChangesMode, err = checkStreamingMode()
	if err != nil {
		utils.ErrExit("error while checking streaming mode: %w\n", err)
	}

	checkIfEndCommandCanBePerformed(msr)

	// backing up the state from the export directory
	saveMigrationReportsFn(msr)
	backupSchemaFilesFn()
	backupDataFilesFn()

	// cleaning only the migration state wherever and  whatever required
	cleanupSourceDB(msr)
	cleanupTargetDB(msr)
	cleanupSourceReplicaDB(msr)
	cleanupFallBackDB(msr)

	backupLogFilesFn()
	if backupDir != "" {
		utils.PrintAndLog("saved the backup at %q", backupDir)
	}

	cleanupExportDir()
	utils.PrintAndLog("Migration ended successfully")
	packAndSendEndMigrationPayload(COMPLETE, "")
}

func packAndSendEndMigrationPayload(status string, errorMsg string) {
	if !shouldSendCallhome() {
		return
	}
	payload := createCallhomePayload()
	payload.MigrationType = OFFLINE
	if streamChangesMode {
		payload.MigrationType = LIVE_MIGRATION
	}
	payload.MigrationPhase = END_MIGRATION_PHASE
	endMigrationPayload := callhome.EndMigrationPhasePayload{
		BackupDataFiles:      bool(backupDataFiles),
		BackupLogFiles:       bool(backupLogFiles),
		BackupSchemaFiles:    bool(backupSchemaFiles),
		SaveMigrationReports: bool(saveMigrationReports),
		Error:                callhome.SanitizeErrorMsg(errorMsg),
	}
	payload.PhasePayload = callhome.MarshalledJsonString(endMigrationPayload)
	payload.Status = status

	err := callhome.SendPayload(&payload)
	if err == nil && (status == COMPLETE || status == ERROR) {
		callHomeErrorOrCompletePayloadSent = true
	}
}

func backupSchemaFilesFn() {
	schemaDirPath := filepath.Join(exportDir, "schema")
	backupSchemaDirPath := filepath.Join(backupDir, "schema")
	if !bool(backupSchemaFiles) {
		return
	} else if utils.FileOrFolderExists(backupSchemaDirPath) {
		// TODO: check can be made more robust by checking the contents of the backup-dir/schema
		utils.PrintAndLog("schema files are already backed up at %q", backupSchemaDirPath)
		return
	}

	utils.PrintAndLog("backing up schema files...")
	cmd := exec.Command("mv", schemaDirPath, backupSchemaDirPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		utils.ErrExit("moving schema files: %s: %v", string(output), err)
	} else {
		log.Infof("moved schema files from %q to %q", schemaDirPath, backupSchemaDirPath)
	}
}

func backupDataFilesFn() {
	if !backupDataFiles {
		return
	}

	utils.PrintAndLog("backing up snapshot sql data files")
	err := os.MkdirAll(filepath.Join(backupDir, "data"), 0755)
	if err != nil {
		utils.ErrExit("creating data directory for backup: %v", err)
	}

	files, err := os.ReadDir(filepath.Join(exportDir, "data"))
	if err != nil {
		utils.ErrExit("reading data directory: %v", err)
	}
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}

		dataFilePath := filepath.Join(exportDir, "data", file.Name())
		backupFilePath := filepath.Join(backupDir, "data", file.Name())

		cmd := exec.Command("mv", dataFilePath, backupFilePath)
		output, err := cmd.CombinedOutput()
		if err != nil {
			utils.ErrExit("moving data files: %s: %v", string(output), err)
		} else {
			log.Infof("moved data file %q to %q", dataFilePath, backupFilePath)
		}
	}
}

func saveMigrationReportsFn(msr *metadb.MigrationStatusRecord) {
	if !saveMigrationReports {
		return
	}

	err := os.MkdirAll(filepath.Join(backupDir, "reports"), 0755)
	if err != nil {
		utils.ErrExit("creating reports directory for backup: %v", err)
	}

	saveMigrationAssessmentReport()
	saveSchemaAnalysisReport()
	if streamChangesMode {
		saveDataMigrationReport(msr)
	} else { // snapshot case
		if msr.SnapshotMechanism != "" {
			saveDataExportReport()
		}
		saveDataImportReport(msr)
	}
}

func saveMigrationAssessmentReport() {
	assessmentReportGlobPath := filepath.Join(backupDir, "reports", fmt.Sprintf("%s.*", ASSESSMENT_FILE_NAME))
	alreadyBackedUp := utils.FileOrFolderExistsWithGlobPattern(assessmentReportGlobPath)
	migrationAssessmentDone, err := IsMigrationAssessmentDone(metaDB)
	if err != nil {
		utils.ErrExit("checking if migration assessment is done: %v", err)
	}
	if alreadyBackedUp {
		utils.PrintAndLog("assessment report is already present at %q", assessmentReportGlobPath)
		return
	} else if !migrationAssessmentDone {
		utils.PrintAndLog("no assessment report to save as assessment command is not executed as part of migration workflow")
		return
	}
	utils.PrintAndLog("saving assessment report...")
	files, err := os.ReadDir(filepath.Join(exportDir, "assessment", "reports"))
	if err != nil {
		utils.ErrExit("reading assessment reports directory: %v", err)
	}
	for _, file := range files {
		if file.IsDir() || !strings.HasPrefix(file.Name(), fmt.Sprintf("%s.", ASSESSMENT_FILE_NAME)) {
			continue
		}
		oldPath := filepath.Join(exportDir, "assessment", "reports", file.Name())
		newPath := filepath.Join(backupDir, "reports", file.Name())
		cmd := exec.Command("mv", oldPath, newPath)
		output, err := cmd.CombinedOutput()
		if err != nil {
			utils.ErrExit("moving assessment report: %s: %v", string(output), err)
		} else {
			log.Infof("moved assessment report %q to %q", oldPath, newPath)
		}
	}
}

func saveSchemaAnalysisReport() {
	analysisReportGlobPath := filepath.Join(backupDir, "reports", fmt.Sprintf("%s.*", ANALYSIS_REPORT_FILE_NAME))
	alreadyBackedUp := utils.FileOrFolderExistsWithGlobPattern(analysisReportGlobPath)
	if alreadyBackedUp {
		utils.PrintAndLog("schema analysis report is already present at %q", analysisReportGlobPath)
		return
	} else if !schemaIsAnalyzed() {
		utils.PrintAndLog("no schema analysis report to save as analyze-schema command is not executed as part of migration workflow")
		return
	}
	utils.PrintAndLog("saving schema analysis report...")
	files, err := os.ReadDir(filepath.Join(exportDir, "reports"))
	if err != nil {
		utils.ErrExit("reading reports directory: %v", err)
	}
	for _, file := range files {
		if file.IsDir() || !strings.HasPrefix(file.Name(), fmt.Sprintf("%s.", ANALYSIS_REPORT_FILE_NAME)) {
			continue
		}

		oldPath := filepath.Join(exportDir, "reports", file.Name())
		newPath := filepath.Join(backupDir, "reports", file.Name())
		cmd := exec.Command("mv", oldPath, newPath)
		output, err := cmd.CombinedOutput()
		if err != nil {
			utils.ErrExit("moving schema analysis report: %s: %v", string(output), err)
		} else {
			log.Infof("moved schema analysis report %q to %q", oldPath, newPath)
		}
	}
}

func saveDataMigrationReport(msr *metadb.MigrationStatusRecord) {
	dataMigrationReportPath := filepath.Join(backupDir, "reports", "data_migration_report.txt")
	if utils.FileOrFolderExists(dataMigrationReportPath) {
		utils.PrintAndLog("data migration report is already present at %q", dataMigrationReportPath)
		return
	}

	utils.PrintAndLog("saving data migration report...")
	askAndStorePasswords(msr)
	passwordsEnvVars := []string{
		fmt.Sprintf("TARGET_DB_PASSWORD=%s", targetDBPassword),
		fmt.Sprintf("SOURCE_REPLICA_DB_PASSWORD=%s", sourceReplicaDBPassword),
		fmt.Sprintf("SOURCE_DB_PASSWORD=%s", sourceDBPassword),
	}

	strCmd := fmt.Sprintf("yb-voyager get data-migration-report --export-dir %s --log-level %s", exportDir, config.LogLevel)
	liveMigrationReportCmd := exec.Command("bash", "-c", strCmd)
	liveMigrationReportCmd.Env = append(os.Environ(), passwordsEnvVars...)
	saveCommandOutput(liveMigrationReportCmd, "data migration report", "", dataMigrationReportPath)
}

func saveDataExportReport() {
	exportDataReportFilePath := filepath.Join(backupDir, "reports", "export_data_report.txt")
	if utils.FileOrFolderExists(exportDataReportFilePath) {
		utils.PrintAndLog("export data report is already present at %q", exportDataReportFilePath)
	}

	utils.PrintAndLog("saving data export report...")
	strCmd := fmt.Sprintf("yb-voyager export data status --export-dir %s", exportDir)
	exportDataStatusCmd := exec.Command("bash", "-c", strCmd)
	saveCommandOutput(exportDataStatusCmd, "export data status", exportDataStatusMsg, exportDataReportFilePath)
}

func saveDataImportReport(msr *metadb.MigrationStatusRecord) {
	if !dataIsExported() {
		utils.PrintAndLog("data is not exported. skipping data import report")
		return
	}

	if msr.TargetDBConf == nil {
		utils.PrintAndLog("data import is not started. skipping data import report")
		return
	}

	importDataReportFilePath := filepath.Join(backupDir, "reports", "import_data_report.txt")
	if utils.FileOrFolderExists(importDataReportFilePath) {
		utils.PrintAndLog("import data report is already present at %q", importDataReportFilePath)
	}

	utils.PrintAndLog("saving data import report...")
	strCmd := fmt.Sprintf("yb-voyager import data status --export-dir %s", exportDir)
	importDataStatusCmd := exec.Command("bash", "-c", strCmd)
	saveCommandOutput(importDataStatusCmd, "import data status", importDataStatusMsg, importDataReportFilePath)
}

func saveCommandOutput(cmd *exec.Cmd, cmdName string, header string, reportFilePath string) {
	var outbuf, errbuf bytes.Buffer
	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf
	err := cmd.Run()
	if err != nil {
		log.Errorf("running %s command: %s: %v", cmdName, errbuf.String(), err)
		utils.ErrExit("running %s command: %s: %v", cmdName, errbuf.String(), err)
	}

	outbufBytes := bytes.Trim(outbuf.Bytes(), " \n")
	outbufBytes = append(outbufBytes, []byte("\n")...)
	if len(outbufBytes) > 0 && string(outbufBytes) != header {
		err = os.WriteFile(reportFilePath, outbufBytes, 0644)
		if err != nil {
			utils.ErrExit("writing %s report: %v", cmdName, err)
		}
	} else {
		utils.PrintAndLog("nothing to save for %s report", cmdName)
	}
}

func backupLogFilesFn() {
	if !backupLogFiles {
		return
	}
	// TODO: in case of failures when cmd is executed again, even if logs were backed up, new log file for end migration will come up

	backupLogDir := filepath.Join(backupDir, "logs")
	err := os.MkdirAll(backupLogDir, 0755)
	if err != nil {
		utils.ErrExit("creating logs directory for backup: %v", err)
	}

	utils.PrintAndLog("backing up log files")
	cmdStr := fmt.Sprintf("mv %s/logs/*.log %s", exportDir, backupLogDir)
	cmd := exec.Command("bash", "-c", cmdStr)
	output, err := cmd.CombinedOutput()
	if err != nil {
		utils.ErrExit("moving log files: %s: %v", string(output), err)
	}
}

func askAndStorePasswords(msr *metadb.MigrationStatusRecord) {
	var err error
	if msr.TargetDBConf != nil {
		targetDBPassword, err = askPassword("target DB", "", "TARGET_DB_PASSWORD")
		if err != nil {
			utils.ErrExit("getting target db password: %v", err)
		}
	}
	if msr.FallForwardEnabled {
		sourceReplicaDBPassword, err = askPassword("source-replica DB", "", "SOURCE_REPLICA_DB_PASSWORD")
		if err != nil {
			utils.ErrExit("getting source-replica db password: %v", err)
		}
	}
	if msr.FallbackEnabled {
		sourceDBPassword, err = askPassword("source DB", "", "SOURCE_DB_PASSWORD")
		if err != nil {
			utils.ErrExit("getting source password: %v", err)
		}
	}
}

func askPassword(destination string, user string, envVar string) (string, error) {
	if os.Getenv(envVar) != "" {
		return os.Getenv(envVar), nil
	}

	if user == "" {
		fmt.Printf("Password to connect to %s (In addition, you can also set the password using the environment variable '%s'): ",
			destination, envVar)
	} else {
		fmt.Printf("Password to connect to '%s' user of %s (In addition, you can also set the password using the environment variable '%s'): ",
			user, destination, envVar)
	}
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", fmt.Errorf("reading password: %w", err)
	}
	fmt.Print("\n")
	return string(bytePassword), nil
}

func cleanupSourceDB(msr *metadb.MigrationStatusRecord) {
	// there won't be anything required to be cleaned up in source-db(Oracle) for debezium snapshot migration
	// TODO: verify it for PG and MySQL
	if !streamChangesMode {
		utils.PrintAndLog("nothing to clean up in source db for snapshot migration")
		return
	}
	utils.PrintAndLog("cleaning up voyager state from source db...")
	source := msr.SourceDBConf
	if source == nil {
		log.Info("source db conf is not set. skipping cleanup")
		return
	}

	var err error
	if sourceDBPassword == "" {
		sourceDBPassword, err = askPassword("source DB", source.User, "SOURCE_DB_PASSWORD")
		if err != nil {
			utils.ErrExit("getting source db password: %v", err)
		}
	}
	source.Password = sourceDBPassword
	err = source.DB().Connect()
	if err != nil {
		utils.ErrExit("connecting to source db: %v", err)
	}
	defer source.DB().Disconnect()
	err = source.DB().ClearMigrationState(migrationUUID, exportDir)
	if err != nil {
		utils.ErrExit("clearing migration state from source db: %v", err)
	}

	deletePGReplicationSlot(msr, source)
	deletePGPublication(msr, source)
}

func deletePGReplicationSlot(msr *metadb.MigrationStatusRecord, source *srcdb.Source) {
	if msr.PGReplicationSlotName == "" || source.DBType != POSTGRESQL {
		log.Infof("pg replication slot name is not set or source db type is not postgresql. skipping deleting pg replication slot name")
		return
	}

	log.Infof("deleting PG replication slot name %q", msr.PGReplicationSlotName)
	pgDB := source.DB().(*srcdb.PostgreSQL)
	err := pgDB.DropLogicalReplicationSlot(nil, msr.PGReplicationSlotName)
	if err != nil {
		utils.ErrExit("dropping PG replication slot name: %v", err)
	}
}

func deletePGPublication(msr *metadb.MigrationStatusRecord, source *srcdb.Source) {
	if msr.PGPublicationName == "" || source.DBType != POSTGRESQL {
		log.Infof("pg publication name is not set or source db type is not postgresql. skipping deleting pg publication name")
		return
	}

	log.Infof("deleting PG publication name %q", msr.PGPublicationName)
	pgDB := source.DB().(*srcdb.PostgreSQL)
	err := pgDB.DropPublication(msr.PGPublicationName)
	if err != nil {
		utils.ErrExit("dropping PG publication name: %v", err)
	}
}

func cleanupTargetDB(msr *metadb.MigrationStatusRecord) {
	utils.PrintAndLog("cleaning up voyager state from target db...")
	if msr.TargetDBConf == nil {
		log.Info("target db conf is not set. skipping cleanup")
		return
	}

	var err error
	tconf := msr.TargetDBConf
	if targetDBPassword == "" {
		targetDBPassword, err = askPassword("target DB", tconf.User, "TARGET_DB_PASSWORD")
		if err != nil {
			utils.ErrExit("getting target db password: %v", err)
		}
	}
	tconf.Password = targetDBPassword
	tdb := tgtdb.NewTargetDB(tconf)
	err = tdb.Init()
	if err != nil {
		utils.ErrExit("initializing target db: %v", err)
	}
	defer tdb.Finalize()
	err = tdb.ClearMigrationState(migrationUUID, exportDir)
	if err != nil {
		utils.ErrExit("clearing migration state from target db: %v", err)
	}

	if msr.ExportFromTargetFallBackStarted || msr.ExportFromTargetFallForwardStarted {
		fmt.Println("deleting yb replication slot and publication")
		sourceYB := srcdb.Source{
			DBType:         tconf.TargetDBType,
			Host:           tconf.Host,
			Port:           tconf.Port,
			User:           tconf.User,
			Password:       tconf.Password,
			DBName:         tconf.DBName,
			Schema:         tconf.Schema,
			SSLMode:        tconf.SSLMode,
			SSLCertPath:    tconf.SSLCertPath,
			SSLKey:         tconf.SSLKey,
			SSLRootCert:    tconf.SSLRootCert,
			SSLCRL:         tconf.SSLCRL,
			SSLQueryString: tconf.SSLQueryString,
			Uri:            tconf.Uri,
		}
		err = sourceYB.DB().Connect()
		if err != nil {
			utils.ErrExit("connecting to YB as source db for deleting replication slot and publication: %v", err)
		}
		defer sourceYB.DB().Disconnect()
		err = deleteYBReplicationSlotAndPublication(msr.YBReplicationSlotName, msr.YBPublicationName, sourceYB)
		if err != nil {
			utils.ErrExit("deleting yb replication slot and publication: %v", err)
		}
	}

	if msr.YBCDCStreamID == "" {
		log.Info("yugabytedb cdc stream id is not set. skipping deleting stream id")
		return
	}
	deleteCDCStreamIDForEndMigration(tconf)
}

func deleteYBReplicationSlotAndPublication(replicationSlotName string, publicationName string, source srcdb.Source) (err error) {
	ybDB, ok := source.DB().(*srcdb.YugabyteDB)
	if !ok {
		return fmt.Errorf("unable to cast source db to yugabytedb")
	}

	if replicationSlotName != "" && source.DBType == YUGABYTEDB {
		log.Info("deleting yb replication slot: ", replicationSlotName)
		err = ybDB.DropLogicalReplicationSlot(nil, replicationSlotName)
		if err != nil {
			return fmt.Errorf("dropping YB replication slot name: %v", err)
		}
	}

	if publicationName != "" && source.DBType == YUGABYTEDB {
		log.Info("deleting yb publication: ", publicationName)
		err = ybDB.DropPublication(publicationName)
		if err != nil {
			return fmt.Errorf("dropping YB publication name: %v", err)
		}
	}

	return nil
}

func deleteCDCStreamIDForEndMigration(tconf *tgtdb.TargetConf) {
	utils.PrintAndLog("Deleting YugabyteDB CDC stream id\n")
	source := srcdb.Source{
		DBType:         tconf.TargetDBType,
		Host:           tconf.Host,
		Port:           tconf.Port,
		User:           tconf.User,
		Password:       tconf.Password,
		DBName:         tconf.DBName,
		Schema:         tconf.Schema,
		SSLMode:        tconf.SSLMode,
		SSLCertPath:    tconf.SSLCertPath,
		SSLKey:         tconf.SSLKey,
		SSLRootCert:    tconf.SSLRootCert,
		SSLCRL:         tconf.SSLCRL,
		SSLQueryString: tconf.SSLQueryString,
		Uri:            tconf.Uri,
	}
	err := source.DB().Connect()
	if err != nil {
		utils.ErrExit("connecting to YB as source db for deleting stream id: %v", err)
	}
	defer source.DB().Disconnect()

	ybCDCClient := dbzm.NewYugabyteDBCDCClient(exportDir, strings.Join(source.DB().GetServers(), ","),
		source.SSLRootCert, source.DBName, strings.Split(source.TableList, ",")[0], metaDB)
	err = ybCDCClient.Init()
	if err != nil {
		utils.ErrExit("initializing yugabytedb cdc client: %v", err)
	}

	_, err = ybCDCClient.ListMastersNodes()
	if err != nil {
		utils.ErrExit("listing yugabytedb master nodes: %v", err)
	}

	// TODO: check the error once streamID is expirted and ignore it
	err = ybCDCClient.DeleteStreamID()
	if err != nil {
		utils.ErrExit("deleting yugabytedb cdc stream id: %v", err)
	}
}

func cleanupSourceReplicaDB(msr *metadb.MigrationStatusRecord) {
	if !msr.FallForwardEnabled {
		return
	}

	utils.PrintAndLog("cleaning up voyager state from source-replica db...")
	var err error
	sourceReplicaconf := msr.SourceReplicaDBConf
	if sourceReplicaDBPassword == "" {
		sourceReplicaDBPassword, err = askPassword("source-replica DB", sourceReplicaconf.User, "SOURCE_REPLICA_DB_PASSWORD")
		if err != nil {
			utils.ErrExit("getting source-replica db password: %v", err)
		}
	}
	sourceReplicaconf.Password = sourceReplicaDBPassword
	sourceReplicaDB := tgtdb.NewTargetDB(sourceReplicaconf)
	err = sourceReplicaDB.Init()
	if err != nil {
		utils.ErrExit("initializing source-replica db: %v", err)
	}
	defer sourceReplicaDB.Finalize()
	err = sourceReplicaDB.ClearMigrationState(migrationUUID, exportDir)
	if err != nil {
		utils.ErrExit("clearing migration state from source-replica db: %v", err)
	}
}

func cleanupFallBackDB(msr *metadb.MigrationStatusRecord) {
	if !msr.FallbackEnabled {
		return
	}

	utils.PrintAndLog("cleaning up voyager state from source db(used for fall-back)...")
	var err error
	fbconf := msr.SourceDBAsTargetConf
	if sourceDBPassword == "" {
		sourceDBPassword, err = askPassword("source DB", fbconf.User, "SOURCE_DB_PASSWORD")
		if err != nil {
			utils.ErrExit("getting source db password: %v", err)
		}
	}
	fbconf.Password = sourceDBPassword
	fbdb := tgtdb.NewTargetDB(fbconf)
	err = fbdb.Init()
	if err != nil {
		utils.ErrExit("initializing source db: %v", err)
	}
	defer fbdb.Finalize()
	err = fbdb.ClearMigrationState(migrationUUID, exportDir)
	if err != nil {
		utils.ErrExit("clearing migration state from source db: %v", err)
	}
}

func cleanupExportDir() {
	utils.PrintAndLog("cleaning up export dir...")
	subdirs := []string{"schema", "assessment", "data", "logs", "reports", "temp", "metainfo", "sqlldr"}
	for _, subdir := range subdirs {
		err := os.RemoveAll(filepath.Join(exportDir, subdir))
		if err != nil {
			utils.ErrExit("removing directory: %q: %v", subdir, err)
		}
	}
}

func validateEndMigrationFlags(cmd *cobra.Command) error {
	flags := []string{"backup-schema-files", "backup-data-files", "save-migration-reports", "backup-log-files"}
	for _, flag := range flags {
		if cmd.Flag(flag).Value.String() == "true" && !cmd.Flag("backup-dir").Changed {
			return fmt.Errorf("flag %s requires --backup-dir flag to be set", flag)
		}
	}

	if backupDir != "" && !utils.FileOrFolderExists(backupDir) { // ignoring the case where backupDir is not set/required
		return fmt.Errorf("backup-dir %q doesn't exists", backupDir)
	}
	return nil
}

func checkIfEndCommandCanBePerformed(msr *metadb.MigrationStatusRecord) {
	// check if any ongoing voyager command
	matches, err := filepath.Glob(filepath.Join(exportDir, ".*.lck"))
	if err != nil {
		utils.ErrExit("checking for ongoing voyager commands: %v", err)
	}
	if len(matches) > 0 {
		var lockFiles []*lockfile.Lockfile
		for _, match := range matches {
			lockFile := lockfile.NewLockfile(match)
			if lockFile.IsPIDActive() && lockFile.GetCmdName() != "end migration" {
				lockFiles = append(lockFiles, lockFile)
			}
		}
		if len(lockFiles) > 0 {
			cmds := getCommandNamesFromLockFiles(lockFiles)
			msg := fmt.Sprintf("found other ongoing voyager commands: '%s'. Do you want to continue with end migration command by stopping them", strings.Join(cmds, "', '"))
			if utils.AskPrompt(msg) {
				stopVoyagerCommands(msr, lockFiles)
			} else {
				utils.ErrExit("aborting the end migration command")
			}
		}
	} else {
		log.Info("no ongoing voyager commands found")
	}

	if bool(backupSchemaFiles) && !msr.ExportSchemaDone {
		utils.PrintAndLog("backup schema files flag is set but schema export is not done, skipping schema backup...")
		backupSchemaFiles = false
	}

	if backupDataFiles {
		if !areOnDifferentFileSystems(exportDir, backupDir) {
			return
		}

		if !msr.ExportDataDone {
			utils.PrintAndLog("backup data files flag is set but data export is not done, skipping data backup...")
			backupDataFiles = false
			return
		}

		// verify that the size of backup-data dir to be greater the export-dir/data dir
		exportDirDataSize, err := calculateDirSizeWithPattern(exportDir, "data/*.sql")
		if err != nil {
			utils.ErrExit("calculating export dir data size: %v", err)
		}

		backupDirSize, err := getFreeDiskSpace(backupDir)
		if err != nil {
			utils.ErrExit("calculating backup dir size: %v", err)
		}

		if exportDirDataSize >= int64(backupDirSize) {
			utils.ErrExit(`backup directory free space is less than the export directory data size.
			Please provide a backup directory with more free space than the export directory data size(%s).`, humanize.Bytes(uint64(exportDirDataSize)))
		}
	}
}

func getLockFileForCommand(lockFiles []*lockfile.Lockfile, cmdName string) *lockfile.Lockfile {
	result, _ := lo.Find(lockFiles, func(lockFile *lockfile.Lockfile) bool {
		return lockFile.GetCmdName() == cmdName
	})
	return result
}

func getCommandNamesFromLockFiles(lockFiles []*lockfile.Lockfile) []string {
	var cmds []string
	for _, lockFile := range lockFiles {
		cmds = append(cmds, lockFile.GetCmdName())
	}
	return cmds
}

func stopVoyagerCommands(msr *metadb.MigrationStatusRecord, lockFiles []*lockfile.Lockfile) {
	if msr.ArchivingEnabled {
		exportDataLockFile := getLockFileForCommand(lockFiles, "export data")
		exportDataFromTargetLockFile := getLockFileForCommand(lockFiles, "export data from target")
		exportDataFromSourceLockFile := getLockFileForCommand(lockFiles, "export data from source")
		archiveChangesLockFile := getLockFileForCommand(lockFiles, "archive changes")
		stopDataExportCommand(exportDataLockFile)
		stopDataExportCommand(exportDataFromSourceLockFile)
		stopDataExportCommand(exportDataFromTargetLockFile)
		stopVoyagerCommand(archiveChangesLockFile, syscall.SIGUSR1)
	}

	for _, lockFile := range lockFiles {
		stopVoyagerCommand(lockFile, syscall.SIGUSR2)
	}
}

// stop the voyager command by sending the signal to the process(if alive)
func stopVoyagerCommand(lockFile *lockfile.Lockfile, signal syscall.Signal) {
	if lockFile == nil || !lockFile.IsPIDActive() {
		return
	}

	ongoingCmd := lockFile.GetCmdName()
	ongoingCmdPID, err := lockFile.GetCmdPID()
	if err != nil {
		utils.ErrExit("getting PID of ongoing voyager command: %q: %v", ongoingCmd, err)
	}

	fmt.Printf("stopping the ongoing command: %s\n", ongoingCmd)
	log.Infof("stopping the ongoing command: %q with PID=%d", ongoingCmd, ongoingCmdPID)
	err = signalProcess(ongoingCmdPID, signal)
	if err != nil {
		log.Warnf("stopping ongoing voyager command %q with PID=%d: %v", ongoingCmd, ongoingCmdPID, err)
	}
	waitForProcessToExit(ongoingCmdPID, -1)
}

func stopDataExportCommand(lockFile *lockfile.Lockfile) {
	if lockFile == nil || !lockFile.IsPIDActive() {
		return
	}

	metaDB.UpdateMigrationStatusRecord(func(record *metadb.MigrationStatusRecord) {
		// dbzm plugin detects this MSR flag, and stops the data export gracefully
		// so that the ongoing segment in closed and can be processed -> archived -> deleted
		record.EndMigrationRequested = true
	})

	ongoingCmd := lockFile.GetCmdName()
	ongoingCmdPID, err := lockFile.GetCmdPID()
	if err != nil {
		utils.ErrExit("getting PID of ongoing voyager command: %q: %v", ongoingCmd, err)
	}

	fmt.Printf("stopping the ongoing command: %s\n", ongoingCmd)
	log.Infof("stopping the ongoing command: %q with PID=%d", ongoingCmd, ongoingCmdPID)

	waitForProcessToExit(ongoingCmdPID, -1)
}

// NOTE: function is for Linux only (Windows won't work)
// TODO: verify with dockerized voyager
func areOnDifferentFileSystems(path1 string, path2 string) bool {
	stat1 := syscall.Stat_t{}
	stat2 := syscall.Stat_t{}

	err1 := syscall.Stat(path1, &stat1)
	err2 := syscall.Stat(path2, &stat2)

	if err1 != nil || err2 != nil {
		utils.ErrExit("getting file system info: for %s and %s: %v, %v", path1, path2, err1, err2)
	}

	return stat1.Dev != stat2.Dev
}

func calculateDirSizeWithPattern(dirPath string, filePattern string) (int64, error) {
	var size int64

	fileMatches, err := filepath.Glob(filepath.Join(dirPath, filePattern))
	if err != nil {
		return 0, fmt.Errorf("matching the file pattern %q in the directory %q: %w", filePattern, dirPath, err)
	}

	for _, filePath := range fileMatches {
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return 0, fmt.Errorf("getting file info for %q: %w", filePath, err)
		}

		if !fileInfo.IsDir() {
			size += fileInfo.Size()
		}
	}

	return size, nil
}

func getFreeDiskSpace(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	// calculate the free space in bytes
	freeSpace := stat.Bfree * uint64(stat.Bsize)
	return freeSpace, nil
}

func init() {
	endCmd.AddCommand(endMigrationCmd)

	BoolVar(endMigrationCmd.Flags(), &backupSchemaFiles, "backup-schema-files", false, "backup migration schema files")
	BoolVar(endMigrationCmd.Flags(), &backupDataFiles, "backup-data-files", false, "backup snapshot data files")
	BoolVar(endMigrationCmd.Flags(), &saveMigrationReports, "save-migration-reports", false, "save migration assessment report, analyze schema report and data migration reports")
	BoolVar(endMigrationCmd.Flags(), &backupLogFiles, "backup-log-files", false, "backup yb-voyager log files for this migration")
	endMigrationCmd.Flags().StringVar(&backupDir, "backup-dir", "", "backup directory is where all the backup files of schema, data, logs and reports will be saved")

	registerCommonGlobalFlags(endMigrationCmd)
	endMigrationCmd.Flags().MarkHidden("send-diagnostics")

	endMigrationCmd.MarkFlagRequired("backup-schema-files")
	endMigrationCmd.MarkFlagRequired("backup-data-files")
	endMigrationCmd.MarkFlagRequired("save-migration-reports")
	endMigrationCmd.MarkFlagRequired("backup-log-files")
	endMigrationCmd.MarkFlagRequired("export-dir")
}
