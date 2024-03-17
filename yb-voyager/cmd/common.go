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
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/fatih/color"
	_ "github.com/godror/godror"
	"github.com/google/uuid"
	"github.com/gosuri/uitable"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mitchellh/go-ps"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"
	"golang.org/x/term"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/cp"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/datafile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/dbzm"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/metadb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/srcdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/jsonfile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

var (
	metaDB               *metadb.MetaDB
	PARENT_COMMAND_USAGE = "Parent command. Refer to the sub-commands for usage help."
)

func updateFilePaths(source *srcdb.Source, exportDir string, tablesProgressMetadata map[string]*utils.TableProgressMetadata) {
	var requiredMap map[string]string

	// TODO: handle the case if table name has double quotes/case sensitive

	sortedKeys := utils.GetSortedKeys(tablesProgressMetadata)
	if source.DBType == "postgresql" {
		requiredMap = getMappingForTableNameVsTableFileName(filepath.Join(exportDir, "data"), false)
		for _, key := range sortedKeys {
			tableName := tablesProgressMetadata[key].TableName
			fullTableName := tableName.Qualified.MinQuoted

			if _, ok := requiredMap[fullTableName]; ok { // checking if toc/dump has data file for table
				tablesProgressMetadata[key].InProgressFilePath = filepath.Join(exportDir, "data", requiredMap[fullTableName])
				if tablesProgressMetadata[key].TableName.SchemaName.Unquoted == "public" {
					tablesProgressMetadata[key].FinalFilePath = filepath.Join(exportDir, "data", tableName.ObjectName.MinQuoted+"_data.sql")
				} else {
					tablesProgressMetadata[key].FinalFilePath = filepath.Join(exportDir, "data", fullTableName+"_data.sql")
				}
			} else {
				log.Infof("deleting an entry %q from tablesProgressMetadata: ", key)
				delete(tablesProgressMetadata, key)
			}
		}
	} else if source.DBType == "oracle" || source.DBType == "mysql" {
		for _, key := range sortedKeys {
			targetTableName := tablesProgressMetadata[key].TableName.ObjectName.Unquoted
			// required if PREFIX_PARTITION is set in ora2pg.conf file
			if tablesProgressMetadata[key].IsPartition {
				targetTableName = tablesProgressMetadata[key].ParentTable + "_" + targetTableName
			}
			tablesProgressMetadata[key].InProgressFilePath = filepath.Join(exportDir, "data", "tmp_"+targetTableName+"_data.sql")
			tablesProgressMetadata[key].FinalFilePath = filepath.Join(exportDir, "data", targetTableName+"_data.sql")
		}
	}

	logMsg := "After updating data file paths, TablesProgressMetadata:"
	for _, key := range sortedKeys {
		logMsg += fmt.Sprintf("%+v\n", tablesProgressMetadata[key])
	}
	log.Infof(logMsg)
}

func getMappingForTableNameVsTableFileName(dataDirPath string, noWait bool) map[string]string {
	tocTextFilePath := filepath.Join(dataDirPath, "toc.txt")
	if noWait && !utils.FileOrFolderExists(tocTextFilePath) { // to avoid infine wait for export data status command
		return nil
	}
	for !utils.FileOrFolderExists(tocTextFilePath) {
		time.Sleep(time.Second * 1)
	}

	pgRestorePath, err := srcdb.GetAbsPathOfPGCommand("pg_restore")
	if err != nil {
		utils.ErrExit("could not get absolute path of pg_restore command: %s", err)
	}
	pgRestoreCmd := exec.Command(pgRestorePath, "-l", dataDirPath)
	stdOut, err := pgRestoreCmd.Output()
	log.Infof("cmd: %s", pgRestoreCmd.String())
	log.Infof("output: %s", string(stdOut))
	if err != nil {
		utils.ErrExit("ERROR: couldn't parse the TOC file to collect the tablenames for data files: %v", err)
	}

	tableNameVsFileNameMap := make(map[string]string)
	var sequencesPostData strings.Builder

	lines := strings.Split(string(stdOut), "\n")
	for _, line := range lines {
		// example of line: 3725; 0 16594 TABLE DATA public categories ds2
		parts := strings.Split(line, " ")

		if len(parts) < 8 { // those lines don't contain table/sequences related info
			continue
		} else if parts[3] == "TABLE" && parts[4] == "DATA" {
			fileName := strings.Trim(parts[0], ";") + ".dat"
			schemaName := parts[5]
			tableName := parts[6]
			if nameContainsCapitalLetter(tableName) || sqlname.IsReservedKeywordPG(tableName) {
				// Surround the table name with double quotes.
				tableName = fmt.Sprintf("\"%s\"", tableName)
			}
			fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)
			tableNameVsFileNameMap[fullTableName] = fileName
		}
	}

	tocTextFileDataBytes, err := os.ReadFile(tocTextFilePath)
	if err != nil {
		utils.ErrExit("Failed to read file %q: %v", tocTextFilePath, err)
	}

	tocTextFileData := strings.Split(string(tocTextFileDataBytes), "\n")
	numLines := len(tocTextFileData)
	setvalRegex := regexp.MustCompile("(?i)SELECT.*setval")

	for i := 0; i < numLines; i++ {
		if setvalRegex.MatchString(tocTextFileData[i]) {
			sequencesPostData.WriteString(tocTextFileData[i])
			sequencesPostData.WriteString("\n")
		}
	}

	//extracted SQL for setval() and put it into a postexport.sql file
	os.WriteFile(filepath.Join(dataDirPath, "postdata.sql"), []byte(sequencesPostData.String()), 0644)
	return tableNameVsFileNameMap
}

func UpdateTableApproxRowCount(source *srcdb.Source, exportDir string, tablesProgressMetadata map[string]*utils.TableProgressMetadata) {
	utils.PrintAndLog("calculating approx num of rows to export for each table...")
	sortedKeys := utils.GetSortedKeys(tablesProgressMetadata)
	for _, key := range sortedKeys {
		approxRowCount := source.DB().GetTableApproxRowCount(tablesProgressMetadata[key].TableName)
		tablesProgressMetadata[key].CountTotalRows = approxRowCount
	}

	log.Tracef("After updating total approx row count, TablesProgressMetadata: %+v", tablesProgressMetadata)
}

func GetTableRowCount(filePath string) map[string]int64 {
	tableRowCountMap := make(map[string]int64)

	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		utils.ErrExit("read file %q: %s", filePath, err)
	}

	lines := strings.Split(strings.Trim(string(fileBytes), "\n"), "\n")

	for _, line := range lines {
		tableName := strings.Split(line, ",")[0]
		rowCount := strings.Split(line, ",")[1]
		rowCountInt64, _ := strconv.ParseInt(rowCount, 10, 64)

		tableRowCountMap[tableName] = rowCountInt64
	}

	log.Infof("tableRowCountMap: %v", tableRowCountMap)
	return tableRowCountMap
}

func getExportedRowCountSnapshot(exportDir string) map[string]int64 {
	tableRowCount := map[string]int64{}
	for _, fileEntry := range datafile.OpenDescriptor(exportDir).DataFileList {
		tableRowCount[fileEntry.TableName] += fileEntry.RowCount
	}
	return tableRowCount
}

func getLeafPartitionsFromRootTable(tables []string) map[string][]string {
	leafPartitions := make(map[string][]string)
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("get migration status record: %v", err)
	}
	if !msr.IsExportTableListSet || msr.SourceDBConf.DBType != POSTGRESQL {
		return leafPartitions
	}
	for leaf, root := range msr.SourceRenameTablesMap {
		leafTable := sqlname.NewSourceNameFromQualifiedName(leaf)
		rootTable := sqlname.NewSourceNameFromQualifiedName(root)
		leaf = leafTable.Qualified.MinQuoted
		if leafTable.SchemaName.MinQuoted == "public" {
			leaf = leafTable.ObjectName.MinQuoted
		}
		root = rootTable.Qualified.MinQuoted
		if rootTable.SchemaName.MinQuoted == "public" {
			root = rootTable.ObjectName.MinQuoted
		}
		if !lo.Contains(tables, root) {
			continue
		}
		leafPartitions[root] = append(leafPartitions[root], leaf)
	}

	return leafPartitions
}

func displayExportedRowCountSnapshot(snapshotViaDebezium bool) {
	fmt.Printf("snapshot export report\n")
	uitable := uitable.New()

	if !snapshotViaDebezium {
		exportedRowCount := getExportedRowCountSnapshot(exportDir)
		if source.Schema != "" {
			addHeader(uitable, "SCHEMA", "TABLE", "ROW COUNT")
		} else {
			addHeader(uitable, "DATABASE", "TABLE", "ROW COUNT")
		}
		keys := lo.Keys(exportedRowCount)
		sort.Strings(keys)
		leafPartitions := getLeafPartitionsFromRootTable(keys)
		for _, key := range keys {
			if source.Schema != "" {
				tableParts := strings.Split(key, ".")
				table := tableParts[0]
				schema, _ := getDefaultSourceSchemaName() // err can be ignored as these table names will be qualified for non-public schema
				if len(tableParts) > 1 {
					schema = tableParts[0]
					table = tableParts[1]
				}
				displayTableName := table
				if source.DBType == POSTGRESQL && leafPartitions[key] != nil {
					partitions := strings.Join(leafPartitions[key], ", ")
					displayTableName = fmt.Sprintf("%s (%s)", table, partitions)
				}
				uitable.AddRow(schema, displayTableName, exportedRowCount[key])
			} else {
				uitable.AddRow(source.DBName, key, exportedRowCount[key])
			}
		}
		if len(keys) > 0 {
			fmt.Print("\n")
			fmt.Println(uitable)
			fmt.Print("\n")
		}
		return
	}

	exportStatus, err := dbzm.ReadExportStatus(filepath.Join(exportDir, "data", "export_status.json"))
	if err != nil {
		utils.ErrExit("failed to read export status during data export snapshot-and-changes report display: %v", err)
	}
	//TODO: report table with case-sensitiveness
	for i, tableStatus := range exportStatus.Tables {
		if i == 0 {
			if tableStatus.SchemaName != "" {
				addHeader(uitable, "SCHEMA", "TABLE", "ROW COUNT")
			} else {
				addHeader(uitable, "DATABASE", "TABLE", "ROW COUNT")
			}
		}
		if tableStatus.SchemaName != "" {
			uitable.AddRow(tableStatus.SchemaName, tableStatus.TableName, tableStatus.ExportedRowCountSnapshot)
		} else {
			uitable.AddRow(tableStatus.DatabaseName, tableStatus.TableName, tableStatus.ExportedRowCountSnapshot)
		}
	}
	fmt.Print("\n")
	fmt.Println(uitable)
	fmt.Print("\n")
}

func renameDatafileDescriptor(exportDir string) {
	datafileDescriptor := datafile.OpenDescriptor(exportDir)
	for _, fileEntry := range datafileDescriptor.DataFileList {
		renamedTable, isRenamed := renameTableIfRequired(fileEntry.TableName)
		if isRenamed {
			fileEntry.TableName = renamedTable
		}
	}
	datafileDescriptor.Save()
}

func renameExportSnapshotStatus(exportSnapshotStatusFile *jsonfile.JsonFile[ExportSnapshotStatus]) error {
	err := exportSnapshotStatusFile.Update(func(exportSnapshotStatus *ExportSnapshotStatus) {
		for i, tableStatus := range exportSnapshotStatus.Tables {
			renamedTable, isRenamed := renameTableIfRequired(tableStatus.TableName)
			if isRenamed {
				if len(strings.Split(renamedTable, ".")) == 1 {
					renamedTable = fmt.Sprintf("public.%s", renamedTable)
				}
				exportSnapshotStatus.Tables[i].TableName = renamedTable
			}
		}
	})
	if err != nil {
		return fmt.Errorf("update export snapshot status: %w", err)
	}
	return nil
}

func displayImportedRowCountSnapshot(state *ImportDataState, tasks []*ImportFileTask) {
	if importerRole == IMPORT_FILE_ROLE {
		fmt.Printf("import report\n")
	} else {
		fmt.Printf("snapshot import report\n")
	}
	tableList := importFileTasksToTableNames(tasks)
	err := retrieveMigrationUUID()
	if err != nil {
		utils.ErrExit("could not retrieve migration UUID: %w", err)
	}
	uitable := uitable.New()

	dbType := "target"
	if importerRole == SOURCE_REPLICA_DB_IMPORTER_ROLE {
		dbType = "source-replica"
	}

	snapshotRowCount := make(map[string]int64)

	if importerRole == IMPORT_FILE_ROLE {
		for _, tableName := range tableList {
			tableRowCount, err := state.GetImportedSnapshotRowCountForTable(tableName)
			if err != nil {
				utils.ErrExit("could not fetch snapshot row count for table %q: %w", tableName, err)
			}
			snapshotRowCount[tableName] = tableRowCount
		}
	} else {
		snapshotRowCount, err = getImportedSnapshotRowsMap(dbType, tableList)
		if err != nil {
			utils.ErrExit("failed to get imported snapshot rows map: %v", err)
		}
	}

	for i, tableName := range tableList {
		if i == 0 {
			addHeader(uitable, "SCHEMA", "TABLE", "IMPORTED ROW COUNT")
		}
		table := tableName
		if len(strings.Split(tableName, ".")) == 2 {
			table = strings.Split(tableName, ".")[1]
		}
		uitable.AddRow(getTargetSchemaName(tableName), table, snapshotRowCount[tableName])
	}
	if len(tableList) > 0 {
		fmt.Printf("\n")
		fmt.Println(uitable)
		fmt.Printf("\n")
	}
}

// setup a project having subdirs for various database objects IF NOT EXISTS
func CreateMigrationProjectIfNotExists(dbType string, exportDir string) {
	// TODO: add a check/prompt if any directories apart from required ones are present in export-dir
	var projectSubdirs = []string{
		"schema", "data", "reports",
		"metainfo", "metainfo/data", "metainfo/conf", "metainfo/ssl",
		"temp", "temp/ora2pg_temp_dir",
	}

	log.Info("Creating a project directory if not exists...")
	//Assuming export directory as a project directory
	projectDirPath := exportDir

	for _, subdir := range projectSubdirs {
		err := exec.Command("mkdir", "-p", filepath.Join(projectDirPath, subdir)).Run()
		if err != nil {
			utils.ErrExit("couldn't create sub-directories under %q: %v", projectDirPath, err)
		}
	}

	// creating subdirs under schema dir
	for _, schemaObjectType := range source.ExportObjectTypeList {
		if schemaObjectType == "INDEX" { //no separate dir for indexes
			continue
		}
		databaseObjectDirName := strings.ToLower(schemaObjectType) + "s"

		err := exec.Command("mkdir", "-p", filepath.Join(projectDirPath, "schema", databaseObjectDirName)).Run()
		if err != nil {
			utils.ErrExit("couldn't create sub-directories under %q: %v", filepath.Join(projectDirPath, "schema"), err)
		}
	}

	initMetaDB()
}

func initMetaDB() {
	err := metadb.CreateAndInitMetaDBIfRequired(exportDir)
	if err != nil {
		utils.ErrExit("could not create and init meta db: %w", err)
	}
	metaDB, err = metadb.NewMetaDB(exportDir)
	if err != nil {
		utils.ErrExit("failed to initialize meta db: %s", err)
	}
	err = metaDB.InitMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("could not init migration status record: %w", err)
	}
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("get migration status record: %v", err)
	}
	if msr.VoyagerVersion != utils.YB_VOYAGER_VERSION {
		userFacingMsg := fmt.Sprintf("Voyager requires the entire migration workflow to be executed using a single Voyager version.\n"+
			"The export-dir %q was created using version %q and the current version is %q. Either use Voyager %q to continue the migration or start afresh "+
			"with a new export-dir.", exportDir, msr.VoyagerVersion, utils.YB_VOYAGER_VERSION, msr.VoyagerVersion)
		if msr.VoyagerVersion == "" { //In case the export dir is already started from older version that will not have VoyagerVersion field in MSR
			userFacingMsg = fmt.Sprintf("Voyager requires the entire migration workflow to be executed using a single Voyager version.\n"+
				"The export-dir %q was created using older version and the current version is %q. Either use older version to continue the migration or start afresh "+
				"with a new export-dir.", exportDir, utils.YB_VOYAGER_VERSION)
		}
		utils.ErrExit(userFacingMsg)
	}
}

// sets the global variable migrationUUID after retrieving it from MigrationStatusRecord
func retrieveMigrationUUID() error {
	if migrationUUID != uuid.Nil {
		return nil
	}
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		return fmt.Errorf("retrieving migration status record: %w", err)
	}
	if msr == nil {
		return fmt.Errorf("migration status record not found")
	}

	migrationUUID = uuid.MustParse(msr.MigrationUUID)
	utils.PrintAndLog("migrationID: %s", migrationUUID)
	return nil
}

func nameContainsCapitalLetter(name string) bool {
	for _, c := range name {
		if unicode.IsUpper(c) {
			return true
		}
	}
	return false
}

func getCutoverStatus() string {
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("get migration status record: %v", err)
	}

	a := msr.CutoverToTargetRequested
	b := msr.CutoverProcessedBySourceExporter
	c := msr.CutoverProcessedByTargetImporter

	if !a {
		return NOT_INITIATED
	}
	if msr.FallForwardEnabled && a && b && c && msr.ExportFromTargetFallForwardStarted {
		return COMPLETED
	} else if msr.FallbackEnabled && a && b && c && msr.ExportFromTargetFallBackStarted {
		return COMPLETED
	} else if !msr.FallForwardEnabled && !msr.FallbackEnabled && a && b && c {
		return COMPLETED
	}
	return INITIATED
}

func checkStreamingMode() (bool, error) {
	var err error
	migrationStatus, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		return false, fmt.Errorf("error while fetching migration status record: %w", err)
	}
	streamChanges := changeStreamingIsEnabled(migrationStatus.ExportType)
	return streamChanges, nil
}

func getCutoverToSourceReplicaStatus() string {
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("get migration status record: %v", err)
	}
	a := msr.CutoverToSourceReplicaRequested
	b := msr.CutoverToSourceReplicaProcessedByTargetExporter
	c := msr.CutoverToSourceReplicaProcessedBySRImporter

	if !a {
		return NOT_INITIATED
	} else if a && b && c {
		return COMPLETED
	}
	return INITIATED
}

func getCutoverToSourceStatus() string {
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("get migration status record: %v", err)
	}
	a := msr.CutoverToSourceRequested
	b := msr.CutoverToSourceProcessedByTargetExporter
	c := msr.CutoverToSourceProcessedBySourceImporter

	if !a {
		return NOT_INITIATED
	} else if a && b && c {
		return COMPLETED
	}
	return INITIATED
}

func getPassword(cmd *cobra.Command, cliArgName, envVarName string) (string, error) {
	if cmd.Flags().Changed(cliArgName) {
		return cmd.Flag(cliArgName).Value.String(), nil
	}
	if os.Getenv(envVarName) != "" {
		return os.Getenv(envVarName), nil
	}
	fmt.Printf("Password to connect to %s (In addition, you can also set the password using the environment variable `%s`): ",
		strings.TrimSuffix(cliArgName, "-password"), envVarName)
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", fmt.Errorf("read password: %w", err)
	}
	fmt.Print("\n")
	return string(bytePassword), nil
}

func addHeader(table *uitable.Table, cols ...string) {
	headerfmt := color.New(color.FgGreen, color.Underline).SprintFunc()
	columns := lo.Map(cols, func(col string, _ int) interface{} {
		return headerfmt(col)
	})
	table.AddRow(columns...)
}

func GetSourceDBTypeFromMSR() string {
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("get migration status record: %v", err)
	}
	if msr == nil {
		utils.ErrExit("migration status record not found")
	}
	return msr.SourceDBConf.DBType
}

func validateMetaDBCreated() {
	if !metaDBIsCreated(exportDir) {
		utils.ErrExit("ERROR: no metadb found in export-dir")
	}
}

func getImportTableList(sourceTableList []string) []string {
	if importerRole == IMPORT_FILE_ROLE {
		return nil
	}
	var tableList []string
	sqlname.SourceDBType = source.DBType
	for _, qualifiedTableName := range sourceTableList {
		// TODO: handle case sensitivity?
		tableName := sqlname.NewSourceNameFromQualifiedName(qualifiedTableName)
		table := tableName.ObjectName.MinQuoted
		if source.DBType == POSTGRESQL && tableName.SchemaName.MinQuoted != "public" {
			table = tableName.Qualified.MinQuoted
		}
		tableList = append(tableList, table)
	}
	return tableList
}

func hideImportFlagsInFallForwardOrBackCmds(cmd *cobra.Command) {
	var flags = []string{"target-db-type", "import-type", "target-endpoints", "use-public-ip", "continue-on-error", "table-list",
		"table-list-file-path", "exclude-table-list", "exclude-table-list-file-path", "enable-upsert"}
	for _, flagName := range flags {
		flag := cmd.Flags().Lookup(flagName)
		if flag != nil {
			flag.Hidden = true
		}
	}
}

func hideExportFlagsInFallForwardOrBackCmds(cmd *cobra.Command) {
	var flags = []string{"source-db-type", "export-type", "parallel-jobs", "start-clean"}
	for _, flagName := range flags {
		flag := cmd.Flags().Lookup(flagName)
		if flag != nil {
			flag.Hidden = true
		}
	}
}

func GetDefaultPGSchema(schema string, separator string) (string, bool) {
	// second return value is true if public is not included in the schema
	// which indicates that the no default schema
	schemas := strings.Split(schema, separator)
	if len(schemas) == 1 {
		return schema, false
	} else if slices.Contains(schemas, "public") {
		return "public", false
	} else {
		return "", true
	}
}

func CleanupChildProcesses() {
	myPid := os.Getpid()
	processes, err := ps.Processes()
	if err != nil {
		log.Errorf("failed to read processes: %v", err)
	}
	childProcesses := lo.Filter(processes, func(process ps.Process, _ int) bool {
		return process.PPid() == myPid
	})
	if len(childProcesses) > 0 {
		log.Info("Cleaning up child processes...")
	}
	for _, process := range childProcesses {
		pid := process.Pid()
		log.Infof("shutting down child pid %d", pid)
		err := ShutdownProcess(pid, 60)
		if err != nil {
			log.Errorf("shut down child pid %d : %v", pid, err)
		} else {
			log.Infof("shut down child pid %d", pid)
		}
	}
}

// this function wait for process to exit after signalling it to stop
func ShutdownProcess(pid int, forceShutdownAfterSeconds int) error {
	err := signalProcess(pid, syscall.SIGTERM)
	if err != nil {
		return fmt.Errorf("send sigterm to %d: %v", pid, err)
	}
	waitForProcessToExit(pid, forceShutdownAfterSeconds)
	return nil
}

func signalProcess(pid int, signal syscall.Signal) error {
	process, _ := os.FindProcess(pid) // Always succeeds on Unix systems
	log.Infof("sending signal=%s to process with PID=%d", signal.String(), pid)
	err := process.Signal(signal)
	if err != nil {
		return fmt.Errorf("sending signal=%s signal to process with PID=%d: %w", signal.String(), pid, err)
	}

	return nil
}

func waitForProcessToExit(pid int, forceShutdownAfterSeconds int) {
	// Reference: https://mezhenskyi.dev/posts/go-linux-processes/
	// Poll every 2 sec to make sure process is stopped
	// here process.Signal(syscall.Signal(0)) will return error only if process is not running
	process, _ := os.FindProcess(pid) // Always succeeds on Unix systems
	secondsWaited := 0
	for {
		time.Sleep(time.Second * 2)
		secondsWaited += 2
		err := process.Signal(syscall.Signal(0))
		if err != nil {
			log.Infof("process with PID=%d is stopped", process.Pid)
			return
		}
		if forceShutdownAfterSeconds > 0 && secondsWaited > forceShutdownAfterSeconds {
			log.Infof("force shutting down pid %d", pid)
			process.Signal(syscall.SIGKILL)
		}
	}
}

func initBaseSourceEvent(bev *cp.BaseEvent, eventType string) {
	*bev = cp.BaseEvent{
		EventType:     eventType,
		MigrationUUID: migrationUUID,
		DBType:        source.DBType,
		DatabaseName:  source.DBName,
		SchemaNames:   cp.GetSchemaList(source.Schema),
	}
}

func initBaseTargetEvent(bev *cp.BaseEvent, eventType string) {
	*bev = cp.BaseEvent{
		EventType:     eventType,
		MigrationUUID: migrationUUID,
		DBType:        tconf.TargetDBType,
		DatabaseName:  tconf.DBName,
		SchemaNames:   []string{tconf.Schema},
	}
}

func renameTableIfRequired(table string) (string, bool) {
	// required to rename the table name from leaf to root partition in case of pg_dump
	// to be load data in target using via root table
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("Failed to get migration status record: %s", err)
	}
	sourceDBType = msr.SourceDBConf.DBType
	sourceDBTypeInMigration := msr.SourceDBConf.DBType
	schema := msr.SourceDBConf.Schema
	sqlname.SourceDBType = source.DBType
	if source.DBType != POSTGRESQL && source.DBType != YUGABYTEDB {
		return table, false
	}
	if source.DBType == POSTGRESQL && msr.SourceRenameTablesMap == nil ||
		source.DBType == YUGABYTEDB && msr.TargetRenameTablesMap == nil {
		return table, false
	}
	if sourceDBTypeInMigration != POSTGRESQL && source.DBType == YUGABYTEDB {
		schema = source.Schema
	}
	renameTablesMap := msr.SourceRenameTablesMap
	if source.DBType == YUGABYTEDB {
		renameTablesMap = msr.TargetRenameTablesMap
	}
	defaultSchema, noDefaultSchema := GetDefaultPGSchema(schema, "|")
	if noDefaultSchema && len(strings.Split(table, ".")) <= 1 {
		utils.ErrExit("no default schema found to qualify table %s", table)
	}
	tableName := sqlname.NewSourceNameFromMaybeQualifiedName(table, defaultSchema)
	fromTable := tableName.Qualified.Unquoted

	if renameTablesMap[fromTable] != "" {
		table := sqlname.NewSourceNameFromQualifiedName(renameTablesMap[fromTable])
		toTable := table.Qualified.MinQuoted
		if table.SchemaName.MinQuoted == "public" {
			toTable = table.ObjectName.MinQuoted
		}
		return toTable, true
	}
	return table, false
}

func getExportedSnapshotRowsMap(tableList []string, exportSnapshotStatus *ExportSnapshotStatus) (map[string]int64, map[string][]string, error) {
	snapshotRowsMap := make(map[string]int64)
	snapshotStatusMap := make(map[string][]string)
	for _, table := range tableList {
		tableStatus := exportSnapshotStatus.GetTableStatusByTableName(table)
		table = strings.TrimPrefix(table, "public.") //safely can remove it for now. TODO: fix with NameRegistry all such occurrences
		for _, status := range tableStatus {
			if status.FileName == "" {
				//in case of root table as well in the tablelist during export an entry with empty file name is there
				continue
			}
			snapshotRowsMap[table] += status.ExportedRowCountSnapshot
			snapshotStatusMap[table] = append(snapshotStatusMap[table], status.Status)
		}
	}
	return snapshotRowsMap, snapshotStatusMap, nil
}

func getImportedSnapshotRowsMap(dbType string, tableList []string) (map[string]int64, error) {
	switch dbType {
	case "target":
		importerRole = TARGET_DB_IMPORTER_ROLE
	case "source-replica":
		importerRole = SOURCE_REPLICA_DB_IMPORTER_ROLE
	}
	state := NewImportDataState(exportDir)
	var snapshotDataFileDescriptor *datafile.Descriptor

	dataFileDescriptorPath := filepath.Join(exportDir, datafile.DESCRIPTOR_PATH)
	if utils.FileOrFolderExists(dataFileDescriptorPath) {
		snapshotDataFileDescriptor = datafile.OpenDescriptor(exportDir)
	}

	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		return nil, fmt.Errorf("get migration status record: %w", err)
	}
	sourceSchemaCount := len(strings.Split(msr.SourceDBConf.Schema, "|"))

	snapshotRowsMap := make(map[string]int64)
	for _, table := range tableList {
		parts := strings.Split(table, ".")
		schemaName := ""
		tableName := parts[0]
		if len(parts) > 1 {
			schemaName = parts[0]
			tableName = parts[1]
		}
		if sourceSchemaCount <= 1 && source.DBType != POSTGRESQL { //this check is for Oracle case
			schemaName = ""
		}
		if schemaName == "public" || schemaName == "" {
			table = tableName
		}
		//Now multiple files can be there for a table in case of partitions
		dataFiles := snapshotDataFileDescriptor.GetDataFileEntriesByTableName(table)
		if len(dataFiles) == 0 {
			dataFile := &datafile.FileEntry{
				FilePath:  "",
				TableName: table,
				RowCount:  0,
				FileSize:  0,
			}
			dataFiles = append(dataFiles, dataFile)
		}
		for _, dataFile := range dataFiles {
			snapshotRowCount, err := state.GetImportedRowCount(dataFile.FilePath, dataFile.TableName)
			if err != nil {
				return nil, fmt.Errorf("could not fetch snapshot row count for table %q: %w", table, err)
			}
			snapshotRowsMap[table] += snapshotRowCount
		}
	}
	return snapshotRowsMap, nil
}

func storeTableListInMSR(tableList []*sqlname.SourceName) error {
	minQuotedTableList := lo.Uniq(lo.Map(tableList, func(table *sqlname.SourceName, _ int) string {
		// Store list of tables in MSR with root table in case of partitions
		renamedTable, isRenamed := renameTableIfRequired(table.Qualified.MinQuoted)
		if isRenamed {
			if len(strings.Split(renamedTable, ".")) == 1 {
				renamedTable = fmt.Sprintf("public.%s", renamedTable)
			}
		}
		return renamedTable
	}))
	err := metaDB.UpdateMigrationStatusRecord(func(record *metadb.MigrationStatusRecord) {
		record.TableListExportedFromSource = minQuotedTableList
	})
	if err != nil {
		return fmt.Errorf("update migration status record: %v", err)
	}
	return nil
}
