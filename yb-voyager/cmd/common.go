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
	"errors"
	"fmt"
	"io/fs"
	"math"
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

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/cp"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/datafile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/dbzm"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/metadb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/migassessment"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/namereg"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/srcdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/jsonfile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

var (
	metaDB               *metadb.MetaDB
	PARENT_COMMAND_USAGE = "Parent command. Refer to the sub-commands for usage help."
	startTime            time.Time
)

func PrintElapsedDuration() {
	uninitialisedTimestamp := time.Time{}
	if startTime == uninitialisedTimestamp {
		return
	}
	log.Infof("End time: %s\n", time.Now())
	timeTakenByCurrentVoyagerInvocation := time.Since(startTime)
	log.Infof("Time taken: %s (%.2f seconds)\n",
		timeTakenByCurrentVoyagerInvocation,
		timeTakenByCurrentVoyagerInvocation.Seconds())
}

func updateFilePaths(source *srcdb.Source, exportDir string, tablesProgressMetadata map[string]*utils.TableProgressMetadata) {
	var requiredMap map[string]string

	// TODO: handle the case if table name has double quotes/case sensitive

	sortedKeys := utils.GetSortedKeys(tablesProgressMetadata)
	if source.DBType == "postgresql" {
		requiredMap = getMappingForTableNameVsTableFileName(filepath.Join(exportDir, "data"), false)
		for _, key := range sortedKeys {
			tableName := tablesProgressMetadata[key].TableName
			fullTableName := tableName.ForKey()
			table := tableName.ForMinOutput()
			if _, ok := requiredMap[fullTableName]; ok { // checking if toc/dump has data file for table
				tablesProgressMetadata[key].InProgressFilePath = filepath.Join(exportDir, "data", requiredMap[fullTableName])
				tablesProgressMetadata[key].FinalFilePath = filepath.Join(exportDir, "data", table+"_data.sql")
			} else {
				log.Infof("deleting an entry %q from tablesProgressMetadata: ", key)
				delete(tablesProgressMetadata, key)
			}
		}
	} else if source.DBType == "oracle" || source.DBType == "mysql" {
		for _, key := range sortedKeys {
			_, tname := tablesProgressMetadata[key].TableName.ForCatalogQuery()
			targetTableName := tname
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
			table, err := namereg.NameReg.LookupTableName(fullTableName)
			if err != nil {
				utils.ErrExit("lookup table %s in name registry : %v", fullTableName, err)
			}
			tableNameVsFileNameMap[table.ForKey()] = fileName
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

func getLeafPartitionsFromRootTable() map[string][]string {
	leafPartitions := make(map[string][]string)
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		utils.ErrExit("get migration status record: %v", err)
	}
	if !msr.IsExportTableListSet || msr.SourceDBConf.DBType != POSTGRESQL {
		return leafPartitions
	}
	tables := msr.TableListExportedFromSource
	for leaf, root := range msr.SourceRenameTablesMap {
		leafTable := sqlname.NewSourceNameFromQualifiedName(getQuotedFromUnquoted(leaf))
		rootTable := sqlname.NewSourceNameFromQualifiedName(getQuotedFromUnquoted(root))
		leaf = leafTable.Qualified.MinQuoted
		if leafTable.SchemaName.MinQuoted == "public" {
			leaf = leafTable.ObjectName.MinQuoted
		}
		root = rootTable.Qualified.MinQuoted
		if !lo.Contains(tables, root) {
			continue
		}
		leafPartitions[root] = append(leafPartitions[root], leaf)
	}

	return leafPartitions
}

func getQuotedFromUnquoted(t string) string {
	//To preserve case sensitiveness in the Unquoted
	parts := strings.Split(t, ".")
	s, t := parts[0], parts[1]
	return fmt.Sprintf(`%s."%s"`, s, t)
}

func displayExportedRowCountSnapshot(snapshotViaDebezium bool) {
	fmt.Printf("snapshot export report\n")
	uitable := uitable.New()

	leafPartitions := getLeafPartitionsFromRootTable()
	if !snapshotViaDebezium {
		exportedRowCount := getExportedRowCountSnapshot(exportDir)
		if source.Schema != "" {
			addHeader(uitable, "SCHEMA", "TABLE", "ROW COUNT")
		} else {
			addHeader(uitable, "DATABASE", "TABLE", "ROW COUNT")
		}
		keys := lo.Keys(exportedRowCount)
		sort.Strings(keys)
		for _, key := range keys {
			table, err := namereg.NameReg.LookupTableName(key)
			if err != nil {
				utils.ErrExit("lookup table %s in name registry : %v", key, err)
			}
			displayTableName := table.CurrentName.Unqualified.MinQuoted
			partitions := leafPartitions[table.ForOutput()]
			if source.DBType == POSTGRESQL && partitions != nil {
				partitions := strings.Join(partitions, ", ")
				displayTableName = fmt.Sprintf("%s (%s)", table.CurrentName.Unqualified.MinQuoted, partitions)
			}
			schema := table.SourceName.SchemaName
			uitable.AddRow(schema, displayTableName, exportedRowCount[key])
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
	for i, tableStatus := range exportStatus.Tables {
		if i == 0 {
			if tableStatus.SchemaName != "" {
				addHeader(uitable, "SCHEMA", "TABLE", "ROW COUNT")
			} else {
				addHeader(uitable, "DATABASE", "TABLE", "ROW COUNT")
			}
		}
		table, err := namereg.NameReg.LookupTableName(fmt.Sprintf("%s.%s", tableStatus.SchemaName, tableStatus.TableName))
		if err != nil {
			utils.ErrExit("lookup table %s in name registry : %v", tableStatus.TableName, err)
		}
		displayTableName := table.CurrentName.Unqualified.MinQuoted
		partitions := leafPartitions[table.ForOutput()]
		if source.DBType == POSTGRESQL && partitions != nil {
			partitions := strings.Join(partitions, ", ")
			displayTableName = fmt.Sprintf("%s (%s)", table.CurrentName.Unqualified.MinQuoted, partitions)
		}
		schema := table.CurrentName.SchemaName
		uitable.AddRow(schema, displayTableName, tableStatus.ExportedRowCountSnapshot)

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
	for k, v := range datafileDescriptor.TableNameToExportedColumns {
		renamedTable, isRenamed := renameTableIfRequired(k)
		if isRenamed {
			datafileDescriptor.TableNameToExportedColumns[renamedTable] = v
			delete(datafileDescriptor.TableNameToExportedColumns, k)
		}
	}
	datafileDescriptor.Save()
}

func renameExportSnapshotStatus(exportSnapshotStatusFile *jsonfile.JsonFile[ExportSnapshotStatus]) error {
	err := exportSnapshotStatusFile.Update(func(exportSnapshotStatus *ExportSnapshotStatus) {
		for i, tableStatus := range exportSnapshotStatus.Tables {
			renamedTable, isRenamed := renameTableIfRequired(tableStatus.TableName)
			if isRenamed {
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
	tableList := importFileTasksToTableNameTuples(tasks)
	err := retrieveMigrationUUID()
	if err != nil {
		utils.ErrExit("could not retrieve migration UUID: %w", err)
	}
	uitable := uitable.New()

	dbType := "target"
	if importerRole == SOURCE_REPLICA_DB_IMPORTER_ROLE {
		dbType = "source-replica"
	}

	snapshotRowCount := utils.NewStructMap[sqlname.NameTuple, int64]()

	if importerRole == IMPORT_FILE_ROLE {
		for _, tableName := range tableList {
			tableRowCount, err := state.GetImportedSnapshotRowCountForTable(tableName)
			if err != nil {
				utils.ErrExit("could not fetch snapshot row count for table %q: %w", tableName, err)
			}
			snapshotRowCount.Put(tableName, tableRowCount)
		}
	} else {
		snapshotRowCount, err = getImportedSnapshotRowsMap(dbType)
		if err != nil {
			utils.ErrExit("failed to get imported snapshot rows map: %v", err)
		}
	}

	for i, tableName := range tableList {
		if i == 0 {
			addHeader(uitable, "SCHEMA", "TABLE", "IMPORTED ROW COUNT")
		}
		s, t := tableName.ForCatalogQuery()

		rowCount, _ := snapshotRowCount.Get(tableName)
		uitable.AddRow(s, t, rowCount)
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
		"assessment", "assessment/metadata", "assessment/dbs", "assessment/metadata/schema", "assessment/reports",
		"metainfo", "metainfo/data", "metainfo/conf", "metainfo/ssl",
		"temp", "temp/ora2pg_temp_dir", "temp/schema",
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

		err := exec.Command("mkdir", "-p", filepath.Join(schemaDir, databaseObjectDirName)).Run()
		if err != nil {
			utils.ErrExit("couldn't create sub-directories under %q: %v", schemaDir, err)
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

func initAssessmentDB() {
	err := migassessment.InitAssessmentDB()
	if err != nil {
		utils.ErrExit("error creating and initializing assessment DB: %v", err)
	}

	assessmentDB, err = migassessment.NewAssessmentDB(source.DBType)
	if err != nil {
		utils.ErrExit("error creating assessment DB instance: %v", err)
	}
}

func InitNameRegistry(
	exportDir string, role string,
	sconf *srcdb.Source, sdb srcdb.SourceDB,
	tconf *tgtdb.TargetConf, tdb tgtdb.TargetDB,
	reregisterYBNames bool) error {

	var sdbReg namereg.SourceDBInterface
	var ybdb namereg.YBDBInterface
	var sourceDbType, sourceDbSchema, sourceDbName string
	var targetDBSchema string

	if sconf != nil {
		sourceDbType = sconf.DBType
		sourceDbName = sconf.DBName
		sourceDbSchema = sconf.Schema
	}
	if sdb != nil {
		sdbReg = sdb.(namereg.SourceDBInterface)
	}

	if tconf != nil {
		targetDBSchema = tconf.Schema
	}
	var ok bool
	if tdb != nil && lo.Contains([]string{TARGET_DB_IMPORTER_ROLE, IMPORT_FILE_ROLE}, role) {
		ybdb, ok = tdb.(namereg.YBDBInterface)
		if !ok {
			return fmt.Errorf("expected targetDB to adhere to YBDBRegirsty")
		}
	}
	nameregistryParams := namereg.NameRegistryParams{
		FilePath:       fmt.Sprintf("%s/metainfo/name_registry.json", exportDir),
		Role:           role,
		SourceDBType:   sourceDbType,
		SourceDBSchema: sourceDbSchema,
		SourceDBName:   sourceDbName,
		TargetDBSchema: targetDBSchema,
		SDB:            sdbReg,
		YBDB:           ybdb,
	}

	err := namereg.InitNameRegistry(nameregistryParams)
	if err != nil {
		return err
	}
	if reregisterYBNames {
		// clean up yb names and re-init.
		err := namereg.NameReg.UnRegisterYBNames()
		if err != nil {
			return fmt.Errorf("unregister yb names: %v", err)
		}
		err = namereg.NameReg.Init()
		if err != nil {
			return fmt.Errorf("init name registry: %v", err)
		}
	}
	return nil
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

func getImportTableList(sourceTableList []string) ([]sqlname.NameTuple, error) {
	if importerRole == IMPORT_FILE_ROLE {
		return nil, nil
	}
	var tableList []sqlname.NameTuple
	sqlname.SourceDBType = source.DBType
	for _, qualifiedTableName := range sourceTableList {
		table, err := namereg.NameReg.LookupTableName(qualifiedTableName)
		if err != nil {
			return nil, fmt.Errorf("lookup table %s in name registry : %v", qualifiedTableName, err)
		}
		tableList = append(tableList, table)
	}
	return tableList, nil
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
	PrintElapsedDuration()
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
		tableTup, err := namereg.NameReg.LookupTableName(renameTablesMap[fromTable])
		if err != nil {
			utils.ErrExit("lookup failed for the table  %s", renameTablesMap[fromTable])
		}

		return tableTup.ForMinOutput(), true
	}
	return table, false
}

func getExportedSnapshotRowsMap(exportSnapshotStatus *ExportSnapshotStatus) (*utils.StructMap[sqlname.NameTuple, int64], *utils.StructMap[sqlname.NameTuple, []string], error) {
	snapshotRowsMap := utils.NewStructMap[sqlname.NameTuple, int64]()
	snapshotStatusMap := utils.NewStructMap[sqlname.NameTuple, []string]()

	for _, tableStatus := range exportSnapshotStatus.Tables {
		if tableStatus.FileName == "" {
			//in case of root table as well in the tablelist during export an entry with empty file name is there
			continue
		}
		nt, err := namereg.NameReg.LookupTableName(tableStatus.TableName)
		if err != nil {
			return nil, nil, fmt.Errorf("lookup table [%s] from name registry: %v", tableStatus.TableName, err)
		}
		existingSnapshotRows, _ := snapshotRowsMap.Get(nt)
		snapshotRowsMap.Put(nt, existingSnapshotRows+tableStatus.ExportedRowCountSnapshot)
		existingStatuses, _ := snapshotStatusMap.Get(nt)
		existingStatuses = append(existingStatuses, tableStatus.Status)
		snapshotStatusMap.Put(nt, existingStatuses)
	}

	return snapshotRowsMap, snapshotStatusMap, nil
}

func getImportedSnapshotRowsMap(dbType string) (*utils.StructMap[sqlname.NameTuple, int64], error) {
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

	snapshotRowsMap := utils.NewStructMap[sqlname.NameTuple, int64]()
	dataFilePathNtMap := map[string]sqlname.NameTuple{}
	if snapshotDataFileDescriptor != nil {
		for _, fileEntry := range snapshotDataFileDescriptor.DataFileList {
			nt, err := namereg.NameReg.LookupTableName(fileEntry.TableName)
			if err != nil {
				return nil, fmt.Errorf("lookup table name from data file descriptor %s : %v", fileEntry.TableName, err)
			}
			dataFilePathNtMap[fileEntry.FilePath] = nt
		}
	}

	for dataFilePath, nt := range dataFilePathNtMap {
		snapshotRowCount, err := state.GetImportedRowCount(dataFilePath, nt)
		if err != nil {
			return nil, fmt.Errorf("could not fetch snapshot row count for table %q: %w", nt, err)
		}
		existingRows, _ := snapshotRowsMap.Get(nt)
		snapshotRowsMap.Put(nt, existingRows+snapshotRowCount)
	}
	return snapshotRowsMap, nil
}


func getImportedSizeMap() (*utils.StructMap[sqlname.NameTuple, int64], error) { //used for import data file case right now
	importerRole = IMPORT_FILE_ROLE
	state := NewImportDataState(exportDir)
	dataFileDescriptor, err := prepareDummyDescriptor(state)
	if err != nil {
		return nil, fmt.Errorf("prepare dummy descriptor: %w", err)
	}
	snapshotRowsMap := utils.NewStructMap[sqlname.NameTuple, int64]()
	for _, fileEntry := range dataFileDescriptor.DataFileList {
		nt, err := namereg.NameReg.LookupTableName(fileEntry.TableName)
		if err != nil {
			return nil, fmt.Errorf("lookup table name from data file descriptor %s : %v", fileEntry.TableName, err)
		}
		byteCount, err := state.GetImportedByteCount(fileEntry.FilePath, nt)
		if err != nil {
			return nil, fmt.Errorf("could not fetch snapshot row count for table %q: %w", nt, err)
		}
		exisitingByteCount, _ := snapshotRowsMap.Get(nt)
		snapshotRowsMap.Put(nt, exisitingByteCount+byteCount)
	}
	return snapshotRowsMap, nil
}

func storeTableListInMSR(tableList []sqlname.NameTuple) error {
	minQuotedTableList := lo.Uniq(lo.Map(tableList, func(table sqlname.NameTuple, _ int) string {
		// Store list of tables in MSR with root table in case of partitions
		renamedTable, isRenamed := renameTableIfRequired(table.ForOutput())
		if isRenamed {
			tuple, err := namereg.NameReg.LookupTableName(renamedTable)
			if err != nil {
				return fmt.Sprintf("lookup table %s in name registry : %v", renamedTable, err)
			}
			return tuple.ForOutput()
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

// =====================================================================

type AssessmentReport struct {
	SchemaSummary        utils.SchemaSummary                   `json:"SchemaSummary"`
	Sizing               *migassessment.SizingAssessmentReport `json:"Sizing"`
	UnsupportedDataTypes []utils.TableColumnsDataTypes         `json:"UnsupportedDataTypes"`
	UnsupportedFeatures  []UnsupportedFeature                  `json:"UnsupportedFeatures"`
	TableIndexStats      *[]migassessment.TableIndexStats      `json:"TableIndexStats"`
}

// =============== for yugabyted controlplane ==============//
// TODO: see if this can be accommodated in controlplane pkg, facing pkg cyclic dependency issue
type AssessMigrationPayload struct {
	AssessmentJsonReport  AssessmentReport
	MigrationComplexity   string
	SourceSizeDetails     SourceDBSizeDetails
	TargetRecommendations TargetSizingRecommendations
	ConversionIssues      []utils.Issue
}

type SourceDBSizeDetails struct {
	TotalDBSize        int64
	TotalTableSize     int64
	TotalIndexSize     int64
	TotalTableRowCount int64
}

type TargetSizingRecommendations struct {
	TotalColocatedSize int64
	TotalShardedSize   int64
}

//==========================================//

func ParseJSONToAssessmentReport(reportPath string) (*AssessmentReport, error) {
	var report AssessmentReport
	err := jsonfile.NewJsonFile[AssessmentReport](reportPath).Load(&report)
	if err != nil {
		return nil, fmt.Errorf("failed to parse json report file %q: %w", reportPath, err)
	}

	return &report, nil
}

func (ar *AssessmentReport) GetShardedTablesRecommendation() ([]string, error) {
	if ar.Sizing == nil {
		return nil, fmt.Errorf("sizing report is null, can't fetch sharded tables")
	}

	return ar.Sizing.SizingRecommendation.ShardedTables, nil
}

func (ar *AssessmentReport) GetColocatedTablesRecommendation() ([]string, error) {
	if ar.Sizing == nil {
		return nil, fmt.Errorf("sizing report is null, can't fetch colocated tables")
	}

	return ar.Sizing.SizingRecommendation.ColocatedTables, nil
}

func (ar *AssessmentReport) GetClusterSizingRecommendation() string {
	if ar.Sizing == nil {
		return ""
	}

	if ar.Sizing.FailureReasoning != "" {
		return ar.Sizing.FailureReasoning
	}

	return fmt.Sprintf("Num Nodes: %f, vCPU per instance: %d, Memory per instance: %d, Estimated Import Time: %f minutes",
		ar.Sizing.SizingRecommendation.NumNodes, ar.Sizing.SizingRecommendation.VCPUsPerInstance,
		ar.Sizing.SizingRecommendation.MemoryPerInstance, ar.Sizing.SizingRecommendation.EstimatedTimeInMinForImport)
}

// ==========================================================================

func createCallhomePayload() callhome.Payload {
	var payload callhome.Payload
	payload.MigrationUUID = migrationUUID
	payload.PhaseStartTime = startTime.UTC().Format("2006-01-02T15:04:05.999999")
	payload.YBVoyagerVersion = utils.YB_VOYAGER_VERSION
	payload.TimeTakenSec = int(math.Ceil(time.Since(startTime).Seconds()))
	payload.CollectedAt = time.Now().UTC().Format("2006-01-02T15:04:05.999999")

	return payload
}

func PackAndSendCallhomePayloadOnExit() {
	if callHomeErrorOrCompletePayloadSent {
		return
	}
	switch currentCommand {
	case assessMigrationCmd.CommandPath():
		packAndSendAssessMigrationPayload(EXIT, "Exiting....")
	case exportSchemaCmd.CommandPath():
		packAndSendExportSchemaPayload(EXIT)
	case analyzeSchemaCmd.CommandPath():
		packAndSendAnalyzeSchemaPayload(EXIT)
	case importSchemaCmd.CommandPath():
		packAndSendImportSchemaPayload(EXIT, "Exiting....")
	case exportDataCmd.CommandPath(), exportDataFromSrcCmd.CommandPath():
		packAndSendExportDataPayload(EXIT)
	case exportDataFromTargetCmd.CommandPath():
		packAndSendExportDataFromTargetPayload(EXIT)
	case importDataCmd.CommandPath(), importDataToTargetCmd.CommandPath():
		packAndSendImportDataPayload(EXIT)
	case importDataToSourceCmd.CommandPath():
		packAndSendImportDataToSourcePayload(EXIT)
	case importDataToSourceReplicaCmd.CommandPath():
		packAndSendImportDataToSrcReplicaPayload(EXIT)
	case endMigrationCmd.CommandPath():
		packAndSendEndMigrationPayload(EXIT)
	case importDataFileCmd.CommandPath():
		packAndSendImportDataFilePayload(EXIT)
	}
}

func updateExportSnapshotDataStatsInPayload(exportDataPayload *callhome.ExportDataPhasePayload) {
	//Updating the payload with totalRows and LargestTableRows for both debezium/non-debezium case
	if !useDebezium || (changeStreamingIsEnabled(exportType) && source.DBType == POSTGRESQL) { 
		//non-debezium and pg live migration snapshot case reading the export_snapshot_status.json file
		if exportSnapshotStatusFile != nil {
			exportStatusSnapshot, err := exportSnapshotStatusFile.Read()
			if err != nil {
				if !errors.Is(err, fs.ErrNotExist) {
					log.Errorf("callhome: failed to read export status file: %v", err)
				}
			} else {
				exportedSnapshotRow, _, err := getExportedSnapshotRowsMap(exportStatusSnapshot)
				if err != nil {
					log.Errorf("callhome: error while getting exported snapshot rows map: %v", err)
				}
				exportedSnapshotRow.IterKV(func(key sqlname.NameTuple, value int64) (bool, error) {
					exportDataPayload.TotalRows += value
					if value >= exportDataPayload.LargestTableRows {
						exportDataPayload.LargestTableRows = value
					}
					return true, nil
				})
			}
		}
		switch source.DBType {
		case POSTGRESQL:
			exportDataPayload.ExportSnapshotMechanism = "pg_dump"
		case ORACLE, MYSQL:
			exportDataPayload.ExportSnapshotMechanism = "ora2pg"
		}
	} else {
		//debezium case reading export_status.json file
		exportStatusFilePath := filepath.Join(exportDir, "data", "export_status.json")
		dbzmStatus, err := dbzm.ReadExportStatus(exportStatusFilePath)
		if err != nil {
			log.Errorf("callhome: error in reading export status: %v", err)
		}
		if dbzmStatus != nil {
			for _, tableExportStatus := range dbzmStatus.Tables {
				exportDataPayload.TotalRows += tableExportStatus.ExportedRowCountSnapshot
				if tableExportStatus.ExportedRowCountSnapshot > exportDataPayload.LargestTableRows {
					exportDataPayload.LargestTableRows = tableExportStatus.ExportedRowCountSnapshot
				}
			}
		}
		exportDataPayload.ExportSnapshotMechanism = "debezium"
	}
}

func sendCallhomePayloadAtIntervals() {
	for {
		if callHomeErrorOrCompletePayloadSent {
			//for just that corner case if there is some timing clash where complete and in-progress payload are sent together
			break
		}
		time.Sleep(15 * time.Minute)
		switch currentCommand {
		case exportDataCmd.CommandPath(), exportDataFromSrcCmd.CommandPath():
			packAndSendExportDataPayload(INPROGRESS)
		case exportDataFromTargetCmd.CommandPath():
			packAndSendExportDataFromTargetPayload(INPROGRESS)
		case importDataCmd.CommandPath(), importDataToTargetCmd.CommandPath():
			packAndSendImportDataPayload(INPROGRESS)
		case importDataToSourceCmd.CommandPath():
			packAndSendImportDataToSourcePayload(INPROGRESS)
		case importDataToSourceReplicaCmd.CommandPath():
			packAndSendImportDataToSrcReplicaPayload(INPROGRESS)
		case importDataFileCmd.CommandPath():
			packAndSendImportDataFilePayload(INPROGRESS)
		}
	}
}
