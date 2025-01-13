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
	"path/filepath"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/datafile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/datastore"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/metadb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/namereg"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/az"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/gcs"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/s3"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

var (
	fileFormat            string
	delimiter             string
	dataDir               string
	fileTableMapping      string
	hasHeader             utils.BoolStr
	supportedFileFormats  = []string{datafile.CSV, datafile.TEXT}
	fileOpts              string
	escapeChar            string
	quoteChar             string
	nullString            string
	supportedCsvFileOpts  = []string{"escape_char", "quote_char"}
	dataStore             datastore.DataStore
	reportProgressInBytes bool
)

var importDataFileCmd = &cobra.Command{
	Use: "file",
	Short: "This command imports data from given files into YugabyteDB database. The files can be present either in local directories or cloud storages like AWS S3, GCS buckets and Azure blob storage. Incremental data load is also supported.\n" +
		"For more details and examples, visit https://docs.yugabyte.com/preview/yugabyte-voyager/migrate/bulk-data-load/",

	PreRun: func(cmd *cobra.Command, args []string) {
		if tconf.TargetDBType == "" {
			tconf.TargetDBType = YUGABYTEDB
		}
		importerRole = IMPORT_FILE_ROLE
		reportProgressInBytes = true
		validateBatchSizeFlag(batchSizeInNumRows)
		checkImportDataFileFlags(cmd)

		sourceDBType = POSTGRESQL // dummy value - this command is not affected by it
		sqlname.SourceDBType = sourceDBType
		CreateMigrationProjectIfNotExists(sourceDBType, exportDir)
		err := retrieveMigrationUUID()
		if err != nil {
			utils.ErrExit("failed to get migration UUID: %w", err)
		}
		tconf.Schema = strings.ToLower(tconf.Schema)
		tdb = tgtdb.NewTargetDB(&tconf)
		err = tdb.Init()
		if err != nil {
			utils.ErrExit("Failed to initialize the target DB: %s", err)
		}
		targetDBDetails = tdb.GetCallhomeTargetDBInfo()
		err = InitNameRegistry(exportDir, importerRole, nil, nil, &tconf, tdb, bool(startClean))
		if err != nil {
			utils.ErrExit("initialize name registry: %v", err)
		}

	},

	Run: func(cmd *cobra.Command, args []string) {
		dataStore = datastore.NewDataStore(dataDir)
		importFileTasks := prepareImportFileTasks()
		prepareForImportDataCmd(importFileTasks)
		importData(importFileTasks)
		packAndSendImportDataFilePayload(COMPLETE, "")

	},
	PostRun: func(cmd *cobra.Command, args []string) {
		tdb.Finalize()
	},
}

func prepareForImportDataCmd(importFileTasks []*ImportFileTask) {
	err := metaDB.UpdateMigrationStatusRecord(func(record *metadb.MigrationStatusRecord) {
		source.DBType = POSTGRESQL
		record.SourceDBConf = source.Clone()
	})
	if err != nil {
		utils.ErrExit("failed to update migration status record: %v", err)
	}

	dataFileList := getFileSizeInfo(importFileTasks)
	dataFileDescriptor = &datafile.Descriptor{
		FileFormat:   fileFormat,
		DataFileList: dataFileList,
		Delimiter:    delimiter,
		HasHeader:    bool(hasHeader),
		ExportDir:    exportDir,
		NullString:   nullString,
	}
	if quoteChar != "" {
		quoteCharBytes := []byte(quoteChar)
		dataFileDescriptor.QuoteChar = quoteCharBytes[0]
	}
	if escapeChar != "" {
		escapeCharBytes := []byte(escapeChar)
		dataFileDescriptor.EscapeChar = escapeCharBytes[0]
	}

	escapeFileOptsCharsIfRequired() // escaping for COPY command should be done after saving fileOpts in data file descriptor
	setImportTableListFlag(importFileTasks)
	setDataIsExported()
}

func getFileSizeInfo(importFileTasks []*ImportFileTask) []*datafile.FileEntry {
	dataFileList := make([]*datafile.FileEntry, 0)
	for _, task := range importFileTasks {
		filePath := task.FilePath
		tableName := task.TableNameTup
		fileEntry := &datafile.FileEntry{
			TableName: tableName.ForKey(),
			FilePath:  filePath,
			FileSize:  task.FileSize,
			RowCount:  -1, // Not available.
		}
		dataFileList = append(dataFileList, fileEntry)
		log.Infof("File size of %q for table %q: %d", filePath, tableName, fileEntry.FileSize)
	}

	return dataFileList
}

func setImportTableListFlag(importFileTasks []*ImportFileTask) {
	tableList := map[string]bool{}
	for _, task := range importFileTasks {
		//TODO:TABLENAME
		tableList[task.TableNameTup.ForKey()] = true
	}
	tconf.TableList = strings.Join(maps.Keys(tableList), ",")
}

func prepareImportFileTasks() []*ImportFileTask {
	result := []*ImportFileTask{}
	if fileTableMapping == "" {
		return result
	}
	kvs := strings.Split(fileTableMapping, ",")
	for i, kv := range kvs {
		globPattern, table := strings.Split(kv, ":")[0], strings.Split(kv, ":")[1]
		filePaths, err := dataStore.Glob(globPattern)
		if err != nil {
			utils.ErrExit("find files matching pattern %q: %v", globPattern, err)
		}
		if len(filePaths) == 0 {
			utils.ErrExit("no files found for matching pattern %q", globPattern)
		}
		tableNameTuple, err := namereg.NameReg.LookupTableName(table)
		if err != nil {
			utils.ErrExit("lookup table name in name registry: %v", err)
		}
		for _, filePath := range filePaths {
			fileSize, err := dataStore.FileSize(filePath)
			if err != nil {
				utils.ErrExit("calculating file size in bytes: %q: %v", filePath, err)
			}
			task := &ImportFileTask{
				ID:           i,
				FilePath:     filePath,
				TableNameTup: tableNameTuple,
				FileSize:     fileSize,
			}
			result = append(result, task)
		}
	}
	return result
}

func checkImportDataFileFlags(cmd *cobra.Command) {
	fileFormat = strings.ToLower(fileFormat)
	checkFileFormat()
	checkDataDirFlag()
	setDefaultForDelimiter()
	checkDelimiterFlag()
	checkHasHeader()
	checkAndParseEscapeAndQuoteChar()
	setDefaultForNullString()
	getTargetPassword(cmd)
	validateTargetPortRange()
	validateTargetSchemaFlag()
	validateParallelismFlags()
}

func checkFileFormat() {
	supported := false
	for _, supportedFileFormat := range supportedFileFormats {
		if fileFormat == supportedFileFormat {
			supported = true
			break
		}
	}

	if !supported {
		utils.ErrExit("--format %q is not supported", fileFormat)
	}
}

func checkDataDirFlag() {
	if dataDir == "" {
		utils.ErrExit(`Error: required flag "data-dir" not set`)
	}
	if strings.HasPrefix(dataDir, "s3://") {
		s3.ValidateObjectURL(dataDir)
		return
	} else if strings.HasPrefix(dataDir, "gs://") {
		gcs.ValidateObjectURL(dataDir)
		return
	} else if strings.HasPrefix(dataDir, "https://") {
		az.ValidateObjectURL(dataDir)
		return
	}
	if !utils.FileOrFolderExists(dataDir) {
		utils.ErrExit("data-dir: %s doesn't exists!!", dataDir)
	}
	dataDirAbs, err := filepath.Abs(dataDir)
	if err != nil {
		utils.ErrExit("unable to resolve absolute path for data-dir: (%q): %v", dataDir, err)
	}

	exportDirAbs, err := filepath.Abs(exportDir)
	if err != nil {
		utils.ErrExit("unable to resolve absolute path for export-dir: (%q): %v", exportDir, err)
	}

	if strings.HasPrefix(dataDirAbs, exportDirAbs) {
		utils.ErrExit("ERROR: data-dir must be outside the export-dir")
	}
	if dataDir == "." {
		fmt.Println("Note: Using current working directory as data directory")
	}
}

func checkDelimiterFlag() {
	var ok bool
	delimiter, ok = interpreteEscapeSequences(delimiter)
	if !ok {
		utils.ErrExit("ERROR: invalid syntax of flag value in --delimiter %s. It should be a valid single-byte value.", delimiter)
	}
	log.Infof("resolved delimiter value: %q", delimiter)
}

func checkHasHeader() {
	if hasHeader && fileFormat != datafile.CSV {
		utils.ErrExit("--has-header flag is only supported for CSV file format")
	}
}

func checkAndParseEscapeAndQuoteChar() {
	switch fileFormat {
	case datafile.CSV:
		// setting default values for escape and quote
		if escapeChar == "" {
			escapeChar = `"`
		}
		if quoteChar == "" {
			quoteChar = `"`
		}

		if fileOpts != "" {
			keyValuePairs := strings.Split(fileOpts, ",")
			for _, keyValuePair := range keyValuePairs {
				key, value := strings.Split(keyValuePair, "=")[0], strings.Split(keyValuePair, "=")[1]
				key = strings.ToLower(key)
				if !slices.Contains(supportedCsvFileOpts, key) {
					utils.ErrExit("ERROR: %q is not a valid csv file option", key)
				} else {
					if key == "escape_char" {
						escapeChar = value
					} else if key == "quote_char" {
						quoteChar = value
					}
				}
			}
		}
		var ok bool

		escapeChar, ok = interpreteEscapeSequences(escapeChar)
		if !ok {
			utils.ErrExit("ERROR: invalid syntax of --escape-char=%s flag. It should be a valid single-byte value.", escapeChar)
		}

		quoteChar, ok = interpreteEscapeSequences(quoteChar)
		if !ok {
			utils.ErrExit("ERROR: invalid syntax of --quote-char=%s flag. It should be a valid single-byte value.", quoteChar)
		}

	default:
		if escapeChar != "" {
			utils.ErrExit("ERROR: --escape-char flag is invalid for %q format", fileFormat)
		}
		if quoteChar != "" {
			utils.ErrExit("ERROR: --quote-char flag is invalid for %q format", fileFormat)
		}
	}

	log.Infof("escapeChar: %s, quoteChar: %s", escapeChar, quoteChar)

}

func packAndSendImportDataFilePayload(status string, errorMsg string) {
	if !shouldSendCallhome() {
		return
	}
	payload := createCallhomePayload()
	payload.MigrationType = BULK_DATA_LOAD
	payload.TargetDBDetails = callhome.MarshalledJsonString(targetDBDetails)
	payload.MigrationPhase = IMPORT_DATA_FILE_PHASE
	dataFileParameters := callhome.DataFileParameters{
		FileFormat: fileFormat,
		HasHeader:  bool(hasHeader),
		Delimiter:  delimiter,
		EscapeChar: escapeChar,
		QuoteChar:  quoteChar,
		NullString: nullString,
	}
	importDataFilePayload := callhome.ImportDataFilePhasePayload{
		ParallelJobs:       int64(tconf.Parallelism),
		StartClean:         bool(startClean),
		DataFileParameters: callhome.MarshalledJsonString(dataFileParameters),
		Error:              callhome.SanitizeErrorMsg(errorMsg),
	}
	switch true {
	case strings.Contains(dataDir, "s3://"):
		importDataFilePayload.FileStorageType = AWS_S3
	case strings.Contains(dataDir, "gs://"):
		importDataFilePayload.FileStorageType = GCS_BUCKETS
	case strings.Contains(dataDir, "https://"):
		importDataFilePayload.FileStorageType = AZURE_BLOBS
	default:
		importDataFilePayload.FileStorageType = LOCAL_DISK
	}
	importSizeMap, err := getImportedSizeMap()
	if err != nil {
		log.Infof("callhome: error in getting the import data: %v", err)
	} else if importSizeMap != nil {
		importSizeMap.IterKV(func(key sqlname.NameTuple, value int64) (bool, error) {
			importDataFilePayload.TotalSize += value
			if value > importDataFilePayload.LargestTableSize {
				importDataFilePayload.LargestTableSize = value
			}
			return true, nil
		})
	}
	payload.PhasePayload = callhome.MarshalledJsonString(importDataFilePayload)
	payload.Status = status

	err = callhome.SendPayload(&payload)
	if err == nil && (status == COMPLETE || status == ERROR) {
		callHomeErrorOrCompletePayloadSent = true
	}
}

func setDefaultForNullString() {
	if nullString != "" {
		return
	}
	switch fileFormat {
	case datafile.CSV:
		nullString = ""
	case datafile.TEXT:
		nullString = "\\N"
	default:
		panic("unsupported file format")
	}
}

func setDefaultForDelimiter() {
	if delimiter != "" {
		return
	}
	switch fileFormat {
	case datafile.CSV:
		delimiter = `,`
	case datafile.TEXT:
		delimiter = `\t`
	default:
		panic("unsupported file format")
	}
}

// resolves and check the given string is a single byte character
func interpreteEscapeSequences(value string) (string, bool) {
	if len(value) == 1 {
		return value, true
	}
	resolvedValue, err := strconv.Unquote(`"` + value + `"`)
	if err != nil || len(resolvedValue) != 1 {
		return value, false
	}
	return resolvedValue, true
}

// in case of csv file format, escape and quote characters are required to be escaped with
// E in copy Command using backslash if there are single quote or backslash provided
func escapeFileOptsCharsIfRequired() {
	if escapeChar == `'` || escapeChar == `\` {
		escapeChar = `\` + escapeChar
	}

	if quoteChar == `'` || quoteChar == `\` {
		quoteChar = `\` + quoteChar
	}
}

func init() {
	importDataCmd.AddCommand(importDataFileCmd)
	registerCommonGlobalFlags(importDataFileCmd)
	registerTargetDBConnFlags(importDataFileCmd)
	registerImportDataCommonFlags(importDataFileCmd)
	registerFlagsForTarget(importDataFileCmd)

	importDataFileCmd.Flags().StringVar(&fileFormat, "format", "csv",
		fmt.Sprintf("supported data file types: (%v)", strings.Join(supportedFileFormats, ",")))

	importDataFileCmd.Flags().StringVar(&delimiter, "delimiter", "",
		`character used as delimiter in rows of the table(s) (default for csv: "," (comma), for TEXT: "\t" (tab) )`)

	importDataFileCmd.Flags().StringVar(&dataDir, "data-dir", "",
		"path to the directory which contains data files to import into table(s)\n"+
			"Note: data-dir can be a local directory or a cloud storage URL\n"+
			"\tfor AWS S3, e.g. s3://<bucket-name>/<path-to-data-dir>\n"+
			"\tfor GCS buckets, e.g. gs://<bucket-name>/<path-to-data-dir>\n"+
			"\tfor Azure blob storage, e.g. https://<account_name>.blob.core.windows.net/<container_name>/<path-to-data-dir>")
	err := importDataFileCmd.MarkFlagRequired("data-dir")
	if err != nil {
		utils.ErrExit("mark 'data-dir' flag required: %v", err)
	}

	importDataFileCmd.Flags().StringVar(&fileTableMapping, "file-table-map", "",
		"comma separated list of mapping between file name in '--data-dir' to a table in database\n"+
			"You can import multiple files in one table either by providing one entry for each file 'fileName1:tableName,fileName2:tableName' OR by passing a glob expression in place of the file name. 'fileName*:tableName'")

	err = importDataFileCmd.MarkFlagRequired("file-table-map")
	if err != nil {
		utils.ErrExit("mark 'file-table-map' flag required: %v", err)
	}
	BoolVar(importDataFileCmd.Flags(), &hasHeader, "has-header", false,
		"Indicate that the first line of data file is a header row (default false)\n"+
			"(Note: only works for csv file type)")

	importDataFileCmd.Flags().StringVar(&escapeChar, "escape-char", "",
		`escape character. Note: only applicable to CSV file format (default double quotes '"')`)

	importDataFileCmd.Flags().StringVar(&quoteChar, "quote-char", "",
		`character used to quote the values. Note: only applicable to CSV file format (default double quotes '"')`)

	importDataFileCmd.Flags().StringVar(&fileOpts, "file-opts", "",
		`comma separated options for csv file format:
		1. escape_char: escape character (default is double quotes '"')
		2. quote_char: 	character used to quote the values (default double quotes '"')
		for eg: --file-opts "escape_char=\",quote_char=\"" or --file-opts 'escape_char=",quote_char="'`)

	importDataFileCmd.Flags().MarkDeprecated("file-opts", "use --escape-char and --quote-char flags instead")

	importDataFileCmd.Flags().StringVar(&nullString, "null-string", "",
		`string that represents null value in the data file (default for csv: ""(empty string), for text: '\N')`)

	BoolVar(importDataFileCmd.Flags(), &startClean, "start-clean", false,
		`Starts a fresh import with data files present in the data directory. 
If any table on YugabyteDB database is non-empty, it prompts whether you want to continue the import without truncating those tables; 
If you go ahead without truncating, then yb-voyager starts ingesting the data present in the data files with upsert mode.
Note that for the cases where a table doesn't have a primary key, this may lead to insertion of duplicate data. To avoid this, exclude the table from --file-table-map or truncate those tables manually before using the start-clean flag (default false)`)

	BoolVar(importDataFileCmd.Flags(), &truncateTables, "truncate-tables", false, "Truncate tables on target YugabyteDB before importing data. Only applicable along with --start-clean true (default false)")

	importDataFileCmd.Flags().MarkHidden("table-list")
	importDataFileCmd.Flags().MarkHidden("exclude-table-list")
	importDataFileCmd.Flags().MarkHidden("table-list-file-path")
	importDataFileCmd.Flags().MarkHidden("exclude-table-list-file-path")
}
