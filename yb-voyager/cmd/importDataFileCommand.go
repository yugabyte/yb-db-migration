/*
Copyright (c) YugaByte, Inc.

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

	"github.com/yugabyte/yb-voyager/yb-voyager/src/datafile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/datastore"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/s3"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

var (
	fileFormat            string
	delimiter             string
	dataDir               string
	fileTableMapping      string
	hasHeader             bool
	importFileTasks       []*ImportFileTask
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
	Use:   "file",
	Short: "This command imports data from given files into YugabyteDB database",

	Run: func(cmd *cobra.Command, args []string) {
		reportProgressInBytes = true
		checkImportDataFileFlags(cmd)
		dataStore = datastore.NewDataStore(dataDir)
		importFileTasks = prepareImportFileTasks()
		prepareForImportDataCmd()
		importData(importFileTasks)
	},
}

func prepareForImportDataCmd() {
	sourceDBType = POSTGRESQL // dummy value - this command is not affected by it
	sqlname.SourceDBType = sourceDBType
	CreateMigrationProjectIfNotExists(sourceDBType, exportDir)
	dataFileList := getFileSizeInfo()
	dataFileDescriptor = &datafile.Descriptor{
		FileFormat:   fileFormat,
		DataFileList: dataFileList,
		Delimiter:    delimiter,
		HasHeader:    hasHeader,
		ExportDir:    exportDir,
	}
	if quoteChar != "" {
		quoteCharBytes := []byte(quoteChar)
		dataFileDescriptor.QuoteChar = quoteCharBytes[0]
	}
	if escapeChar != "" {
		escapeCharBytes := []byte(escapeChar)
		dataFileDescriptor.EscapeChar = escapeCharBytes[0]
	}
	// `import data status` depends on the saved descriptor file for the file-sizes.
	// Can't get rid of it until we extract the file-size info out of descriptor file.
	dataFileDescriptor.Save()

	escapeFileOptsCharsIfRequired() // escaping for COPY command should be done after saving fileOpts in data file descriptor
	prepareCopyCommands()
	setImportTableListFlag()
	createExportDataDoneFlag()
}

func getFileSizeInfo() []*datafile.FileEntry {
	dataFileList := make([]*datafile.FileEntry, 0)
	for _, task := range importFileTasks {
		filePath := task.FilePath
		tableName := task.TableName
		fileSize, err := dataStore.FileSize(filePath)
		if err != nil {
			utils.ErrExit("calculating file size of %q in bytes: %v", filePath, err)
		}
		fileEntry := &datafile.FileEntry{
			TableName: tableName,
			FilePath:  filePath,
			FileSize:  fileSize,
			RowCount:  -1, // Not available.
		}
		dataFileList = append(dataFileList, fileEntry)
		log.Infof("File size of %q for table %q: %d", filePath, tableName, fileSize)
	}

	return dataFileList
}

func prepareCopyCommands() {
	log.Infof("preparing copy commands for the tables to import")
	for _, task := range importFileTasks {
		filePath := task.FilePath
		table := task.TableName
		// Skip the loop body if the `table` entry is already present in the copyTableFromCommands map.
		// TODO: Calculate the copy commands per file and not per table.
		if _, ok := copyTableFromCommands.Load(table); ok {
			continue
		}
		cmd := ""
		if fileFormat == datafile.CSV {
			if hasHeader {
				reader, err := dataStore.Open(filePath)
				if err != nil {
					utils.ErrExit("preparing reader for copy commands on file %q: %v", filePath, err)
				}
				df, err := datafile.NewDataFile(filePath, reader, dataFileDescriptor)
				if err != nil {
					utils.ErrExit("opening datafile %q to prepare copy command: %v", filePath, err)
				}
				cmd = fmt.Sprintf(`COPY %s(%s) FROM STDIN WITH (FORMAT %s, DELIMITER E'%c', ESCAPE E'%s', QUOTE E'%s', HEADER, NULL '%s',`,
					table, df.GetHeader(), fileFormat, []rune(delimiter)[0], escapeChar, quoteChar, nullString)
				df.Close()
			} else {
				cmd = fmt.Sprintf(`COPY %s FROM STDIN WITH (FORMAT %s, DELIMITER E'%c', ESCAPE E'%s', QUOTE E'%s', NULL '%s',`,
					table, fileFormat, []rune(delimiter)[0], escapeChar, quoteChar, nullString)
			}
		} else if fileFormat == datafile.TEXT {
			cmd = fmt.Sprintf(`COPY %s FROM STDIN WITH (FORMAT %s, DELIMITER E'%c', NULL '%s',`,
				table, fileFormat, []rune(delimiter)[0], nullString)
		} else {
			panic(fmt.Sprintf("File Type %q not implemented\n", fileFormat))
		}
		cmd += ` ROWS_PER_TRANSACTION %v)`
		copyTableFromCommands.Store(table, cmd)
	}
}

func setImportTableListFlag() {
	tableList := map[string]bool{}
	for _, task := range importFileTasks {
		tableList[task.TableName] = true
	}
	target.TableList = strings.Join(maps.Keys(tableList), ",")
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
		for _, filePath := range filePaths {
			task := &ImportFileTask{
				ID:        i,
				FilePath:  filePath,
				TableName: table,
			}
			result = append(result, task)
		}
	}
	return result
}

func checkImportDataFileFlags(cmd *cobra.Command) {
	validateExportDirFlag()
	fileFormat = strings.ToLower(fileFormat)
	checkFileFormat()
	checkDataDirFlag()
	setDefaultForDelimiter()
	checkDelimiterFlag()
	checkHasHeader()
	checkAndParseEscapeAndQuoteChar()
	setDefaultForNullString()
	validateTargetPassword(cmd)
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
	}
	if !utils.FileOrFolderExists(dataDir) {
		utils.ErrExit("data-dir: %s doesn't exists!!", dataDir)
	}
	dataDirAbs, err := filepath.Abs(dataDir)
	if err != nil {
		utils.ErrExit("unable to resolve absolute path for data-dir(%q): %v", dataDir, err)
	}

	exportDirAbs, err := filepath.Abs(exportDir)
	if err != nil {
		utils.ErrExit("unable to resolve absolute path for export-dir(%q): %v", exportDir, err)
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
	registerCommonImportFlags(importDataFileCmd)
	registerImportDataFlags(importDataFileCmd)

	importDataFileCmd.Flags().StringVar(&fileFormat, "format", "csv",
		fmt.Sprintf("supported data file types: %v", supportedFileFormats))

	importDataFileCmd.Flags().StringVar(&delimiter, "delimiter", "",
		`character used as delimiter in rows of the table(s)(default is comma for CSV and tab for TEXT format)`)

	importDataFileCmd.Flags().StringVar(&dataDir, "data-dir", "",
		"path to the directory contains data files to import into table(s)")
	err := importDataFileCmd.MarkFlagRequired("data-dir")
	if err != nil {
		utils.ErrExit("mark 'data-dir' flag required: %v", err)
	}

	importDataFileCmd.Flags().StringVar(&fileTableMapping, "file-table-map", "",
		"comma separated list of mapping between file name in '--data-dir' to a table in database.\n"+
			"Note: default will be to import all the files in --data-dir with given format, for eg: table1.csv to import into table1 on target database.")

	importDataFileCmd.Flags().BoolVar(&hasHeader, "has-header", false,
		"true - if first line of data file is a list of columns for rows (default false)\n"+
			"(Note: only works for csv file type)")

	importDataFileCmd.Flags().StringVar(&escapeChar, "escape-char", "",
		`escape character (default double quotes '"') only applicable to CSV file format`)

	importDataFileCmd.Flags().StringVar(&quoteChar, "quote-char", "",
		`character used to quote the values (default double quotes '"') only applicable to CSV file format`)

	importDataFileCmd.Flags().StringVar(&fileOpts, "file-opts", "",
		`comma separated options for csv file format:
		1. escape_char: escape character (default is double quotes '"')
		2. quote_char: 	character used to quote the values (default double quotes '"')
		for eg: --file-opts "escape_char=\",quote_char=\"" or --file-opts 'escape_char=",quote_char="'`)

	importDataFileCmd.Flags().MarkDeprecated("file-opts", "use --escape-char and --quote-char flags instead")

	importDataFileCmd.Flags().StringVar(&nullString, "null-string", "",
		`string that represents null value in the data file (default for csv: ""(empty string), for text: '\N')`)
}
