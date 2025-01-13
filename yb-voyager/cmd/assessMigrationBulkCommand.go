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
	_ "embed"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/config"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

var bulkAssessmentDir string
var fleetConfigPath string
var continueOnError utils.BoolStr
var bulkAssessmentReport BulkAssessmentReport
var bulkAssessmentDBConfigs []AssessMigrationDBConfig

var assessMigrationBulkCmd = &cobra.Command{
	Use:   "assess-migration-bulk",
	Short: "Bulk Assessment of multiple schemas across one or more Oracle database instances",
	Long:  "Bulk Assessment of multiple schemas across one or more Oracle database instances",

	PreRun: func(cmd *cobra.Command, args []string) {
		err := retrieveMigrationUUID()
		if err != nil {
			utils.ErrExit("failed to get migration UUID: %w", err)
		}
		err = validateFleetConfigFile(fleetConfigPath)
		if err != nil {
			utils.ErrExit("validating fleet config file: %s", err.Error())
		}
	},

	Run: func(cmd *cobra.Command, args []string) {
		err := assessMigrationBulk()
		if err != nil {
			utils.ErrExit("failed assess migration bulk: %s", err)
		}
		packAndSendAssessMigrationBulkPayload(COMPLETE, "")
	},
}

func packAndSendAssessMigrationBulkPayload(status string, errorMsg string) {
	if !shouldSendCallhome() {
		return
	}
	log.Infof("sending callhome payload for assess-migration-bulk cmd with status as %s", status)
	payload := createCallhomePayload()
	payload.MigrationPhase = ASSESS_MIGRATION_BULK_PHASE

	for i := 0; i < len(bulkAssessmentDBConfigs); i++ {
		bulkAssessmentDBConfigs[i].Password = ""
	}
	assessMigBulkPayload := callhome.AssessMigrationBulkPhasePayload{
		FleetConfigCount: len(bulkAssessmentDBConfigs),
		Error:            callhome.SanitizeErrorMsg(errorMsg),
	}

	payload.PhasePayload = callhome.MarshalledJsonString(assessMigBulkPayload)
	payload.Status = status

	err := callhome.SendPayload(&payload)
	if err == nil && (status == COMPLETE || status == ERROR) {
		callHomeErrorOrCompletePayloadSent = true
	}
}
func init() {
	rootCmd.AddCommand(assessMigrationBulkCmd)

	// register common global flags
	BoolVar(assessMigrationBulkCmd.Flags(), &perfProfile, "profile", false,
		"profile yb-voyager for performance analysis")
	assessMigrationBulkCmd.Flags().MarkHidden("profile")
	assessMigrationBulkCmd.PersistentFlags().BoolVarP(&utils.DoNotPrompt, "yes", "y", false,
		"assume answer as yes for all questions during migration (default false)")
	BoolVar(assessMigrationBulkCmd.Flags(), &callhome.SendDiagnostics, "send-diagnostics", true,
		"enable or disable the 'send-diagnostics' feature that sends analytics data to YugabyteDB.(default true)")
	assessMigrationBulkCmd.PersistentFlags().StringVarP(&config.LogLevel, "log-level", "l", "info",
		"log level for yb-voyager. Accepted values: (trace, debug, info, warn, error, fatal, panic)")

	const fleetConfigFileHelp = `
Path to the CSV file with connection parameters for schema(s) to be assessed.
Fields (case-insensitive): 'source-db-type', 'source-db-host', 'source-db-port', 'source-db-name', 'oracle-db-sid', 'oracle-tns-alias', 'source-db-user', 'source-db-password', 'source-db-schema'.
Mandatory: 'source-db-type', 'source-db-user', 'source-db-schema', and one of ['source-db-name', 'oracle-db-sid', 'oracle-tns-alias'].
Guidelines:
	- The first line must be a header row.
	- Ensure mandatory fields are included and correctly spelled.

Sample fleet_config_file:
	source-db-type,source-db-host,source-db-port,source-db-name,oracle-db-sid,oracle-tns-alias,source-db-user,source-db-password,source-db-schema
	oracle,localhost,1521,orclpdb,,,user1,password1,schema1
	oracle,localhost,1521,,orclsid,,user2,password2,schema2
`

	// defining flags
	assessMigrationBulkCmd.Flags().StringVar(&fleetConfigPath, "fleet-config-file", "", fleetConfigFileHelp)
	BoolVar(assessMigrationBulkCmd.Flags(), &continueOnError, "continue-on-error", true, "If true, it will print the error message on console and continue to next schema’s assessment")
	assessMigrationBulkCmd.Flags().StringVar(&bulkAssessmentDir, "bulk-assessment-dir", "", "Top-level directory storing the export-dir of each schema")
	BoolVar(assessMigrationBulkCmd.Flags(), &startClean, "start-clean", false, "Cleans up all the export-dirs in bulk assessment directory to start everything from scratch")

	// marking mandatory flags
	assessMigrationBulkCmd.MarkFlagRequired("fleet-config-file")
	assessMigrationBulkCmd.MarkFlagRequired("bulk-assessment-dir")
}

func assessMigrationBulk() error {
	if startClean {
		proceed := utils.AskPrompt(
			"CAUTION: Using --start-clean will delete all progress in each export directory present inside the bulk-assessment-dir. " +
				"Do you want to proceed")
		if !proceed {
			return nil
		}

		// cleaning all export-dir present inside bulk-assessment-dir
		matches, err := filepath.Glob(fmt.Sprintf("%s/*-export-dir", bulkAssessmentDir))
		if err != nil {
			return fmt.Errorf("error while cleaning up export directories: %w", err)
		}
		for _, match := range matches {
			utils.CleanDir(match)
		}

		err = os.RemoveAll(filepath.Join(bulkAssessmentDir, fmt.Sprintf("%s%s", BULK_ASSESSMENT_FILE_NAME, HTML_EXTENSION)))
		if err != nil {
			return fmt.Errorf("failed to remove bulk assessment report: %w", err)
		}
	}

	var err error
	bulkAssessmentDBConfigs, err = parseFleetConfigFile(fleetConfigPath)
	if err != nil {
		return fmt.Errorf("failed to parse fleet config file: %w", err)
	}

	for _, dbConfig := range bulkAssessmentDBConfigs {
		utils.PrintAndLog("\nAssessing '%s' schema", dbConfig.GetSchemaIdentifier())

		if isMigrationAssessmentDoneForConfig(dbConfig) {
			utils.PrintAndLog("assessment report for schema %s already exists, skipping...", dbConfig.GetSchemaIdentifier())
			continue
		}

		err = executeAssessment(dbConfig)
		if err != nil {
			log.Errorf("failed to assess migration for schema %s: %v", dbConfig.GetSchemaIdentifier(), err)
			fmt.Printf("failed to assess migration for schema %s: %v\n", dbConfig.GetSchemaIdentifier(), err)
			log.Infof("For detailed information on the '%s' schema assessment, please refer to the corresponding log file at: %s\n",
				dbConfig.GetSchemaIdentifier(), dbConfig.GetAssessmentLogFilePath())
			if !continueOnError {
				break
			}
		} else {
			log.Infof("For detailed information on the '%s' schema assessment, please refer to the corresponding log file at: %s\n",
				dbConfig.GetSchemaIdentifier(), dbConfig.GetAssessmentLogFilePath())
		}

		if ProcessShutdownRequested {
			log.Info("Exiting from assess-migration-bulk. Further assessments will not be executed due to a shutdown request.")
			return nil
		}
	}

	err = generateBulkAssessmentReport(bulkAssessmentDBConfigs)
	if err != nil {
		return fmt.Errorf("failed to generate bulk assessment report: %w", err)
	}
	return nil
}

func executeAssessment(dbConfig AssessMigrationDBConfig) error {
	log.Infof("executing assessment for schema %q", dbConfig.GetSchemaIdentifier())
	exportDirPath := dbConfig.GetAssessmentExportDirPath()
	cmdArgs := buildCommandArguments(dbConfig, exportDirPath)
	if err := os.MkdirAll(exportDirPath, 0755); err != nil {
		return fmt.Errorf("creating export-directory %q for schema %q: %w", exportDirPath, dbConfig.GetSchemaIdentifier(), err)
	}

	execCmd := exec.Command(os.Args[0], cmdArgs...)
	// password either has to be provided via fleet_config_file or can be provided at run-time by the user.
	// user setting the env var route is not supported for assess-migration-bulk command
	execCmd.Env = append(os.Environ(), "SOURCE_DB_PASSWORD="+dbConfig.Password)
	execCmd.Stdout = os.Stdout
	execCmd.Stderr = os.Stderr
	execCmd.Stdin = os.Stdin
	log.Infof("executing the cmd: %s", execCmd.String())
	err := execCmd.Run()
	if err != nil {
		return fmt.Errorf("error while assess migration of schema-%s: %v", dbConfig.GetSchemaIdentifier(), err)
	}
	return nil
}

func buildCommandArguments(dbConfig AssessMigrationDBConfig, exportDirPath string) []string {
	log.Infof("building assess-migration command arguments for schema %q", dbConfig.GetSchemaIdentifier())
	args := []string{"assess-migration",
		"--source-db-type", dbConfig.DbType,
		"--source-db-schema", dbConfig.Schema,
		"--export-dir", exportDirPath,
		"--log-level", config.LogLevel,
	}

	if dbConfig.User != "" {
		args = append(args, "--source-db-user", dbConfig.User)
	}
	if dbConfig.DbName != "" {
		args = append(args, "--source-db-name", dbConfig.DbName)
	} else {
		// special handling due to issue https://yugabyte.atlassian.net/browse/DB-12481
		args = append(args, "--source-db-name", "")
	}
	if dbConfig.TnsAlias != "" {
		args = append(args, "--oracle-tns-alias", dbConfig.TnsAlias)
	}
	if dbConfig.SID != "" {
		args = append(args, "--oracle-db-sid", dbConfig.SID)
	}
	if dbConfig.Host != "" {
		args = append(args, "--source-db-host", dbConfig.Host)
	}
	if dbConfig.Port != "" {
		args = append(args, "--source-db-port", dbConfig.Port)
	}

	// Always safe to use --start-clean to cleanup if there is some state from previous runs in export-dir
	// since bulk command has separate check to decide beforehand whether the report exists or assessment needs to be performed.
	args = append(args, "--start-clean", "true")

	if utils.DoNotPrompt {
		args = append(args, "--yes")
	}

	return args
}

/*
Sample header: <source-db-type>,<source-db-host>,<source-db-port>,<source-db-name>,<oracle-db-sid>,<oracle-tns-alias>,<source-db-user>,<source-db-password>,<source-db-schema>
*/
func parseFleetConfigFile(filePath string) ([]AssessMigrationDBConfig, error) {
	log.Infof("parsing fleet config file %q", filePath)
	var dbConfigs []AssessMigrationDBConfig
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read fleet config file header: %w", err)
	}
	header = normalizeFleetConfFileHeader(header)

	lineNum := 2
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("failed to read line %d: %w", lineNum, err)
		}

		dbConfig, err := createDBConfigFromRecord(record, header)
		if err != nil {
			return nil, fmt.Errorf("failed to create config for line %d in fleet config file: %w", lineNum, err)
		}
		dbConfigs = append(dbConfigs, *dbConfig)
		lineNum++
	}

	return dbConfigs, nil
}

func createDBConfigFromRecord(record []string, header []string) (*AssessMigrationDBConfig, error) {
	configMap := make(map[string]string)
	for i, field := range header {
		configMap[field] = strings.TrimSpace(record[i])
	}

	// Check if mandatory fields[fleetConfigRequiredFields + fleetConfDbIdentifierFields] are present and non-empty
	missingFields := []string{}
	for _, field := range fleetConfRequiredFields {
		if val, ok := configMap[field]; !ok || val == "" {
			missingFields = append(missingFields, field)
		}
	}
	hasDBIdentifier := false
	for _, field := range fleetConfDbIdentifierFields {
		if val, ok := configMap[field]; ok && val != "" {
			hasDBIdentifier = true
			break
		}
	}
	if !hasDBIdentifier {
		missingFields = append(missingFields, fmt.Sprintf("one of [%s]", strings.Join(fleetConfDbIdentifierFields, ", ")))
	}
	if len(missingFields) > 0 {
		return nil, fmt.Errorf("mandatory fields missing in the record: '%s'", strings.Join(missingFields, "', '"))
	}

	// Check if the source-db-type is supported (only 'oracle' allowed)
	if dbType := configMap[SOURCE_DB_TYPE]; strings.ToLower(dbType) != ORACLE {
		return nil, fmt.Errorf("unsupported/invalid source-db-type: '%s'. Only '%s' is supported", dbType, ORACLE)
	}

	return &AssessMigrationDBConfig{
		DbType:   configMap[SOURCE_DB_TYPE],
		Host:     configMap[SOURCE_DB_HOST],
		Port:     configMap[SOURCE_DB_PORT],
		DbName:   configMap[SOURCE_DB_NAME],
		SID:      configMap[ORACLE_DB_SID],
		TnsAlias: configMap[ORACLE_TNS_ALIAS],
		User:     configMap[SOURCE_DB_USER],
		Password: configMap[SOURCE_DB_PASSWORD],
		Schema:   configMap[SOURCE_DB_SCHEMA],
	}, nil
}

const REPORT_PATH_NOTE = "To automatically apply the recommendations, continue the migration steps(export-schema, import-schema, ..) using the auto-generated export-dirs.</br> " +
	"If using a different export-dir, specify report path in export-schema cmd with `--assessment-report-path` flag  to apply the recommendations."

func generateBulkAssessmentReport(dbConfigs []AssessMigrationDBConfig) error {
	log.Infof("generating bulk assessment report")
	for _, dbConfig := range dbConfigs {
		// extension will set later on during html/json report generation
		assessmentReportBasePath := dbConfig.GetAssessmentReportBasePath()
		var assessmentDetail = AssessmentDetail{
			Schema:             dbConfig.Schema,
			DatabaseIdentifier: dbConfig.GetDatabaseIdentifier(),
			Status:             COMPLETE,
		}
		if !isMigrationAssessmentDoneForConfig(dbConfig) {
			assessmentDetail.Status = ERROR
		} else {
			assessmentReportRelBasePath, err := filepath.Rel(bulkAssessmentDir, assessmentReportBasePath)
			if err != nil {
				return fmt.Errorf("failed to get relative path for %s schema assessment report: %w", dbConfig.GetSchemaIdentifier(), err)
			}
			assessmentDetail.ReportPath = assessmentReportRelBasePath
		}
		bulkAssessmentReport.Details = append(bulkAssessmentReport.Details, assessmentDetail)
	}

	// add notes to the report
	bulkAssessmentReport.Notes = append(bulkAssessmentReport.Notes, REPORT_PATH_NOTE)

	err := generateBulkAssessmentJsonReport()
	if err != nil {
		return fmt.Errorf("failed to generate bulk assessment json report: %w", err)
	}

	err = generateBulkAssessmentHtmlReport()
	if err != nil {
		return fmt.Errorf("failed to generate bulk assessment html report: %w", err)
	}
	return nil
}

func generateBulkAssessmentJsonReport() error {
	for i := range bulkAssessmentReport.Details {
		if bulkAssessmentReport.Details[i].ReportPath != "" {
			bulkAssessmentReport.Details[i].ReportPath = utils.ChangeFileExtension(bulkAssessmentReport.Details[i].ReportPath, JSON_EXTENSION)
		}
	}

	reportPath := filepath.Join(bulkAssessmentDir, fmt.Sprintf("%s%s", BULK_ASSESSMENT_FILE_NAME, JSON_EXTENSION))
	strReport, err := json.MarshalIndent(bulkAssessmentReport, "", "\t")
	if err != nil {
		return fmt.Errorf("failed to marshal the buk assessment report: %w", err)
	}

	err = os.WriteFile(reportPath, strReport, 0644)
	if err != nil {
		return fmt.Errorf("failed to write bulk assessment report to file: %w", err)
	}

	utils.PrintAndLog("generated bulk assessment JSON report at: %s", reportPath)
	return nil
}

//go:embed templates/bulk_assessment_report.template
var bulkAssessmentHtmlTmpl string

func generateBulkAssessmentHtmlReport() error {
	for i := range bulkAssessmentReport.Details {
		if bulkAssessmentReport.Details[i].ReportPath != "" {
			bulkAssessmentReport.Details[i].ReportPath = utils.ChangeFileExtension(bulkAssessmentReport.Details[i].ReportPath, HTML_EXTENSION)
		}
	}

	tmpl, err := template.New("bulk-assessement-report").Parse(bulkAssessmentHtmlTmpl)
	if err != nil {
		return fmt.Errorf("failed to parse the bulkAssessmentReport template: %w", err)
	}

	reportPath := filepath.Join(bulkAssessmentDir, fmt.Sprintf("%s%s", BULK_ASSESSMENT_FILE_NAME, HTML_EXTENSION))
	file, err := os.Create(reportPath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer file.Close()

	err = tmpl.Execute(file, bulkAssessmentReport)
	if err != nil {
		return fmt.Errorf("failed to execute parsed template file: %w", err)
	}
	utils.PrintAndLog("generated bulk assessment HTML report at: %s", reportPath)
	return nil
}

func isMigrationAssessmentDoneForConfig(dbConfig AssessMigrationDBConfig) bool {
	if !utils.FileOrFolderExists(dbConfig.GetHtmlAssessmentReportPath()) ||
		!utils.FileOrFolderExists(dbConfig.GetJsonAssessmentReportPath()) {
		return false
	}

	// Note: Checking for report existence might be sufficient
	// but checking MSR as an additionally covers for edge cases if any.
	metaDBInstance := initMetaDB(dbConfig.GetAssessmentExportDirPath())
	assessmentDone, err := IsMigrationAssessmentDone(metaDBInstance)
	if err != nil {
		log.Warnf("checking migration assessment done: %v", err)
	}
	return assessmentDone
}

func validateBulkAssessmentDirFlag() {
	if bulkAssessmentDir == "" {
		utils.ErrExit(`ERROR: required flag "bulk-assessment-dir" not set`)
	}
	if !utils.FileOrFolderExists(bulkAssessmentDir) {
		utils.ErrExit("bulk-assessment-dir doesn't exists: %q\n", bulkAssessmentDir)
	} else {
		if bulkAssessmentDir == "." {
			fmt.Println("Note: Using current directory as bulk-assessment-dir")
		}
		var err error
		bulkAssessmentDir, err = filepath.Abs(bulkAssessmentDir)
		if err != nil {
			utils.ErrExit("Failed to get absolute path for bulk-assessment-dir: %q: %v\n", exportDir, err)
		}
		bulkAssessmentDir = filepath.Clean(bulkAssessmentDir)
	}
}

var fleetConfFileHeaderFields = []string{SOURCE_DB_TYPE, SOURCE_DB_HOST, SOURCE_DB_PORT, SOURCE_DB_NAME, ORACLE_DB_SID,
	ORACLE_TNS_ALIAS, SOURCE_DB_USER, SOURCE_DB_PASSWORD, SOURCE_DB_SCHEMA}

var fleetConfRequiredFields = []string{SOURCE_DB_TYPE, SOURCE_DB_USER, SOURCE_DB_SCHEMA}
var fleetConfDbIdentifierFields = []string{SOURCE_DB_NAME, ORACLE_DB_SID, ORACLE_TNS_ALIAS}

func validateFleetConfigFile(filePath string) error {
	if filePath == "" {
		utils.ErrExit(`ERROR: required flag "fleet-config-file" not set`)
	}
	// Check if the file exists
	if !utils.FileOrFolderExists(filePath) {
		return fmt.Errorf("fleet config file %q does not exist", filePath)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("could not open fleet config file: %w", err)
	}
	defer file.Close()

	// Check if the file is empty
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("could not obtain fleet config file stats: %w", err)
	}
	if stat.Size() == 0 {
		return fmt.Errorf("fleet config file is empty")
	}

	reader := csv.NewReader(file)
	// we can set it as 0 to error out during Read() but we won't be able to tell - "expected %d fields, got %d"
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read the header: %w", err)
	} else if len(header) == 0 {
		return fmt.Errorf("header is empty or missing")
	}
	header = normalizeFleetConfFileHeader(header)

	// Validate that all fields in the header are allowed ones
	invalidFields := []string{}
	for _, field := range header {
		if !utils.ContainsString(fleetConfFileHeaderFields, field) {
			invalidFields = append(invalidFields, field)
		}
	}
	if len(invalidFields) > 0 {
		return fmt.Errorf("invalid fields found in the fleet config file's header: ['%s']", strings.Join(invalidFields, "', '"))
	}

	// Validate that all the mandatory fields are provided in the header
	err = checkMandatoryFieldsInHeader(header)
	if err != nil {
		return fmt.Errorf("mandatory fields in header validation failed: %w", err)
	}

	// Validate all records to ensure they are correctly formatted as CSV
	lineNumber := 2 // start after header line
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading line %d: %w", lineNumber, err)
		}
		if len(record) != len(header) {
			return fmt.Errorf("line %d does not match header length: expected %d fields, got %d",
				lineNumber, len(header), len(record))
		}
		lineNumber++
	}

	// Check if there were no lines after the header
	if lineNumber == 2 {
		return fmt.Errorf("fleet config file contains only a header with no data lines")
	}
	return nil
}

func checkMandatoryFieldsInHeader(header []string) error {
	missingFields := []string{}
	for _, field := range fleetConfRequiredFields {
		if !utils.ContainsString(header, field) {
			missingFields = append(missingFields, field)
		}
	}

	// Check if at least one of the optional DB identifier fields is present
	hasDBIdentifier := false
	for _, field := range fleetConfDbIdentifierFields {
		if utils.ContainsString(header, field) {
			hasDBIdentifier = true
			break
		}
	}

	if !hasDBIdentifier {
		missingFields = append(missingFields, fmt.Sprintf("one of [%s]", strings.Join(fleetConfDbIdentifierFields, ", ")))
	}
	if len(missingFields) > 0 {
		return fmt.Errorf("mandatory fields missing in the header: '%s'", strings.Join(missingFields, "', '"))
	}
	return nil
}

func normalizeFleetConfFileHeader(header []string) []string {
	header = utils.ToCaseInsensitiveNames(header)
	for i := 0; i < len(header); i++ {
		header[i] = strings.TrimSpace(header[i])
	}
	return header
}

func isBulkAssessmentCommand(cmd *cobra.Command) bool {
	return cmd.Name() == assessMigrationBulkCmd.Name()
}
