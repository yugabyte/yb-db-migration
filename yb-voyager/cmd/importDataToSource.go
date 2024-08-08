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

	"github.com/spf13/cobra"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/dbzm"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

var importDataToSourceCmd = &cobra.Command{
	Use: "source",
	Short: "Import data into the source DB to prepare for fall-back.\n" +
		"For more details and examples, visit https://docs.yugabyte.com/preview/yugabyte-voyager/migrate/live-fall-back/",
	Long: ``,

	Run: func(cmd *cobra.Command, args []string) {
		validateMetaDBCreated()
		importType = SNAPSHOT_AND_CHANGES
		importerRole = SOURCE_DB_IMPORTER_ROLE
		err := initTargetConfFromSourceConf()
		if err != nil {
			utils.ErrExit("failed to setup target conf from source conf in MSR: %v", err)
		}
		importDataCmd.PreRun(cmd, args)
		importDataCmd.Run(cmd, args)
	},
}

func init() {
	importDataToCmd.AddCommand(importDataToSourceCmd)
	registerCommonGlobalFlags(importDataToSourceCmd)
	registerCommonImportFlags(importDataToSourceCmd)
	registerSourceDBAsTargetConnFlags(importDataToSourceCmd)
	registerFlagsForSourceReplica(importDataToSourceCmd)
	registerImportDataCommonFlags(importDataToSourceCmd)
	hideImportFlagsInFallForwardOrBackCmds(importDataToSourceCmd)
	importDataToSourceCmd.Flags().MarkHidden("batch-size")
}

func initTargetConfFromSourceConf() error {
	msr, err := metaDB.GetMigrationStatusRecord()
	if err != nil {
		return fmt.Errorf("get migration status record: %v", err)
	}
	sconf := msr.SourceDBConf
	tconf.TargetDBType = sconf.DBType
	tconf.Host = sconf.Host
	tconf.Port = sconf.Port
	tconf.User = sconf.User
	tconf.DBName = sconf.DBName
	if tconf.TargetDBType == POSTGRESQL {
		tconf.Schema = strings.Join(strings.Split(sconf.Schema, "|"), ",")
	} else {
		tconf.Schema = sconf.Schema
	}
	tconf.SSLMode = sconf.SSLMode
	tconf.SSLMode = sconf.SSLMode
	tconf.SSLCertPath = sconf.SSLCertPath
	tconf.SSLKey = sconf.SSLKey
	tconf.SSLRootCert = sconf.SSLRootCert
	tconf.SSLCRL = sconf.SSLCRL
	tconf.SSLQueryString = sconf.SSLQueryString
	tconf.DBSid = sconf.DBSid
	tconf.TNSAlias = sconf.TNSAlias
	tconf.OracleHome = sconf.OracleHome
	tconf.Uri = sconf.Uri
	return nil
}

func packAndSendImportDataToSourcePayload(status string) {

	if !shouldSendCallhome() {
		return
	}
	payload := createCallhomePayload()

	payload.MigrationType = LIVE_MIGRATION

	sourceDBDetails := callhome.SourceDBDetails{
		Host:      tconf.Host,
		DBType:    tconf.TargetDBType,
		DBVersion: targetDBDetails.DBVersion,
	}
	payload.SourceDBDetails = callhome.MarshalledJsonString(sourceDBDetails)

	payload.MigrationPhase = IMPORT_DATA_SOURCE_PHASE
	importDataPayload := callhome.ImportDataPhasePayload{
		ParallelJobs:     int64(tconf.Parallelism),
		StartClean:       bool(startClean),
		LiveWorkflowType: FALL_BACK,
		CommandLineArgs:  cliArgsString,
	}

	importDataPayload.Phase = importPhase

	if importPhase != dbzm.MODE_SNAPSHOT {
		importDataPayload.EventsImportRate = statsReporter.EventsImportRateLast3Min
		importDataPayload.TotalImportedEvents = statsReporter.TotalEventsImported
	}

	payload.PhasePayload = callhome.MarshalledJsonString(importDataPayload)
	payload.Status = status

	err := callhome.SendPayload(&payload)
	if err == nil && (status == COMPLETE || status == ERROR) {
		callHomeErrorOrCompletePayloadSent = true
	}
}
