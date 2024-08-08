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
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/tebeka/atexit"
	"golang.org/x/exp/slices"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/cp"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/cp/noopcp"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/cp/yugabyted"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/lockfile"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

var (
	cfgFile                            string
	exportDir                          string
	schemaDir                          string
	startClean                         utils.BoolStr
	lockFile                           *lockfile.Lockfile
	migrationUUID                      uuid.UUID
	perfProfile                        utils.BoolStr
	ProcessShutdownRequested           bool
	controlPlane                       cp.ControlPlane
	currentCommand                     string
	callHomeErrorOrCompletePayloadSent bool
	cliArgsString                      string
)

var rootCmd = &cobra.Command{
	Use:   "yb-voyager",
	Short: "A CLI based migration engine to migrate complete database(schema + data) from some source database to YugabyteDB",
	Long: `A CLI based migration engine for complete database migration from a source database to YugabyteDB. Currently supported source databases are Oracle, MySQL, PostgreSQL.
Refer to docs (https://docs.yugabyte.com/preview/migrate/) for more details like setting up source/target, migration workflow etc.`,

	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		currentCommand = cmd.CommandPath()
		if !shouldRunPersistentPreRun(cmd) {
			return
		}
		validateExportDirFlag()
		schemaDir = filepath.Join(exportDir, "schema")
		if shouldLock(cmd) {
			lockFPath := filepath.Join(exportDir, fmt.Sprintf(".%sLockfile.lck", GetCommandID(cmd)))
			lockFile = lockfile.NewLockfile(lockFPath)
			lockFile.Lock()
		}
		InitLogging(exportDir, cmd.Use == "status", GetCommandID(cmd))
		startTime = time.Now()

		if callhome.SendDiagnostics {
			createCLIArgsString(cmd)
			go sendCallhomePayloadAtIntervals()
		}

		log.Infof("Start time: %s\n", startTime)
		if metaDBIsCreated(exportDir) {
			initMetaDB()
		}
		if perfProfile {
			go startPprofServer()
		}
		setControlPlane()
	},

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			os.Exit(0)
		}
	},

	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		if shouldLock(cmd) {
			lockFile.Unlock()
		}
		if shouldRunPersistentPreRun(cmd) {
			controlPlane.Finalize()
		}
		atexit.Exit(0)
	},
}

func createCLIArgsString(cmd *cobra.Command) {
	var flagStrings []string
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		if flag.Changed && !slices.Contains(callhome.DoNotStoreFlags, flag.Name) {
			flagStrings = append(flagStrings, fmt.Sprintf("--%s=%s", flag.Name, flag.Value))
		}
	})
	cliArgsString = strings.Join(flagStrings, " ")
}

func startPprofServer() {
	// Server for pprof
	err := http.ListenAndServe("localhost:6060", nil)
	if err != nil {
		fmt.Println("Error starting pprof server")
	}
	/*
		Steps to use pprof for profiling yb-voyager:
		1. install graphviz on the machine using - sudo yum install graphviz gv
		2. start voyager with profile flag - yb-voyager ... --profile true
		3. use the following command to start a web ui for the profile data-
			go tool pprof -http=[<client_machine_ip>:<port>] http://localhost:6060/debug/pprof/profile
	*/
}

var noLockNeededList = []string{
	"yb-voyager",
	"yb-voyager version",
	"yb-voyager help",
	"yb-voyager import",
	"yb-voyager import data to",
	"yb-voyager import data status",
	"yb-voyager export",
	"yb-voyager export data from",
	"yb-voyager export data status",
	"yb-voyager cutover",
	"yb-voyager cutover status",
	"yb-voyager get data-migration-report",
	"yb-voyager initiate",
	"yb-voyager end",
	"yb-voyager archive",
}

var noPersistentPreRunNeededList = []string{
	"yb-voyager",
	"yb-voyager version",
	"yb-voyager help",
	"yb-voyager import",
	"yb-voyager import data to",
	"yb-voyager export",
	"yb-voyager export data from",
	"yb-voyager initiate",
	"yb-voyager initiate cutover",
	"yb-voyager initiate cutover to",
	"yb-voyager cutover",
	"yb-voyager archive",
	"yb-voyager end",
}

func shouldLock(cmd *cobra.Command) bool {
	return !slices.Contains(noLockNeededList, cmd.CommandPath())
}

func shouldRunPersistentPreRun(cmd *cobra.Command) bool {
	return !slices.Contains(noPersistentPreRunNeededList, cmd.CommandPath())
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.CompletionOptions.DisableDefaultCmd = true

	callhome.ReadEnvSendDiagnostics()
}

func registerCommonGlobalFlags(cmd *cobra.Command) {
	BoolVar(cmd.Flags(), &perfProfile, "profile", false,
		"profile yb-voyager for performance analysis")
	cmd.Flags().MarkHidden("profile")

	registerExportDirFlag(cmd)

	cmd.PersistentFlags().BoolVarP(&utils.DoNotPrompt, "yes", "y", false,
		"assume answer as yes for all questions during migration (default false)")

	BoolVar(cmd.Flags(), &callhome.SendDiagnostics, "send-diagnostics", true,
		"enable or disable the 'send-diagnostics' feature that sends analytics data to YugabyteDB.")
}

func registerExportDirFlag(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&exportDir, "export-dir", "e", "",
		"export directory is the workspace used to keep the exported schema, data, state, and logs")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".yb-voyager" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".yb-voyager")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}

func validateExportDirFlag() {
	if exportDir == "" {
		utils.ErrExit(`ERROR: required flag "export-dir" not set`)
	}
	if !utils.FileOrFolderExists(exportDir) {
		utils.ErrExit("export-dir %q doesn't exists.\n", exportDir)
	} else {
		if exportDir == "." {
			fmt.Println("Note: Using current directory as export-dir")
		}
		var err error
		exportDir, err = filepath.Abs(exportDir)
		if err != nil {
			utils.ErrExit("Failed to get absolute path for export-dir %q: %v\n", exportDir, err)
		}
		exportDir = filepath.Clean(exportDir)
	}
}

func GetCommandID(c *cobra.Command) string {
	if c.HasParent() {
		p := GetCommandID(c.Parent())
		if p == "" {
			return c.Name()
		} else {
			return p + "-" + c.Name()
		}
	}
	return ""
}

func BoolVar(flagSet *pflag.FlagSet, p *utils.BoolStr, name string, value bool, usage string) {
	*p = utils.BoolStr(value)
	flagSet.AddFlag(&pflag.Flag{
		Name:     name,
		Usage:    usage + "; accepted values (true, false, yes, no, 0, 1)",
		Value:    p,
		DefValue: fmt.Sprintf("%t", value),
	})
}

func metaDBIsCreated(exportDir string) bool {
	return utils.FileOrFolderExists(filepath.Join(exportDir, "metainfo", "meta.db"))
}

func setControlPlane() {
	cpType := os.Getenv("CONTROL_PLANE_TYPE")

	switch cpType {
	case "":
		log.Infof("'CONTROL_PLANE_TYPE' environment variable not set. Setting cp to NoopControlPlane.")
		controlPlane = noopcp.New()
	case "yugabyted":
		ybdConnString := os.Getenv("YUGABYTED_DB_CONN_STRING")
		if ybdConnString == "" {
			utils.ErrExit("'YUGABYTED_DB_CONN_STRING' environment variable needs to be set if 'CONTROL_PLANE_TYPE' is 'yugabyted'.")
		}
		controlPlane = yugabyted.New(exportDir)
		log.Infof("Migration UUID %s", migrationUUID)
		err := controlPlane.Init()
		if err != nil {
			utils.ErrExit("ERROR: Failed to initialize the target DB for visualization. %s", err)
		}
	}
}
