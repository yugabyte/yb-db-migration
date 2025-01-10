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
package dbzm

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

const (
	TARGET_DB_EXPORTER_FF_ROLE = "target_db_exporter_ff"
	TARGET_DB_EXPORTER_FB_ROLE = "target_db_exporter_fb"
)

type Config struct {
	LogLevel           string
	MigrationUUID      uuid.UUID
	RunId              string
	SourceDBType       string
	ExporterRole       string
	ExportDir          string
	MetadataDBPath     string
	UseYBgRPCConnector bool

	Host     string
	Port     int
	Username string
	Password string

	DatabaseName                string
	PDBName                     string
	SchemaNames                 string
	TableList                   []string
	ColumnSequenceMapping       string
	InitSequenceMaxMapping      string
	TableRenameMapping          string
	ColumnList                  []string
	Uri                         string
	TNSAdmin                    string
	OracleJDBCWalletLocationSet bool

	SSLMode               string
	SSLCertPath           string
	SSLKey                string
	SSLRootCert           string
	SSLKeyStore           string
	SSLKeyStorePassword   string
	SSLTrustStore         string
	SSLTrustStorePassword string
	YBStreamID            string
	YBMasterNodes         string
	SnapshotMode          string
	ReplicationSlotName   string
	PublicationName       string
	TransactionOrdering   utils.BoolStr
}

var baseConfigTemplate = `
debezium.format.value=connect
debezium.format.key=connect
quarkus.http.port=%d
quarkus.log.console.json=false
quarkus.log.level=%s
`

var baseSrcConfigTemplate = `
debezium.source.database.user=%s

debezium.source.snapshot.mode=%s
debezium.source.offset.storage.file.filename=%s
debezium.source.offset.flush.interval.ms=0

debezium.source.table.include.list=%s
debezium.source.interval.handling.mode=string
debezium.source.include.unknown.datatypes=true
debezium.source.datatype.propagate.source.type=.*BOX.*,.*LINE.*,.*LSEG.*,.*PATH.*,.*POLYGON.*,.*CIRCLE.*,.*DATE.*,.*INTERVAL.*,.*CHAR.*,.*TIMESTAMP.*,.*LONG.*,.*HSTORE.*
debezium.source.tombstones.on.delete=false

debezium.source.topic.naming.strategy=io.debezium.server.ybexporter.DummyTopicNamingStrategy
debezium.source.topic.prefix=yb-voyager
debezium.source.database.server.name=yb-voyager
`
var baseSinkConfigTemplate = `
debezium.sink.type=ybexporter
debezium.sink.ybexporter.dataDir=%s
debezium.sink.ybexporter.column_sequence.map=%s
debezium.sink.ybexporter.sequence.max.map=%s
debezium.sink.ybexporter.tables.rename=%s
debezium.sink.ybexporter.queueSegmentMaxBytes=%d
debezium.sink.ybexporter.metadata.db.path=%s
debezium.sink.ybexporter.run.id=%s
debezium.sink.ybexporter.exporter.role=%s
`

var postgresSrcConfigTemplate = `
debezium.source.database.hostname=%s
debezium.source.database.port=%d
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.database.dbname=%s
debezium.source.schema.include.list=%s
debezium.source.plugin.name=pgoutput
debezium.source.hstore.handling.mode=map
debezium.source.converters=postgres_to_yb_converter
debezium.source.postgres_to_yb_converter.type=io.debezium.server.ybexporter.PostgresToYbValueConverter
debezium.source.provide.transaction.metadata=true
debezium.source.publication.autocreate.mode=disabled
`

var postgresSSLConfigTemplate = `
debezium.source.database.sslmode=%s
debezium.source.database.sslcert=%s
debezium.source.database.sslkey=%s
debezium.source.database.sslpassword=
debezium.source.database.sslrootcert=%s
`

var postgresReplicationSlotNameTemplate = `
debezium.source.slot.name=%s`

var postgresPublicationNameTemplate = `
debezium.source.publication.name=%s
`

var postgresConfigTemplate = baseConfigTemplate +
	baseSrcConfigTemplate +
	postgresSrcConfigTemplate +
	baseSinkConfigTemplate

// ref for perf tuning - https://debezium.io/blog/2023/06/29/debezium-oracle-series-part-3/

var oracleSrcConfigTemplate = `
debezium.source.database.url=%s
debezium.source.connector.class=io.debezium.connector.oracle.OracleConnector
debezium.source.database.dbname=PLACEHOLDER
#debezium.source.database.pdb.name=ORCLPDB1
debezium.source.schema.include.list=%s
debezium.source.hstore.handling.mode=map
debezium.source.database.history=io.debezium.relational.history.FileDatabaseHistory
debezium.source.database.history.file.filename=%s
debezium.source.schema.history.internal=io.debezium.storage.file.history.FileSchemaHistory
debezium.source.schema.history.internal.file.filename=%s
debezium.source.schema.history.internal.skip.unparseable.ddl=true
debezium.source.schema.history.internal.store.only.captured.tables.ddl=true
debezium.source.schema.history.internal.store.only.captured.databases.ddl=true
debezium.source.include.schema.changes=false
debezium.source.log.mining.batch.size.min=10000
debezium.source.log.mining.batch.size.max=100000
debezium.source.log.mining.batch.size.default=10000
debezium.source.log.mining.query.filter.mode=in
debezium.source.log.mining.sleep.time.default.ms=200
debezium.source.log.mining.sleep.time.max.ms=400
debezium.source.log.mining.flush.table.name=%s
debezium.source.provide.transaction.metadata=true
debezium.source.max.batch.size=10000
debezium.source.max.queue.size=50000
debezium.source.query.fetch.size=10000
`

// ref for snapshot boundary mode - https://debezium.zulipchat.com/#narrow/stream/348250-community-oracle/topic/Missing.20change.20events.20from.20in-flight.20transactions.3F
//   - https://aws.amazon.com/blogs/database/how-aws-dms-handles-open-transactions-when-starting-a-full-load-and-cdc-task/
var oracleLiveMigrationSrcConfigTemplate = `
debezium.source.internal.log.mining.transaction.snapshot.boundary.mode=all
`

var oracleSrcPDBConfigTemplate = `
debezium.source.database.pdb.name=%s
`

var oracleConfigTemplate = baseConfigTemplate +
	baseSrcConfigTemplate +
	oracleSrcConfigTemplate +
	baseSinkConfigTemplate

var mysqlSrcConfigTemplate = `
debezium.source.database.hostname=%s
debezium.source.database.port=%d
debezium.source.database.include.list=%s
debezium.source.database.server.id=%d
debezium.source.connector.class=io.debezium.connector.mysql.MySqlConnector

debezium.source.schema.history.internal=io.debezium.storage.file.history.FileSchemaHistory
debezium.source.schema.history.internal.file.filename=%s
debezium.source.include.schema.changes=false
`

var mysqlConfigTemplate = baseConfigTemplate +
	baseSrcConfigTemplate +
	mysqlSrcConfigTemplate +
	baseSinkConfigTemplate

var mysqlSSLConfigTemplate = `
debezium.source.database.ssl.mode=%s
`

var mysqlSSLKeyStoreConfigTemplate = `
debezium.source.database.ssl.keystore=%s
debezium.source.database.ssl.keystore.password=%s
`

var mysqlSSLTrustStoreConfigTemplate = `
debezium.source.database.ssl.truststore=%s
debezium.source.database.ssl.truststore.password=%s
`

var yugabyteSrcConfigTemplate = `
debezium.source.connector.class=io.debezium.connector.yugabytedb.YugabyteDBConnector
debezium.source.database.hostname=%s
debezium.source.database.port=%d
debezium.source.database.dbname=%s
debezium.source.database.streamid=%s
debezium.source.database.master.addresses=%s
debezium.source.schema.include.list=%s
debezium.source.hstore.handling.mode=map
debezium.source.decimal.handling.mode=precise
debezium.source.converters=postgres_source_converter
debezium.source.postgres_source_converter.type=io.debezium.server.ybexporter.PostgresToYbValueConverter
`

var yugabyteLogicalReplicationSrcConfigTemplate = `
debezium.source.connector.class=io.debezium.connector.postgresql.YugabyteDBConnector
debezium.source.database.hostname=%s
debezium.source.database.port=%d
debezium.source.database.dbname=%s
debezium.source.plugin.name=yboutput
debezium.source.schema.include.list=%s
debezium.source.hstore.handling.mode=map
debezium.source.decimal.handling.mode=string
debezium.source.converters=postgres_source_converter
debezium.source.postgres_source_converter.type=io.debezium.server.ybexporter.PostgresToYbValueConverter
`

var yugabyteLogicalReplicationSlotNameTemplate = `
debezium.source.slot.name=%s`

var yugabyteLogicalReplicationPublicationNameTemplate = `
debezium.source.publication.name=%s
`

var yugabyteSrcTransactionOrderingConfigTemplate = `
debezium.source.transaction.ordering=true
debezium.source.tasks.max=1
`
var yugabyteConfigTemplate = baseConfigTemplate +
	baseSrcConfigTemplate +
	yugabyteSrcConfigTemplate +
	baseSinkConfigTemplate

// If we want the logical replication connector we need to use yugabyteLogicalReplicationSrcConfigTemplate
var yugabyteLogicalReplicationConfigTemplate = baseConfigTemplate +
	baseSrcConfigTemplate +
	yugabyteLogicalReplicationSrcConfigTemplate +
	baseSinkConfigTemplate

var yugabyteSSLModeTemplate = `
debezium.source.database.sslmode=%s
`
var yugabyteSSLRootCertTemplate = `
debezium.source.database.sslrootcert=%s
`

func (c *Config) String() string {
	dataDir := filepath.Join(c.ExportDir, "data")
	offsetFileName := fmt.Sprintf("offsets.%s.dat", c.ExporterRole)
	offsetFile := filepath.Join(dataDir, offsetFileName)
	schemaNames := strings.Join(strings.Split(c.SchemaNames, "|"), ",")
	logMiningFlushTable := utils.GetLogMiningFlushTableName(c.MigrationUUID)
	// queuedSegmentMaxBytes := int641024 * 1024 * 1024 // 1GB
	queueSegmentMaxBytes, err := strconv.ParseInt(os.Getenv("QUEUE_SEGMENT_MAX_BYTES"), 10, 64)
	if err != nil {
		// defaults to 1GB
		log.Infof("QUEUE_SEGMENT_MAX_BYTES not set, defaulting to 1GB")
		queueSegmentMaxBytes = 1073741824
	} else {
		log.Infof("QUEUE_SEGMENT_MAX_BYTES: %d", queueSegmentMaxBytes)
	}

	quarkusLogPort, err := utils.GetFreePort()
	if err != nil {
		log.Warnf("failed to get a free port for quarkus http server, falling back to 8080: %v", err)
		quarkusLogPort = 8080
	}
	log.Infof("using port number %d for quarkus http server", quarkusLogPort)

	var conf string
	switch c.SourceDBType {
	case "postgresql":
		conf = fmt.Sprintf(postgresConfigTemplate,
			quarkusLogPort,
			c.LogLevel,
			c.Username,
			c.SnapshotMode,
			offsetFile,
			strings.Join(c.TableList, ","),

			c.Host, c.Port,
			c.DatabaseName,
			schemaNames,

			dataDir,
			c.ColumnSequenceMapping,
			c.InitSequenceMaxMapping,
			c.TableRenameMapping,
			queueSegmentMaxBytes,
			c.MetadataDBPath,
			c.RunId,
			c.ExporterRole)
		sslConf := fmt.Sprintf(postgresSSLConfigTemplate,
			c.SSLMode,
			c.SSLCertPath,
			c.SSLKey,
			c.SSLRootCert)
		conf = conf + sslConf
		if c.ReplicationSlotName != "" {
			conf = conf + fmt.Sprintf(postgresReplicationSlotNameTemplate, c.ReplicationSlotName)
		}
		if c.PublicationName != "" {
			conf = conf + fmt.Sprintf(postgresPublicationNameTemplate, c.PublicationName)
		}
	case "yugabytedb":
		if !c.UseYBgRPCConnector {
			conf = fmt.Sprintf(yugabyteLogicalReplicationConfigTemplate,
				quarkusLogPort,
				c.LogLevel,
				c.Username,
				"never",
				offsetFile,
				strings.Join(c.TableList, ","),

				c.Host, c.Port,
				c.DatabaseName,
				schemaNames,

				dataDir,
				c.ColumnSequenceMapping,
				c.InitSequenceMaxMapping,
				c.TableRenameMapping,
				queueSegmentMaxBytes,
				c.MetadataDBPath,
				c.RunId,
				c.ExporterRole)

			if c.ReplicationSlotName != "" {
				conf = conf + fmt.Sprintf(yugabyteLogicalReplicationSlotNameTemplate, c.ReplicationSlotName)
			}
			if c.PublicationName != "" {
				conf = conf + fmt.Sprintf(yugabyteLogicalReplicationPublicationNameTemplate, c.PublicationName)
			}
		} else {
			conf = fmt.Sprintf(yugabyteConfigTemplate,
				quarkusLogPort,
				c.LogLevel,
				c.Username,
				"never",
				offsetFile,
				strings.Join(c.TableList, ","),

				c.Host, c.Port,
				c.DatabaseName,
				c.YBStreamID,
				c.YBMasterNodes,
				schemaNames,

				dataDir,
				c.ColumnSequenceMapping,
				c.InitSequenceMaxMapping,
				c.TableRenameMapping,
				queueSegmentMaxBytes,
				c.MetadataDBPath,
				c.RunId,
				c.ExporterRole)
		}
		sslConf := fmt.Sprintf(yugabyteSSLModeTemplate, c.SSLMode)
		if c.SSLRootCert != "" {
			sslConf += fmt.Sprintf(yugabyteSSLRootCertTemplate, c.SSLRootCert)
		}
		conf = conf + sslConf
		//TODO test SSL for other methods for yugabytedb
		if c.TransactionOrdering {
			conf = conf + yugabyteSrcTransactionOrderingConfigTemplate
		}
	case "oracle":
		conf = fmt.Sprintf(oracleConfigTemplate,
			quarkusLogPort,
			c.LogLevel,
			c.Username,
			c.SnapshotMode,
			offsetFile,
			strings.Join(c.TableList, ","),

			c.Uri,
			schemaNames,
			filepath.Join(c.ExportDir, "data", "history.dat"),
			filepath.Join(c.ExportDir, "data", "schema_history.json"),
			logMiningFlushTable,

			dataDir,
			c.ColumnSequenceMapping,
			c.InitSequenceMaxMapping,
			c.TableRenameMapping,
			queueSegmentMaxBytes,
			c.MetadataDBPath,
			c.RunId,
			c.ExporterRole)
		if c.SnapshotMode == "initial" {
			conf = conf + oracleLiveMigrationSrcConfigTemplate
		}

		if c.PDBName != "" {
			// cdb setup.
			conf = conf + fmt.Sprintf(oracleSrcPDBConfigTemplate, c.PDBName)
		}

	case "mysql":
		conf = fmt.Sprintf(mysqlConfigTemplate,
			quarkusLogPort,
			c.LogLevel,
			c.Username,
			c.SnapshotMode,
			offsetFile,
			strings.Join(c.TableList, ","),

			c.Host, c.Port,
			c.DatabaseName,
			getDatabaseServerID(),
			filepath.Join(c.ExportDir, "data", "schema_history.json"),

			dataDir,
			c.ColumnSequenceMapping,
			c.InitSequenceMaxMapping,
			c.TableRenameMapping,
			queueSegmentMaxBytes,
			c.MetadataDBPath,
			c.RunId,
			c.ExporterRole)
		sslConf := fmt.Sprintf(mysqlSSLConfigTemplate, c.SSLMode)
		if c.SSLKeyStore != "" {
			sslConf += fmt.Sprintf(mysqlSSLKeyStoreConfigTemplate,
				c.SSLKeyStore,
				c.SSLKeyStorePassword)
		}
		if c.SSLTrustStore != "" {
			sslConf += fmt.Sprintf(mysqlSSLTrustStoreConfigTemplate,
				c.SSLTrustStore,
				c.SSLTrustStorePassword)
		}
		conf = conf + sslConf
	default:
		panic(fmt.Sprintf("unknown source db type %s", c.SourceDBType))
	}

	if c.ColumnList != nil {
		conf += fmt.Sprintf("\ndebezium.source.column.include.list=%s", strings.Join(c.ColumnList, ","))
	}

	return conf
}

func (c *Config) WriteToFile(filePath string) error {
	config := c.String()
	if c.Password == "" { //empty password have issues with Env variable https://yugabyte.atlassian.net/browse/DB-7533
		config += fmt.Sprintf("\ndebezium.source.database.password=%s", c.Password)
	}
	err := os.WriteFile(filePath, []byte(config), 0644)
	if err != nil {
		return fmt.Errorf("failed to write config file %s: %v", filePath, err)
	}
	return nil
}

func isTargetDBExporter(exporterRole string) bool {
	return exporterRole == TARGET_DB_EXPORTER_FB_ROLE || exporterRole == TARGET_DB_EXPORTER_FF_ROLE
}

// read config file DEBEZIUM_CONF_FILEPATH into a string
func readConfigFile() (string, error) {
	config, err := os.ReadFile(DEBEZIUM_CONF_FILEPATH)
	if err != nil {
		return "", fmt.Errorf("failed to read config file %s: %w", DEBEZIUM_CONF_FILEPATH, err)
	}

	return string(config), nil
}

// generate/fetch the value for 'debezium.source.database.server.id' property for MySQL
func getDatabaseServerID() int {
	databaseServerId := rand.Intn(math.MaxInt-10000) + 10000
	log.Infof("randomly generated database server id: %d", databaseServerId)
	config, err := readConfigFile()
	if err != nil {
		log.Errorf("failed to read config file: %v", err)
		return databaseServerId
	}

	// if config file exists, read the value of 'debezium.source.database.server.id' property
	if strings.Contains(config, "debezium.source.database.server.id") {
		re := regexp.MustCompile(`(?m)^debezium.source.database.server.id=(\d+)$`)
		matches := re.FindStringSubmatch(config)
		if len(matches) == 2 {
			databaseServerId, err = strconv.Atoi(matches[1])
			if err != nil {
				log.Errorf("failed to convert database server id to int: %v", err)
				return databaseServerId
			}
		}
	}
	log.Infof("final database server id: %d", databaseServerId)
	return databaseServerId
}
