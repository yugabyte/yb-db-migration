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
package yugabyted

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
	controlPlane "github.com/yugabyte/yb-voyager/yb-voyager/src/cp"
)

type YugabyteD struct {
	sync.Mutex
	migrationDirectory       string
	waitGroup                sync.WaitGroup
	eventChan                chan (MigrationEvent)
	rowCountUpdateEventChan  chan ([]VisualizerTableMetrics)
	connPool                 *pgxpool.Pool
	lastRowCountUpdate       map[string]time.Time
	latestInvocationSequence int
}

func New(exportDir string) *YugabyteD {
	return &YugabyteD{migrationDirectory: exportDir}
}

// Initialize the yugabyted DB for visualisation metadata
func (cp *YugabyteD) Init() error {

	cp.eventChan = make(chan MigrationEvent, 100)
	cp.rowCountUpdateEventChan = make(chan []VisualizerTableMetrics, 200)

	err := cp.connect()
	if err != nil {
		return err
	}

	err = cp.setupDatabase()
	if err != nil {
		return err
	}

	cp.lastRowCountUpdate = make(map[string]time.Time)
	cp.latestInvocationSequence = 0

	go cp.eventPublisher()
	go cp.rowCountUpdateEventPublisher()

	return nil
}

// Wait for events to publish and Close the connection.
func (cp *YugabyteD) Finalize() {
	cp.waitGroup.Wait()
	cp.disconnect()
}

func (cp *YugabyteD) eventPublisher() {
	defer cp.panicHandler()
	for {
		event := <-cp.eventChan
		err := cp.sendMigrationEvent(event)
		if err != nil {
			log.Warnf("Couldn't send metadata for visualization. %s", err)
		}
		cp.waitGroup.Done()
	}
}

func (cp *YugabyteD) rowCountUpdateEventPublisher() {
	defer cp.panicHandler()
	for {
		event := <-cp.rowCountUpdateEventChan
		err := cp.sendVisualizerTableMetrics(event)
		if err != nil {
			log.Warnf("Couldn't send metadata for visualization. %s", err)
		}
		cp.waitGroup.Done()
	}
}

func (cp *YugabyteD) createAndSendEvent(event *controlPlane.BaseEvent, status string, payload string) {

	timestamp := time.Now().Format("2006-01-02 15:04:05")

	invocationSequence, err := cp.getInvocationSequence(event.MigrationUUID,
		MIGRATION_PHASE_MAP[event.EventType])

	if err != nil {
		log.Warnf("Cannot send metadata for visualization. %s", err)
		return
	}

	migrationEvent := MigrationEvent{
		MigrationUUID:       event.MigrationUUID,
		MigrationPhase:      MIGRATION_PHASE_MAP[event.EventType],
		InvocationSequence:  invocationSequence,
		DatabaseName:        event.DatabaseName,
		SchemaName:          strings.Join(event.SchemaNames[:], "|"),
		Payload:             payload,
		DBType:              event.DBType,
		Status:              status,
		InvocationTimestamp: timestamp,
	}

	select {
	case cp.eventChan <- migrationEvent:
		cp.waitGroup.Add(1)
	default:
		log.Warnf("Could not publish migration event %v", migrationEvent)
	}
}

func (cp *YugabyteD) createAndSendUpdateRowCountEvent(events []*controlPlane.BaseUpdateRowCountEvent) {

	timestamp := time.Now().Format("2006-01-02 15:04:05")

	var rowCountUpdateEvent []VisualizerTableMetrics

	for _, event := range events {
		snapshotMigrateTableMetrics := VisualizerTableMetrics{
			MigrationUUID:       event.MigrationUUID,
			TableName:           event.TableName,
			Schema:              strings.Join(event.SchemaNames[:], "|"),
			MigrationPhase:      MIGRATION_PHASE_MAP[event.EventType],
			Status:              UPDATE_ROW_COUNT_STATUS_STR_TO_INT[event.Status],
			CountLiveRows:       event.CompletedRowCount,
			CountTotalRows:      event.TotalRowCount,
			InvocationTimestamp: timestamp,
		}

		rowCountUpdateEvent = append(rowCountUpdateEvent,
			snapshotMigrateTableMetrics)
	}

	select {
	case cp.rowCountUpdateEventChan <- rowCountUpdateEvent:
		cp.waitGroup.Add(1)
	default:
		log.Warnf("Could not publish row count update event %v", rowCountUpdateEvent)
	}
}

func (cp *YugabyteD) MigrationAssessmentStarted(ev *controlPlane.MigrationAssessmentStartedEvent) {
	cp.createAndSendEvent(&ev.BaseEvent, "IN PROGRESS", "")
}

func (cp *YugabyteD) MigrationAssessmentCompleted(ev *controlPlane.MigrationAssessmentCompletedEvent) {
	cp.createAndSendEvent(&ev.BaseEvent, "COMPLETED", ev.Report)
}

func (cp *YugabyteD) ExportSchemaStarted(exportSchemaEvent *controlPlane.ExportSchemaStartedEvent) {
	cp.createAndSendEvent(&exportSchemaEvent.BaseEvent, "IN PROGRESS", "")
}

func (cp *YugabyteD) ExportSchemaCompleted(exportSchemaEvent *controlPlane.ExportSchemaCompletedEvent) {
	cp.createAndSendEvent(&exportSchemaEvent.BaseEvent, "COMPLETED", "")
}

func (cp *YugabyteD) SchemaAnalysisStarted(schemaAnalysisEvent *controlPlane.SchemaAnalysisStartedEvent) {
	cp.createAndSendEvent(&schemaAnalysisEvent.BaseEvent, "IN PROGRESS", "")
}

func (cp *YugabyteD) SchemaAnalysisIterationCompleted(schemaAnalysisReport *controlPlane.SchemaAnalysisIterationCompletedEvent) {

	jsonBytes, err := json.Marshal(schemaAnalysisReport.AnalysisReport)
	if err != nil {
		log.Warnf("%v", err)
		return
	}
	payload := string(jsonBytes)

	cp.createAndSendEvent(&schemaAnalysisReport.BaseEvent, "COMPLETED", payload)
}

func (cp *YugabyteD) SnapshotExportStarted(snapshotExportEvent *controlPlane.SnapshotExportStartedEvent) {
	cp.createAndSendEvent(&snapshotExportEvent.BaseEvent, "IN PROGRESS", "")
}

func (cp *YugabyteD) SnapshotExportCompleted(snapshotExportEvent *controlPlane.SnapshotExportCompletedEvent) {
	cp.createAndSendEvent(&snapshotExportEvent.BaseEvent, "COMPLETED", "")
}

func (cp *YugabyteD) UpdateExportedRowCount(
	snapshotExportTablesMetrics []*controlPlane.UpdateExportedRowCountEvent) {

	cp.Mutex.Lock()
	defer cp.Mutex.Unlock()

	var updateExportedRowCountEvents []*controlPlane.BaseUpdateRowCountEvent
	for _, updateExportedRowCountEvent := range snapshotExportTablesMetrics {

		lastRowCountUpdateTime, check := cp.lastRowCountUpdate[updateExportedRowCountEvent.TableName]

		if updateExportedRowCountEvent.Status == "COMPLETED" {
			updateExportedRowCountEvents = append(updateExportedRowCountEvents, &updateExportedRowCountEvent.BaseUpdateRowCountEvent)
		} else if !check {
			cp.lastRowCountUpdate[updateExportedRowCountEvent.TableName] = time.Now()

			updateExportedRowCountEvents = append(updateExportedRowCountEvents, &updateExportedRowCountEvent.BaseUpdateRowCountEvent)
		} else if !lastRowCountUpdateTime.Add(time.Second * 5).After(time.Now()) {
			cp.lastRowCountUpdate[updateExportedRowCountEvent.TableName] = time.Now()

			updateExportedRowCountEvents = append(updateExportedRowCountEvents, &updateExportedRowCountEvent.BaseUpdateRowCountEvent)
		}

	}
	if len(updateExportedRowCountEvents) > 0 {
		cp.createAndSendUpdateRowCountEvent(updateExportedRowCountEvents)
	}
}

func (cp *YugabyteD) ImportSchemaStarted(importSchemaEvent *controlPlane.ImportSchemaStartedEvent) {
	cp.createAndSendEvent(&importSchemaEvent.BaseEvent, "IN PROGRESS", "")
}

func (cp *YugabyteD) ImportSchemaCompleted(importSchemaEvent *controlPlane.ImportSchemaCompletedEvent) {
	cp.createAndSendEvent(&importSchemaEvent.BaseEvent, "COMPLETED", "")
}

func (cp *YugabyteD) SnapshotImportStarted(snapshotImportEvent *controlPlane.SnapshotImportStartedEvent) {
	cp.createAndSendEvent(&snapshotImportEvent.BaseEvent, "IN PROGRESS", "")
}

func (cp *YugabyteD) SnapshotImportCompleted(snapshotImportEvent *controlPlane.SnapshotImportCompletedEvent) {
	cp.createAndSendEvent(&snapshotImportEvent.BaseEvent, "COMPLETED", "")
}

func (cp *YugabyteD) UpdateImportedRowCount(
	snapshotImportTableMetrics []*controlPlane.UpdateImportedRowCountEvent) {

	cp.Mutex.Lock()
	defer cp.Mutex.Unlock()

	var updateImportedRowCountEvents []*controlPlane.BaseUpdateRowCountEvent
	for _, updateImportedRowCountEvent := range snapshotImportTableMetrics {

		lastRowCountUpdateTime, check := cp.lastRowCountUpdate[updateImportedRowCountEvent.TableName]

		if updateImportedRowCountEvent.Status == "COMPLETED" {
			updateImportedRowCountEvents = append(updateImportedRowCountEvents, &updateImportedRowCountEvent.BaseUpdateRowCountEvent)
		} else if !check {
			cp.lastRowCountUpdate[updateImportedRowCountEvent.TableName] = time.Now()
			updateImportedRowCountEvents = append(updateImportedRowCountEvents, &updateImportedRowCountEvent.BaseUpdateRowCountEvent)
		} else if !lastRowCountUpdateTime.Add(time.Second * 5).After(time.Now()) {
			cp.lastRowCountUpdate[updateImportedRowCountEvent.TableName] = time.Now()
			updateImportedRowCountEvents = append(updateImportedRowCountEvents, &updateImportedRowCountEvent.BaseUpdateRowCountEvent)
		}

	}
	if len(updateImportedRowCountEvents) > 0 {
		cp.createAndSendUpdateRowCountEvent(updateImportedRowCountEvents)
	}
}

func (cp *YugabyteD) MigrationEnded(migrationEndedEvent *controlPlane.MigrationEndedEvent) {
}

func (cp *YugabyteD) panicHandler() {
	if r := recover(); r != nil {
		// Handle the panic for eventPublishers
		log.Errorf(fmt.Sprintf("Panic occurred: %v. No further events will be published to YugabyteD DB.\n"+
			"Stack trace of panic location:\n%s", r, string(debug.Stack())))
	}
}

func (cp *YugabyteD) getConnPool() (*pgxpool.Pool, error) {
	var err error
	err = nil
	if cp.connPool == nil {
		log.Warnf("No Connections to YugabyteD DB found. Creating a connection pool...")
		err = cp.connect()
	}

	return cp.connPool, err
}

func (cp *YugabyteD) reconnect() error {
	var err error
	cp.disconnect()
	for attempt := 1; attempt < 5; attempt++ {
		err = cp.connect()
		if err == nil {
			return nil
		}
		log.Infof("Failed to reconnect to the yugabyted database: %s", err)
		time.Sleep(time.Second)
	}
	return fmt.Errorf("reconnect to yugabyted db: %w", err)
}

func (cp *YugabyteD) connect() error {
	if cp.connPool != nil {
		return nil
	}
	connectionUri := os.Getenv("YUGABYTED_DB_CONN_STRING")

	connPool, err := pgxpool.Connect(context.Background(), connectionUri)
	if err != nil {
		return fmt.Errorf("error while connecting to yugabyted db. error: %w", err)
	}

	cp.connPool = connPool
	return nil
}

func (cp *YugabyteD) disconnect() {
	if cp.connPool == nil {
		return
	}

	cp.connPool.Close()
	cp.connPool = nil
}

const VISUALIZER_METADATA_SCHEMA = "ybvoyager_visualizer"

// Set-up YBD database for visualisation metadata
func (cp *YugabyteD) setupDatabase() error {
	err := cp.createVoyagerSchema()
	if err != nil {
		return err
	}

	err = cp.createYugabytedMetadataTable()
	if err != nil {
		return err
	}

	err = cp.createYugabytedTableMetricsTable()
	if err != nil {
		return err
	}

	return nil
}

// Create the visualisation schema
func (cp *YugabyteD) createVoyagerSchema() error {
	cmd := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, VISUALIZER_METADATA_SCHEMA)

	return cp.executeCmdOnTarget(cmd)
}

const YUGABYTED_METADATA_TABLE_NAME = VISUALIZER_METADATA_SCHEMA + "." + "ybvoyager_visualizer_metadata"

// Create visualisation metadata table
func (cp *YugabyteD) createYugabytedMetadataTable() error {
	cmd := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			migration_uuid UUID,
			migration_phase INT,
			invocation_sequence INT,
			migration_dir VARCHAR(250),
			database_name VARCHAR(250),
			schema_name VARCHAR(250),
			payload TEXT,
			complexity VARCHAR(30),
			db_type VARCHAR(30),
			status VARCHAR(30),
			invocation_timestamp TIMESTAMPTZ,
			PRIMARY KEY (migration_uuid, migration_phase, invocation_sequence)
			);`, YUGABYTED_METADATA_TABLE_NAME)

	return cp.executeCmdOnTarget(cmd)
}

const YUGABYTED_TABLE_METRICS_TABLE_NAME = VISUALIZER_METADATA_SCHEMA + "." + "ybvoyager_visualizer_table_metrics"

// Create table metrics table
func (cp *YugabyteD) createYugabytedTableMetricsTable() error {
	cmd := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			migration_uuid UUID,
			table_name VARCHAR(250),
			schema_name VARCHAR(250),
			migration_phase INT,
			status INT,
			count_live_rows INT,
			count_total_rows INT,
			invocation_timestamp TIMESTAMPTZ,
			PRIMARY KEY (migration_uuid, table_name, migration_phase, schema_name)
			);`, YUGABYTED_TABLE_METRICS_TABLE_NAME)

	return cp.executeCmdOnTarget(cmd)
}

// Get the latest invocation sequence for a given migration_uuid and migration phase
func (cp *YugabyteD) getInvocationSequence(mUUID uuid.UUID, phase int) (int, error) {

	// Increment and return invocation sequence number if already initialised.
	if cp.latestInvocationSequence > 0 {
		cp.latestInvocationSequence += 1
		return cp.latestInvocationSequence, nil
	}

	cmd := fmt.Sprintf(`SELECT MAX(invocation_sequence) AS latest_sequence 
		FROM %s 
		WHERE migration_uuid = '%s' AND migration_phase = %d`, YUGABYTED_METADATA_TABLE_NAME,
		mUUID, phase)

	log.Infof("Executing on yugabyted DB: [%s]", cmd)

	var latestSequence sql.NullInt32
	connPool, err := cp.getConnPool()
	if err != nil {
		return 0, err
	}

	err = connPool.QueryRow(context.Background(), cmd).Scan(&latestSequence)
	if err != nil {
		if err == pgx.ErrNoRows {
			cp.latestInvocationSequence = 1
			return cp.latestInvocationSequence, nil
		} else {
			return 0, fmt.Errorf("couldn't get the latest sequence number: %w", err)
		}
	}

	if latestSequence.Valid {
		cp.latestInvocationSequence = int(latestSequence.Int32) + 1
		return cp.latestInvocationSequence, nil
	} else {
		cp.latestInvocationSequence = 1
		return cp.latestInvocationSequence, nil
	}
}

// Send visualisation metadata
func (cp *YugabyteD) sendMigrationEvent(
	migrationEvent MigrationEvent) error {
	cmd := fmt.Sprintf("INSERT INTO %s ("+
		"migration_uuid, "+
		"migration_phase, "+
		"invocation_sequence, "+
		"migration_dir, "+
		"database_name, "+
		"schema_name, "+
		"payload, "+
		"db_type, "+
		"status, "+
		"invocation_timestamp"+
		") VALUES ("+
		"$1, $2, $3, $4, $5, $6, $7, $8, $9, $10"+
		")", YUGABYTED_METADATA_TABLE_NAME)

	var maxAttempts = 5
	migrationEvent.MigrationDirectory = cp.migrationDirectory

	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = cp.executeInsertQuery(cmd, migrationEvent)
		if err == nil {
			break
		} else {
			if attempt == maxAttempts {
				return fmt.Errorf("error while sending migration event data to yugabyted for %d max attempts"+
					" query: %s. migration event data: %v. error: %w", maxAttempts, cmd, migrationEvent, err)
			}
		}

		timestamp := time.Now().Format("2006-01-02 15:04:05")
		migrationEvent.InvocationTimestamp = timestamp
	}

	return nil
}

// Send Table metrics
func (cp *YugabyteD) sendVisualizerTableMetrics(
	visualizerTableMetricsList []VisualizerTableMetrics) error {

	cmd := fmt.Sprintf("INSERT INTO %s ("+
		"migration_uuid, "+
		"table_name, "+
		"schema_name,"+
		"migration_phase, "+
		"status, "+
		"count_live_rows, "+
		"count_total_rows, "+
		"invocation_timestamp"+
		") VALUES ", YUGABYTED_TABLE_METRICS_TABLE_NAME)

	var valuesList []string
	for _, metrics := range visualizerTableMetricsList {
		value := fmt.Sprintf("('%s', '%s', '%s', %d, %d, %d, %d, '%s')",
			metrics.MigrationUUID,
			metrics.TableName,
			metrics.Schema,
			metrics.MigrationPhase,
			metrics.Status,
			metrics.CountLiveRows,
			metrics.CountTotalRows,
			metrics.InvocationTimestamp)

		valuesList = append(valuesList, value)
	}

	cmd += strings.Join(valuesList, ",")

	cmd += fmt.Sprintf(" ON CONFLICT (migration_uuid, table_name, schema_name, migration_phase) " +
		"DO UPDATE " +
		"SET " +
		"status = EXCLUDED.status," +
		"count_live_rows = EXCLUDED.count_live_rows," +
		"count_total_rows = EXCLUDED.count_total_rows," +
		"invocation_timestamp = EXCLUDED.invocation_timestamp;")

	return cp.executeCmdOnTarget(cmd)
}

func (cp *YugabyteD) executeInsertQuery(cmd string,
	migrationEvent MigrationEvent) error {

	cp.Mutex.Lock()
	defer cp.Mutex.Unlock()

	var err error

	log.Infof("Executing on yugabyted DB: [%s] for [%+v]", cmd, migrationEvent)
	connPool, err := cp.getConnPool()
	if err != nil {
		return err
	}

	_, err = connPool.Exec(context.Background(), cmd,
		migrationEvent.MigrationUUID,
		migrationEvent.MigrationPhase,
		migrationEvent.InvocationSequence,
		migrationEvent.MigrationDirectory,
		migrationEvent.DatabaseName,
		migrationEvent.SchemaName,
		migrationEvent.Payload,
		migrationEvent.DBType,
		migrationEvent.Status,
		migrationEvent.InvocationTimestamp)

	if err == nil {
		return nil
	}
	log.Warnf("Error while running [%s]: %s", cmd, err)
	time.Sleep(time.Second)
	err2 := cp.reconnect()
	if err2 != nil {
		log.Warnf("Failed to reconnect to the yugabyted database: %s", err2)
	}

	if err != nil {
		return fmt.Errorf("couldn't excute command %s on yugabyted db. error: %w", cmd, err)
	}

	return nil
}

// Function to execute any cmd on yugabyted DB.
func (cp *YugabyteD) executeCmdOnTarget(cmd string) error {
	maxAttempts := 5
	var err error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		log.Infof("Executing on YugabyteD DB: [%s]", cmd)
		connPool, err := cp.getConnPool()
		if err != nil {
			return err
		}

		_, err = connPool.Exec(context.Background(), cmd)
		if err == nil {
			return nil
		}
		log.Warnf("Error while running [%s] attempt %d: %s", cmd, attempt, err)
		time.Sleep(time.Second)
		err2 := cp.reconnect()
		if err2 != nil {
			log.Warnf("Failed to reconnect to the yugabyted database: %s", err2)
			break
		}
	}
	if err != nil {
		return fmt.Errorf("couldn't excute command %s on yugabyted db. error: %w", cmd, err)
	}
	return nil
}
