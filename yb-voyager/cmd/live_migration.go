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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/callhome"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/metadb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/namereg"
	reporter "github.com/yugabyte/yb-voyager/yb-voyager/src/reporter/stats"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/tgtdb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

var NUM_EVENT_CHANNELS int
var EVENT_CHANNEL_SIZE int // has to be > MAX_EVENTS_PER_BATCH
var MAX_EVENTS_PER_BATCH int
var MAX_INTERVAL_BETWEEN_BATCHES int //ms
var END_OF_QUEUE_SEGMENT_EVENT = &tgtdb.Event{Op: "end_of_source_queue_segment"}
var FLUSH_BATCH_EVENT = &tgtdb.Event{Op: "flush_batch"}
var eventQueue *EventQueue
var statsReporter *reporter.StreamImportStatsReporter

func init() {
	NUM_EVENT_CHANNELS = utils.GetEnvAsInt("NUM_EVENT_CHANNELS", 100)
	EVENT_CHANNEL_SIZE = utils.GetEnvAsInt("EVENT_CHANNEL_SIZE", 500)
	MAX_EVENTS_PER_BATCH = utils.GetEnvAsInt("MAX_EVENTS_PER_BATCH", 500)
	MAX_INTERVAL_BETWEEN_BATCHES = utils.GetEnvAsInt("MAX_INTERVAL_BETWEEN_BATCHES", 2000)
}

func streamChanges(state *ImportDataState, tableNames []sqlname.NameTuple) error {
	log.Infof("NUM_EVENT_CHANNELS: %d, EVENT_CHANNEL_SIZE: %d, MAX_EVENTS_PER_BATCH: %d, MAX_INTERVAL_BETWEEN_BATCHES: %d",
		NUM_EVENT_CHANNELS, EVENT_CHANNEL_SIZE, MAX_EVENTS_PER_BATCH, MAX_INTERVAL_BETWEEN_BATCHES)
	// re-initilizing name registry in case it hadn't picked up the names registered on source/target/source-replica
	err := namereg.NameReg.Init()
	if err != nil {
		return fmt.Errorf("init name registry again: %v", err)
	}
	tdb.PrepareForStreaming()
	err = state.InitLiveMigrationState(migrationUUID, NUM_EVENT_CHANNELS, bool(startClean), tableNames)
	if err != nil {
		utils.ErrExit("Failed to init event channels metadata table on target DB: %s", err)
	}
	eventChannelsMetaInfo, err := state.GetEventChannelsMetaInfo(migrationUUID)
	if err != nil {
		return fmt.Errorf("failed to fetch event channel meta info from target : %w", err)
	}
	numInserts, numUpdates, numDeletes, err := state.GetTotalNumOfEventsImportedByType(migrationUUID)
	if err != nil {
		return fmt.Errorf("failed to fetch import stats meta by type: %w", err)
	}
	statsReporter = reporter.NewStreamImportStatsReporter(importerRole)
	err = statsReporter.Init(migrationUUID, metaDB, numInserts, numUpdates, numDeletes)
	if err != nil {
		return fmt.Errorf("failed to initialize stats reporter: %w", err)
	}

	if !disablePb {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go statsReporter.ReportStats(ctx)
		defer statsReporter.Finalize()
	}

	eventQueue = NewEventQueue(exportDir)
	// setup target event channels
	var evChans []chan *tgtdb.Event
	var processingDoneChans []chan bool
	for i := 0; i < NUM_EVENT_CHANNELS; i++ {
		evChans = append(evChans, make(chan *tgtdb.Event, EVENT_CHANNEL_SIZE))
		processingDoneChans = append(processingDoneChans, make(chan bool, 1))
	}

	log.Infof("streaming changes from %s", eventQueue.QueueDirPath)
	for !eventQueue.EndOfQueue { // continuously get next segments to stream
		segment, err := eventQueue.GetNextSegment()
		if err != nil {
			if segment == nil && (errors.Is(err, os.ErrNotExist) || errors.Is(err, sql.ErrNoRows)) {
				time.Sleep(2 * time.Second)
				continue
			}
			return fmt.Errorf("error getting next segment to stream: %v", err)
		}
		log.Infof("got next segment to stream: %v", segment)

		err = streamChangesFromSegment(segment, evChans, processingDoneChans, eventChannelsMetaInfo, statsReporter, state)
		if err != nil {
			return fmt.Errorf("error streaming changes for segment %s: %v", segment.FilePath, err)
		}
	}
	return nil
}

var prevExporterRole = ""

func streamChangesFromSegment(
	segment *EventQueueSegment,
	evChans []chan *tgtdb.Event,
	processingDoneChans []chan bool,
	eventChannelsMetaInfo map[int]EventChannelMetaInfo,
	statsReporter *reporter.StreamImportStatsReporter,
	state *ImportDataState) error {

	err := segment.Open()
	if err != nil {
		return err
	}
	defer segment.Close()

	// start target event channel processors
	for i := 0; i < NUM_EVENT_CHANNELS; i++ {
		var chanLastAppliedVsn int64
		chanMetaInfo, exists := eventChannelsMetaInfo[i]
		if exists {
			chanLastAppliedVsn = chanMetaInfo.LastAppliedVsn
		} else {
			return fmt.Errorf("unable to find channel meta info for channel - %v", i)
		}
		go processEvents(i, evChans[i], chanLastAppliedVsn, processingDoneChans[i], statsReporter, state)
	}

	log.Infof("streaming changes for segment %s", segment.FilePath)
	for !segment.IsProcessed() {
		event, err := segment.NextEvent()
		if err != nil {
			return err
		}

		if event == nil && segment.IsProcessed() {
			break
		}

		// segment switch and cutover(for example: source changed from PG to YB)
		if event != nil && prevExporterRole != event.ExporterRole {
			/*
				Note: `sourceDBType` is a global variable, which always represent the initial source db type
				which does not change even after cutover to target but for conflict detection cache,
				we need to use the actual source db type at the moment since we save information like
				TableToUniqueKeyColumns during export(from source/target) to reuse it during import
			*/
			sourceDBTypeForConflictCache := lo.Ternary(isTargetDBExporter(event.ExporterRole), "yugabytedb", sourceDBType)
			err = initializeConflictDetectionCache(evChans, event.ExporterRole, sourceDBTypeForConflictCache)
			if err != nil {
				return fmt.Errorf("error initializing conflict detection cache: %w", err)
			}
			prevExporterRole = event.ExporterRole
		}

		if event.IsCutoverToTarget() && importerRole == TARGET_DB_IMPORTER_ROLE ||
			event.IsCutoverToSourceReplica() && importerRole == SOURCE_REPLICA_DB_IMPORTER_ROLE ||
			event.IsCutoverToSource() && importerRole == SOURCE_DB_IMPORTER_ROLE { // cutover or fall-forward command

			updateCallhomeImportPhase(event)

			eventQueue.EndOfQueue = true
			segment.MarkProcessed()
			break
		}

		err = handleEvent(event, evChans)
		if err != nil {
			return fmt.Errorf("error handling event: %v", err)
		}
	}

	for i := 0; i < NUM_EVENT_CHANNELS; i++ {
		evChans[i] <- END_OF_QUEUE_SEGMENT_EVENT
	}

	for i := 0; i < NUM_EVENT_CHANNELS; i++ {
		<-processingDoneChans[i]
	}

	err = metaDB.MarkEventQueueSegmentAsProcessed(segment.SegmentNum, importerRole)
	if err != nil {
		return fmt.Errorf("error marking segment %s as processed: %v", segment.FilePath, err)
	}
	log.Infof("finished streaming changes from segment %s\n", filepath.Base(segment.FilePath))
	return nil
}

func updateCallhomeImportPhase(event *tgtdb.Event) {
	if !callhome.SendDiagnostics {
		return
	}
	switch true {
	case event.IsCutoverToTarget() && importerRole == TARGET_DB_IMPORTER_ROLE:
		importPhase = CUTOVER_TO_TARGET
	case event.IsCutoverToSourceReplica() && importerRole == SOURCE_REPLICA_DB_IMPORTER_ROLE:
		importPhase = CUTOVER_TO_SOURCE_REPLICA
	case event.IsCutoverToSource() && importerRole == SOURCE_DB_IMPORTER_ROLE:
		importPhase = CUTOVER_TO_SOURCE
	}

}

func shouldFormatValues(event *tgtdb.Event) bool {
	switch tconf.TargetDBType {
	case YUGABYTEDB, POSTGRESQL:
		return event.Op == "u"
	case ORACLE:
		return true
	}
	return false
}
func handleEvent(event *tgtdb.Event, evChans []chan *tgtdb.Event) error {
	if event.IsCutoverEvent() {
		// nil in case of cutover or fall_forward events for unconcerned importer
		return nil
	}
	log.Debugf("handling event: %v", event)

	// hash event
	// Note: hash the event before running the keys/values through the value converter.
	// This is because the value converter can generate different values (formatting vs no formatting) for the same key
	// which will affect hash value.
	h := hashEvent(event)

	/*
		Checking for all possible conflicts among events
		For more details about ConflictDetectionCache see the comment on line 11 in [conflictDetectionCache.go](../conflictDetectionCache.go)
	*/
	uniqueKeyCols, _ := conflictDetectionCache.tableToUniqueKeyColumns.Get(event.TableNameTup)
	if len(uniqueKeyCols) > 0 {
		if event.Op == "d" {
			conflictDetectionCache.Put(event)
		} else { // "i" or "u"
			conflictDetectionCache.WaitUntilNoConflict(event)
			if event.IsUniqueKeyChanged(uniqueKeyCols) {
				conflictDetectionCache.Put(event)
			}
		}
	}

	// preparing value converters for the streaming mode
	err := valueConverter.ConvertEvent(event, event.TableNameTup, shouldFormatValues(event))
	if err != nil {
		return fmt.Errorf("error transforming event key fields: %v", err)
	}

	evChans[h] <- event
	log.Tracef("inserted event %v into channel %v", event.Vsn, h)
	return nil
}

// Returns a hash value between 0..NUM_EVENT_CHANNELS
func hashEvent(e *tgtdb.Event) int {
	hash := fnv.New64a()
	hash.Write([]byte(e.TableNameTup.ForKey()))

	keyColumns := make([]string, 0)
	for k := range e.Key {
		keyColumns = append(keyColumns, k)
	}

	// sort to ensure input to hash is consistent.
	sort.Strings(keyColumns)
	for _, k := range keyColumns {
		hash.Write([]byte(*e.Key[k]))
	}
	return int(hash.Sum64() % (uint64(NUM_EVENT_CHANNELS)))
}

func processEvents(chanNo int, evChan chan *tgtdb.Event, lastAppliedVsn int64, done chan bool, statsReporter *reporter.StreamImportStatsReporter, state *ImportDataState) {
	endOfProcessing := false
	for !endOfProcessing {
		batch := []*tgtdb.Event{}
		timer := time.NewTimer(time.Duration(MAX_INTERVAL_BETWEEN_BATCHES) * time.Millisecond)
	Batching:
		for {
			// read from channel until MAX_EVENTS_PER_BATCH or MAX_INTERVAL_BETWEEN_BATCHES
			select {
			case event := <-evChan:
				if event == END_OF_QUEUE_SEGMENT_EVENT {
					endOfProcessing = true
					break Batching
				}
				if event == FLUSH_BATCH_EVENT {
					break Batching
				}
				if event.Vsn <= lastAppliedVsn {
					log.Tracef("ignoring event %v because event vsn <= %v", event, lastAppliedVsn)
					continue
				}
				if importerRole == SOURCE_DB_IMPORTER_ROLE && event.ExporterRole != TARGET_DB_EXPORTER_FB_ROLE {
					log.Tracef("ignoring event %v because importer role is FB_DB_IMPORTER_ROLE and event exporter role is not TARGET_DB_EXPORTER_FB_ROLE.", event)
					continue
				}
				batch = append(batch, event)
				if len(batch) >= MAX_EVENTS_PER_BATCH {
					break Batching
				}
			case <-timer.C:
				break Batching
			}
		}
		timer.Stop()

		if len(batch) == 0 {
			continue
		}

		start := time.Now()
		eventBatch := tgtdb.NewEventBatch(batch, chanNo)
		var err error
		sleepIntervalSec := 0
		for attempt := 0; attempt < EVENT_BATCH_MAX_RETRY_COUNT; attempt++ {
			err = tdb.ExecuteBatch(migrationUUID, eventBatch)
			if err == nil {
				conflictDetectionCache.RemoveEvents(eventBatch)
				break
			} else if tdb.IsNonRetryableCopyError(err) {
				break
			}
			log.Warnf("retriable error executing batch(%s) on channel %v (last VSN: %d): %v", eventBatch.ID(), chanNo, eventBatch.GetLastVsn(), err)
			sleepIntervalSec += 10
			if sleepIntervalSec > MAX_SLEEP_SECOND {
				sleepIntervalSec = MAX_SLEEP_SECOND
			}
			log.Infof("sleep for %d seconds before retrying the batch on channel %v (attempt %d)",
				sleepIntervalSec, chanNo, attempt)
			time.Sleep(time.Duration(sleepIntervalSec) * time.Second)

			// In certain situations, we get an error on `targetDB.ExecuteBatch`, but eventually the transaction is committed.
			// For example, in Yugabyte, we can get an `rpc timeout` on commit, and the commit eventually succeeds on YB server.
			// Retrying an already executed batch has consequences:
			// - It can fail with some duplicate / unique key constraint errors
			// - Stats will double count the events.
			// Therefore, we check if batch has already been imported before retrying.
			alreadyImported, aerr := checkifEventBatchAlreadyImported(state, eventBatch, migrationUUID)
			if aerr != nil {
				utils.ErrExit("error checking if event batch channel %d (last VSN: %d) already imported: %v", chanNo, eventBatch.GetLastVsn(), aerr)
			}
			if alreadyImported {
				log.Infof("batch on channel %d (last VSN: %d) already imported", chanNo, eventBatch.GetLastVsn())
				err = nil
				break
			}
		}
		if err != nil {
			utils.ErrExit("error executing batch on channel %v: %v", chanNo, err)
		}
		statsReporter.BatchImported(eventBatch.EventCounts.NumInserts, eventBatch.EventCounts.NumUpdates, eventBatch.EventCounts.NumDeletes)
		log.Debugf("processEvents from channel %v: Executed Batch of size - %d successfully in time %s",
			chanNo, len(batch), time.Since(start).String())
	}
	done <- true
}

func initializeConflictDetectionCache(evChans []chan *tgtdb.Event, exporterRole string, sourceDBTypeForConflictCache string) error {
	tableToUniqueKeyColumns, err := getTableToUniqueKeyColumnsMapFromMetaDB(exporterRole)
	if err != nil {
		return fmt.Errorf("get table unique key columns map: %w", err)
	}
	log.Infof("initializing conflict detection cache")
	conflictDetectionCache = NewConflictDetectionCache(tableToUniqueKeyColumns, evChans, sourceDBTypeForConflictCache)
	return nil
}

func getTableToUniqueKeyColumnsMapFromMetaDB(exporterRole string) (*utils.StructMap[sqlname.NameTuple, []string], error) {
	log.Infof("fetching table to unique key columns map from metaDB")
	var metaDbData map[string][]string
	res := utils.NewStructMap[sqlname.NameTuple, []string]()

	key := fmt.Sprintf("%s_%s", metadb.TABLE_TO_UNIQUE_KEY_COLUMNS_KEY, exporterRole)
	found, err := metaDB.GetJsonObject(nil, key, &metaDbData)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("table to unique key columns map not found in metaDB")
	}
	log.Infof("fetched table to unique key columns map: %v", metaDbData)

	for tableNameRaw, columns := range metaDbData {
		tableName, err := namereg.NameReg.LookupTableName(tableNameRaw)
		if err != nil {
			return nil, fmt.Errorf("lookup table %s in name registry: %v", tableNameRaw, err)
		}
		res.Put(tableName, columns)
	}
	return res, nil
}

func checkifEventBatchAlreadyImported(state *ImportDataState, eventBatch *tgtdb.EventBatch, migrationUUID uuid.UUID) (bool, error) {
	var res bool
	var err error
	sleepIntervalSec := 0
	for attempt := 0; attempt < EVENT_BATCH_MAX_RETRY_COUNT; attempt++ {
		res, err = state.IsEventBatchAlreadyImported(eventBatch, migrationUUID)
		if err == nil {
			break
		} else if tdb.IsNonRetryableCopyError(err) {
			break
		}
		sleepIntervalSec += 10
		if sleepIntervalSec > MAX_SLEEP_SECOND {
			sleepIntervalSec = MAX_SLEEP_SECOND
		}
		log.Infof("sleep for %d seconds before retrying to check if event batch (last vsn: %d) already imported (attempt %d)",
			sleepIntervalSec, eventBatch.GetLastVsn(), attempt)
		time.Sleep(time.Duration(sleepIntervalSec) * time.Second)
	}
	return res, err
}
