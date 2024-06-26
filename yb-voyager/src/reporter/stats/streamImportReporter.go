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
package stats

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/gosuri/uilive"
	"github.com/samber/lo"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/metadb"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

type StreamImportStatsReporter struct {
	sync.Mutex
	migrationUUID            uuid.UUID
	importerRole             string
	TotalEventsImported      int64
	EventsImportRateLast3Min int64
	CurrImportedEvents       int64
	startTime                time.Time
	eventsSlidingWindow      [61]int64 // stores events per 10 secs for last 10 mins
	remainingEvents          int64
	estimatedTimeToCatchUp   time.Duration
	uitable                  *uilive.Writer
	metaDB                   *metadb.MetaDB
}

func NewStreamImportStatsReporter(importerRole string) *StreamImportStatsReporter {
	return &StreamImportStatsReporter{importerRole: importerRole}
}

func (s *StreamImportStatsReporter) Init(migrationUUID uuid.UUID, metaDB *metadb.MetaDB,
	numInserts, numUpdates, numDeletes int64) error {
	s.migrationUUID = migrationUUID
	s.TotalEventsImported = numInserts + numUpdates + numDeletes
	s.startTime = time.Now()
	s.metaDB = metaDB
	return nil
}

func (s *StreamImportStatsReporter) Finalize() {
	s.refreshStats()
	s.uitable.Stop()
}

var headerRow, seperator1, seperator2, seperator3, row1, row2, row3, row4, row5, row6, timerRow io.Writer

func (s *StreamImportStatsReporter) ReportStats(ctx context.Context) {
	displayTicker := time.NewTicker(10 * time.Second)
	defer displayTicker.Stop()
	s.uitable = uilive.New()
	headerRow = s.uitable.Newline()
	seperator1 = s.uitable.Newline()
	seperator2 = s.uitable.Newline()
	seperator3 = s.uitable.Newline()
	row1 = s.uitable.Newline()
	row2 = s.uitable.Newline()
	row3 = s.uitable.Newline()
	row4 = s.uitable.Newline()
	row5 = s.uitable.Newline()
	row6 = s.uitable.Newline()
	timerRow = s.uitable.Newline()

	s.uitable.Start()

	for range displayTicker.C {
		select {
		case <-ctx.Done():
			return
		default:
			s.refreshStats()
		}
	}
}

func (s *StreamImportStatsReporter) refreshStats() {
	elapsedTime := math.Round(time.Since(s.startTime).Minutes()*100) / 100
	s.slideWindow()
	s.UpdateRemainingEvents()
	fmt.Fprint(seperator1, color.GreenString("| %-30s | %30s |\n", "-----------------------------", "-----------------------------"))
	fmt.Fprint(headerRow, color.GreenString("| %-30s | %30s |\n", "Metric", "Value"))
	fmt.Fprint(seperator2, color.GreenString("| %-30s | %30s |\n", "-----------------------------", "-----------------------------"))
	fmt.Fprint(row1, color.GreenString("| %-30s | %30s |\n", "Total Imported events", strconv.FormatInt(s.TotalEventsImported, 10)))
	fmt.Fprint(row2, color.GreenString("| %-30s | %30s |\n", "Events Imported in this Run", strconv.FormatInt(s.CurrImportedEvents, 10)))
	var averageRateLast3Mins, averageRateLast10Mins int64
	if elapsedTime < 3 {
		averageRateLast3Mins = s.getIngestionRateForLastNMinutes(int64(elapsedTime) + 1)
	} else {
		averageRateLast3Mins = s.getIngestionRateForLastNMinutes(3)
	}
	if elapsedTime < 10 {
		averageRateLast10Mins = s.getIngestionRateForLastNMinutes(int64(elapsedTime) + 1)
	} else {
		averageRateLast10Mins = s.getIngestionRateForLastNMinutes(10)
	}
	fmt.Fprint(row3, color.GreenString("| %-30s | %30s |\n", "Ingestion Rate (last 3 mins)", fmt.Sprintf("%d events/sec", averageRateLast3Mins/60)))
	fmt.Fprint(row4, color.GreenString("| %-30s | %30s |\n", "Ingestion Rate (last 10 mins)", fmt.Sprintf("%d events/sec", averageRateLast10Mins/60)))
	fmt.Fprint(timerRow, color.GreenString("| %-30s | %30s |\n", "Time taken in this Run", fmt.Sprintf("%.2f mins", elapsedTime)))
	fmt.Fprint(row5, color.GreenString("| %-30s | %30s |\n", "Remaining Events", strconv.FormatInt(s.remainingEvents, 10)))
	fmt.Fprint(row6, color.GreenString("| %-30s | %30s |\n", "Estimated Time to catch up", s.estimatedTimeToCatchUp.String()))
	fmt.Fprint(seperator3, color.GreenString("| %-30s | %30s |\n", "-----------------------------", "-----------------------------"))
	s.uitable.Flush()
}

func (s *StreamImportStatsReporter) slideWindow() {
	s.Mutex.Lock()
	for i := len(s.eventsSlidingWindow) - 1; i > 0; i-- {
		s.eventsSlidingWindow[i] = s.eventsSlidingWindow[i-1]
	}
	s.eventsSlidingWindow[0] = 0
	s.Mutex.Unlock()
}

func (s *StreamImportStatsReporter) BatchImported(numInserts, numUpdates, numDeletes int64) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	total := numInserts + numUpdates + numDeletes
	s.CurrImportedEvents += total
	s.TotalEventsImported += total
	s.eventsSlidingWindow[0] += total
	s.EventsImportRateLast3Min = s.getIngestionRateForLastNMinutes(3) / 60 // this is just used for call-home stats
}

func (s *StreamImportStatsReporter) getIngestionRateForLastNMinutes(n int64) int64 {
	windowSize := 6*n + 1 //6*n as sliding window every 10 secs
	return lo.Sum(s.eventsSlidingWindow[1:windowSize]) / n
}

func (s *StreamImportStatsReporter) UpdateRemainingEvents() {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	var totalExportedEvents int64
	var err error
	if s.importerRole == "source_db_importer" {
		totalExportedEvents, _, err = s.metaDB.GetTotalExportedEventsByExporterRole("target_db_exporter_fb", time.Now().String())
		if err != nil {
			utils.ErrExit("failed to fetch exported events stats from meta db: %v", err)
		}
	} else {
		totalExportedEvents, _, err = s.metaDB.GetTotalExportedEvents(time.Now().String())
		if err != nil {
			utils.ErrExit("failed to fetch exported events stats from meta db: %v", err)
		}
	}
	s.remainingEvents = totalExportedEvents - s.TotalEventsImported
	lastMinIngestionRate := s.getIngestionRateForLastNMinutes(1)
	if lastMinIngestionRate > 0 {
		s.estimatedTimeToCatchUp = time.Duration(s.remainingEvents/lastMinIngestionRate) * time.Minute
	}
}
