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
package tgtdb

import (
	"fmt"
	"strings"
	"sync"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/namereg"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/sqlname"
)

type Event struct {
	Vsn          int64 // Voyager Sequence Number
	Op           string
	TableNameTup sqlname.NameTuple
	Key          map[string]*string
	Fields       map[string]*string
	BeforeFields map[string]*string
	ExporterRole string
}

func (e *Event) UnmarshalJSON(data []byte) error {
	if string(data) == "null" || string(data) == `""` {
		return nil
	}
	var err error
	// This is how this json really looks like.
	var rawEvent struct {
		Vsn          int64              `json:"vsn"` // Voyager Sequence Number
		Op           string             `json:"op"`
		SchemaName   string             `json:"schema_name"`
		TableName    string             `json:"table_name"`
		Key          map[string]*string `json:"key"`
		Fields       map[string]*string `json:"fields"`
		BeforeFields map[string]*string `json:"before_fields"`
		ExporterRole string             `json:"exporter_role"`
	}

	if err = json.Unmarshal(data, &rawEvent); err != nil {
		return err
	}
	e.Vsn = rawEvent.Vsn
	e.Op = rawEvent.Op
	e.Key = rawEvent.Key
	e.Fields = rawEvent.Fields
	e.BeforeFields = rawEvent.BeforeFields
	e.ExporterRole = rawEvent.ExporterRole
	if !e.IsCutoverEvent() {
		e.TableNameTup, err = namereg.NameReg.LookupTableName(fmt.Sprintf("%s.%s", rawEvent.SchemaName, rawEvent.TableName))
		if err != nil {
			return fmt.Errorf("lookup table %s.%s in name registry: %w", rawEvent.SchemaName, rawEvent.TableName, err)
		}
	}

	return nil
}

var cachePreparedStmt = sync.Map{}

func (e *Event) String() string {
	// Helper function to print a map[string]*string
	mapStr := func(m map[string]*string) string {
		var elements []string
		for key, value := range m {
			if value != nil {
				elements = append(elements, fmt.Sprintf("%s:%s", key, *value))
			} else {
				elements = append(elements, fmt.Sprintf("%s:<nil>", key))
			}
		}
		return "{" + strings.Join(elements, ", ") + "}"
	}

	return fmt.Sprintf("Event{vsn=%v, op=%v, table=%v, key=%v, before_fields=%v, fields=%v, exporter_role=%v}",
		e.Vsn, e.Op, e.TableNameTup, mapStr(e.Key), mapStr(e.BeforeFields), mapStr(e.Fields), e.ExporterRole)
}

func (e *Event) Copy() *Event {
	idFn := func(k string, v *string) (string, *string) {
		return k, v
	}
	return &Event{
		Vsn:          e.Vsn,
		Op:           e.Op,
		TableNameTup: e.TableNameTup,
		Key:          lo.MapEntries(e.Key, idFn),
		Fields:       lo.MapEntries(e.Fields, idFn),
		BeforeFields: lo.MapEntries(e.BeforeFields, idFn),
		ExporterRole: e.ExporterRole,
	}
}

func (e *Event) IsCutoverToTarget() bool {
	return e.Op == "cutover.target"
}

func (e *Event) IsCutoverToSourceReplica() bool {
	return e.Op == "cutover.source_replica"
}

func (e *Event) IsCutoverToSource() bool {
	return e.Op == "cutover.source"
}

func (e *Event) IsCutoverEvent() bool {
	return e.IsCutoverToTarget() || e.IsCutoverToSourceReplica() || e.IsCutoverToSource()
}

func (e *Event) GetSQLStmt(tdb TargetDB) (string, error) {
	switch e.Op {
	case "c":
		return e.getInsertStmt(tdb)
	case "u":
		return e.getUpdateStmt(tdb)
	case "d":
		return e.getDeleteStmt(tdb)
	default:
		panic("unknown op: " + e.Op)
	}
}

func (e *Event) GetPreparedSQLStmt(tdb TargetDB, targetDBType string) (string, error) {
	psName := e.GetPreparedStmtName()
	if stmt, ok := cachePreparedStmt.Load(psName); ok {
		return stmt.(string), nil
	}
	var ps string
	var err error
	switch e.Op {
	case "c":
		ps, err = e.getPreparedInsertStmt(tdb, targetDBType)
		if err != nil {
			return "", fmt.Errorf("get prepared insert stmt: %w", err)
		}
	case "u":
		ps, err = e.getPreparedUpdateStmt(tdb)
		if err != nil {
			return "", fmt.Errorf("get prepared update stmt: %w", err)
		}
	case "d":
		ps, err = e.getPreparedDeleteStmt(tdb)
		if err != nil {
			return "", fmt.Errorf("get prepared delete stmt: %w", err)
		}
	default:
		panic("unknown op: " + e.Op)
	}

	cachePreparedStmt.Store(psName, ps)
	return ps, nil
}

func (e *Event) GetParams() []interface{} {
	switch e.Op {
	case "c":
		return e.getInsertParams()
	case "u":
		return e.getUpdateParams()
	case "d":
		return e.getDeleteParams()
	default:
		panic("unknown op: " + e.Op)
	}
}

func (event *Event) GetPreparedStmtName() string {
	var ps strings.Builder
	ps.WriteString(event.TableNameTup.ForUserQuery())
	ps.WriteString("_")
	ps.WriteString(event.Op)
	if event.Op == "u" {
		keys := strings.Join(utils.GetMapKeysSorted(event.Fields), ",")
		ps.WriteString(":")
		ps.WriteString(keys)
	}
	return ps.String()
}

const insertTemplate = "INSERT INTO %s (%s) VALUES (%s)"
const updateTemplate = "UPDATE %s SET %s WHERE %s"
const deleteTemplate = "DELETE FROM %s WHERE %s"

func (event *Event) getInsertStmt(tdb TargetDB) (string, error) {
	columnList := make([]string, 0, len(event.Fields))
	valueList := make([]string, 0, len(event.Fields))
	for column, value := range event.Fields {
		column, err := tdb.QuoteAttributeName(event.TableNameTup, column)
		if err != nil {
			return "", fmt.Errorf("quote column name %s: %w", column, err)
		}
		columnList = append(columnList, column)
		if value == nil {
			valueList = append(valueList, "NULL")
		} else {
			valueList = append(valueList, *value)
		}
	}
	columns := strings.Join(columnList, ", ")
	values := strings.Join(valueList, ", ")
	stmt := fmt.Sprintf(insertTemplate, event.TableNameTup.ForUserQuery(), columns, values)
	return stmt, nil
}

func (event *Event) getUpdateStmt(tdb TargetDB) (string, error) {
	setClauses := make([]string, 0, len(event.Fields))
	for column, value := range event.Fields {
		column, err := tdb.QuoteAttributeName(event.TableNameTup, column)
		if err != nil {
			return "", fmt.Errorf("quote column name %s: %w", column, err)
		}
		if value == nil {
			setClauses = append(setClauses, fmt.Sprintf("%s = NULL", column))
		} else {
			setClauses = append(setClauses, fmt.Sprintf("%s = %s", column, *value))
		}
	}
	setClause := strings.Join(setClauses, ", ")

	whereClauses := make([]string, 0, len(event.Key))
	for column, value := range event.Key {
		if value == nil { // value can't be nil for keys
			panic("key value is nil")
		}
		column, err := tdb.QuoteAttributeName(event.TableNameTup, column)
		if err != nil {
			return "", fmt.Errorf("quote column name %s: %w", column, err)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", column, *value))
	}
	whereClause := strings.Join(whereClauses, " AND ")
	return fmt.Sprintf(updateTemplate, event.TableNameTup.ForUserQuery(), setClause, whereClause), nil
}

func (event *Event) getDeleteStmt(tdb TargetDB) (string, error) {
	whereClauses := make([]string, 0, len(event.Key))
	for column, value := range event.Key {
		if value == nil { // value can't be nil for keys
			panic("key value is nil")
		}
		column, err := tdb.QuoteAttributeName(event.TableNameTup, column)
		if err != nil {
			return "", fmt.Errorf("quote column name %s: %w", column, err)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = %s", column, *value))
	}
	whereClause := strings.Join(whereClauses, " AND ")
	return fmt.Sprintf(deleteTemplate, event.TableNameTup.ForUserQuery(), whereClause), nil
}

func (event *Event) getPreparedInsertStmt(tdb TargetDB, targetDBType string) (string, error) {
	columnList := make([]string, 0, len(event.Fields))
	valueList := make([]string, 0, len(event.Fields))
	keys := utils.GetMapKeysSorted(event.Fields)
	for pos, key := range keys {
		column, err := tdb.QuoteAttributeName(event.TableNameTup, key)
		if err != nil {
			panic(fmt.Errorf("quote column name %s: %w", column, err))
		}
		columnList = append(columnList, column)
		valueList = append(valueList, fmt.Sprintf("$%d", pos+1))
	}
	columns := strings.Join(columnList, ", ")
	values := strings.Join(valueList, ", ")
	stmt := fmt.Sprintf(insertTemplate, event.TableNameTup.ForUserQuery(), columns, values)
	if targetDBType == POSTGRESQL || targetDBType == YUGABYTEDB {
		keyColumns := utils.GetMapKeysSorted(event.Key)
		for i, column := range keyColumns {
			column, err := tdb.QuoteAttributeName(event.TableNameTup, column)
			if err != nil {
				return "", fmt.Errorf("quote column name %s: %w", column, err)
			}
			keyColumns[i] = column
		}
		stmt = fmt.Sprintf("%s ON CONFLICT (%s) DO NOTHING", stmt, strings.Join(keyColumns, ","))
	}
	return stmt, nil
}

// NOTE: PS for each event of same table can be different as it depends on columns being updated
func (event *Event) getPreparedUpdateStmt(tdb TargetDB) (string, error) {
	setClauses := make([]string, 0, len(event.Fields))
	keys := utils.GetMapKeysSorted(event.Fields)
	for pos, key := range keys {
		key, err := tdb.QuoteAttributeName(event.TableNameTup, key)
		if err != nil {
			return "", fmt.Errorf("quote column name %s: %w", key, err)
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", key, pos+1))
	}
	setClause := strings.Join(setClauses, ", ")

	whereClauses := make([]string, 0, len(event.Key))
	keys = utils.GetMapKeysSorted(event.Key)
	for i, key := range keys {
		key, err := tdb.QuoteAttributeName(event.TableNameTup, key)
		if err != nil {
			return "", fmt.Errorf("quote column name %s: %w", key, err)
		}
		pos := i + 1 + len(event.Fields)
		whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", key, pos))
	}
	whereClause := strings.Join(whereClauses, " AND ")
	return fmt.Sprintf(updateTemplate, event.TableNameTup.ForUserQuery(), setClause, whereClause), nil
}

func (event *Event) getPreparedDeleteStmt(tdb TargetDB) (string, error) {
	whereClauses := make([]string, 0, len(event.Key))
	keys := utils.GetMapKeysSorted(event.Key)
	for pos, key := range keys {
		key, err := tdb.QuoteAttributeName(event.TableNameTup, key)
		if err != nil {
			return "", fmt.Errorf("quote column name %s: %w", key, err)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", key, pos+1))
	}
	whereClause := strings.Join(whereClauses, " AND ")
	return fmt.Sprintf(deleteTemplate, event.TableNameTup.ForUserQuery(), whereClause), nil
}

func (event *Event) getInsertParams() []interface{} {
	return getMapValuesForQuery(event.Fields)
}

func (event *Event) getUpdateParams() []interface{} {
	params := make([]interface{}, 0, len(event.Fields)+len(event.Key))
	params = append(params, getMapValuesForQuery(event.Fields)...)
	params = append(params, getMapValuesForQuery(event.Key)...)
	return params
}

func (event *Event) getDeleteParams() []interface{} {
	return getMapValuesForQuery(event.Key)
}

func getMapValuesForQuery(m map[string]*string) []interface{} {
	keys := utils.GetMapKeysSorted(m)
	values := make([]interface{}, 0, len(keys))
	for _, key := range keys {
		values = append(values, m[key])
	}
	return values
}

func (event *Event) IsUniqueKeyChanged(uniqueKeyCols []string) bool {
	return event.Op == "u" &&
		len(uniqueKeyCols) > 0 &&
		lo.Some(lo.Keys(event.Fields), uniqueKeyCols)
}

// ==============================================================================================================================

type EventCounter struct {
	TotalEvents int64
	NumInserts  int64
	NumUpdates  int64
	NumDeletes  int64
}

func (ec *EventCounter) CountEvent(ev *Event) {
	ec.TotalEvents++
	switch ev.Op {
	case "c":
		ec.NumInserts++
	case "u":
		ec.NumUpdates++
	case "d":
		ec.NumDeletes++
	}
}

func (ec *EventCounter) Merge(ec2 *EventCounter) {
	ec.TotalEvents += ec2.TotalEvents
	ec.NumInserts += ec2.NumInserts
	ec.NumUpdates += ec2.NumUpdates
	ec.NumDeletes += ec2.NumDeletes
}

// ==============================================================================================================================

type EventBatch struct {
	Events             []*Event
	ChanNo             int
	EventCounts        *EventCounter
	EventCountsByTable *utils.StructMap[sqlname.NameTuple, *EventCounter]
}

func NewEventBatch(events []*Event, chanNo int) *EventBatch {
	batch := &EventBatch{
		Events:             events,
		ChanNo:             chanNo,
		EventCounts:        &EventCounter{},
		EventCountsByTable: utils.NewStructMap[sqlname.NameTuple, *EventCounter](),
	}
	batch.updateCounts()
	return batch
}

func (eb *EventBatch) GetLastVsn() int64 {
	return eb.Events[len(eb.Events)-1].Vsn
}

func (eb *EventBatch) ID() string {
	return fmt.Sprintf("%d:%d", eb.Events[0].Vsn, eb.GetLastVsn())
}

func (eb *EventBatch) GetChannelMetadataUpdateQuery(migrationUUID uuid.UUID) string {
	queryTemplate := `UPDATE %s 
	SET 
		last_applied_vsn=%d, 
		num_inserts = num_inserts + %d, 
		num_updates = num_updates + %d, 
		num_deletes = num_deletes + %d  
	where 
		migration_uuid='%s' AND channel_no=%d
	`
	return fmt.Sprintf(queryTemplate,
		EVENT_CHANNELS_METADATA_TABLE_NAME,
		eb.GetLastVsn(),
		eb.EventCounts.NumInserts,
		eb.EventCounts.NumUpdates,
		eb.EventCounts.NumDeletes,
		migrationUUID, eb.ChanNo)
}

func (eb *EventBatch) GetQueriesToUpdateEventStatsByTable(migrationUUID uuid.UUID, tableNameTup sqlname.NameTuple) string {
	queryTemplate := `UPDATE %s 
	SET 
		total_events = total_events + %d, 
		num_inserts = num_inserts + %d, 
		num_updates = num_updates + %d, 
		num_deletes = num_deletes + %d  
	where 
		migration_uuid='%s' AND table_name='%s' AND channel_no=%d
	`

	eventCounter, _ := eb.EventCountsByTable.Get(tableNameTup)

	return fmt.Sprintf(queryTemplate,
		EVENTS_PER_TABLE_METADATA_TABLE_NAME,
		eventCounter.TotalEvents,
		eventCounter.NumInserts,
		eventCounter.NumUpdates,
		eventCounter.NumDeletes,
		migrationUUID, tableNameTup.ForKey(), eb.ChanNo)
}

func (eb *EventBatch) GetQueriesToInsertEventStatsByTable(migrationUUID uuid.UUID, tableNameTup sqlname.NameTuple) string {
	queryTemplate := `INSERT INTO %s 
	(migration_uuid, table_name, channel_no, total_events, num_inserts, num_updates, num_deletes) 
	VALUES ('%s', '%s', %d, %d, %d, %d, %d)
	`

	eventCounter, _ := eb.EventCountsByTable.Get(tableNameTup)
	return fmt.Sprintf(queryTemplate,
		EVENTS_PER_TABLE_METADATA_TABLE_NAME,
		migrationUUID, tableNameTup.ForKey(), eb.ChanNo,
		eventCounter.TotalEvents,
		eventCounter.NumInserts,
		eventCounter.NumUpdates,
		eventCounter.NumDeletes)
}

func (eb *EventBatch) GetTableNames() []sqlname.NameTuple {
	tablenames := []sqlname.NameTuple{}
	eb.EventCountsByTable.IterKV(func(nt sqlname.NameTuple, ec *EventCounter) (bool, error) {
		tablenames = append(tablenames, nt)
		return true, nil
	})
	return tablenames
}

func (eb *EventBatch) updateCounts() {
	for _, event := range eb.Events {
		var eventCounter *EventCounter
		var found bool
		eventCounter, found = eb.EventCountsByTable.Get(event.TableNameTup)
		if !found {
			eb.EventCountsByTable.Put(event.TableNameTup, &EventCounter{})
			eventCounter, _ = eb.EventCountsByTable.Get(event.TableNameTup)
		}
		eventCounter.CountEvent(event)
		eb.EventCounts.CountEvent(event)
	}
}
