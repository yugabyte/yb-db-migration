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
package sqlname

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/samber/lo"
)

//================================================

type identifier struct {
	Quoted, Unquoted, MinQuoted string
}

// Can be a name of a table, sequence, materialised view, etc.
type ObjectName struct {
	SchemaName        string
	FromDefaultSchema bool

	Qualified    identifier
	Unqualified  identifier
	MinQualified identifier
}

func NewObjectName(dbType, defaultSchemaName, schemaName, tableName string) *ObjectName {
	result := &ObjectName{
		SchemaName:        schemaName,
		FromDefaultSchema: schemaName == defaultSchemaName,
		Qualified: identifier{
			Quoted:    schemaName + "." + quote2(dbType, tableName),
			Unquoted:  schemaName + "." + tableName,
			MinQuoted: schemaName + "." + minQuote2(tableName, dbType),
		},
		Unqualified: identifier{
			Quoted:    quote2(dbType, tableName),
			Unquoted:  tableName,
			MinQuoted: minQuote2(tableName, dbType),
		},
	}
	result.MinQualified = lo.Ternary(result.FromDefaultSchema, result.Unqualified, result.Qualified)
	return result
}

func (nv *ObjectName) String() string {
	return nv.MinQualified.MinQuoted
}

func (nv *ObjectName) MatchesPattern(pattern string) (bool, error) {
	parts := strings.Split(pattern, ".")
	switch true {
	case len(parts) == 2:
		if !strings.EqualFold(parts[0], nv.SchemaName) {
			return false, nil
		}
		pattern = parts[1]
	case len(parts) == 1:
		if !nv.FromDefaultSchema {
			return false, nil
		}
		pattern = parts[0]
	default:
		return false, fmt.Errorf("invalid pattern: %s", pattern)
	}
	match1, err := filepath.Match(pattern, nv.Unqualified.Unquoted)
	if err != nil {
		return false, fmt.Errorf("invalid pattern: %s", pattern)
	}
	if match1 {
		return true, nil
	}
	match2, err := filepath.Match(pattern, nv.Unqualified.Quoted)
	if err != nil {
		return false, fmt.Errorf("invalid pattern: %s", pattern)
	}
	return match2, nil
}

// <SourceTableName, TargetTableName>
type NameTuple struct {
	// Mode        string
	CurrentName *ObjectName
	SourceName  *ObjectName
	TargetName  *ObjectName
}

func (t1 *NameTuple) Equals(t2 *NameTuple) bool {
	return reflect.DeepEqual(t1, t2)
}

// func (t *NameTuple) SetMode(mode string) {
// 	t.Mode = mode
// 	switch mode {
// 	case TARGET_DB_IMPORTER_ROLE:
// 		t.CurrentName = t.TargetName
// 	case SOURCE_DB_IMPORTER_ROLE:
// 		t.CurrentName = t.SourceName
// 	case SOURCE_REPLICA_DB_IMPORTER_ROLE:
// 		t.CurrentName = t.SourceName
// 	case SOURCE_DB_EXPORTER_ROLE:
// 		t.CurrentName = t.SourceName
// 	case TARGET_DB_EXPORTER_FF_ROLE, TARGET_DB_EXPORTER_FB_ROLE:
// 		t.CurrentName = t.TargetName
// 	default:
// 		t.CurrentName = nil
// 	}
// }

func (t *NameTuple) String() string {
	return t.CurrentName.String()
}

func (t *NameTuple) MatchesPattern(pattern string) (bool, error) {
	for _, tableName := range []*ObjectName{t.SourceName, t.TargetName} {
		if tableName == nil {
			continue
		}
		match, err := tableName.MatchesPattern(pattern)
		if err != nil {
			return false, err
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func (t *NameTuple) ForUserQuery() string {
	return t.CurrentName.Qualified.Quoted
}

func (t *NameTuple) ForCatalogQuery() (string, string) {
	return t.CurrentName.SchemaName, t.CurrentName.Unqualified.Unquoted
}

func (t *NameTuple) ForKey() string {
	if t.SourceName != nil {
		return t.SourceName.Qualified.Quoted
	}
	return t.TargetName.Qualified.Quoted
}

//================================================

func quote2(dbType, name string) string {
	switch dbType {
	case POSTGRESQL, YUGABYTEDB, ORACLE:
		return `"` + name + `"`
	case MYSQL:
		return name
	default:
		panic("unknown source db type")
	}
}

func minQuote2(objectName, sourceDBType string) string {
	switch sourceDBType {
	case YUGABYTEDB, POSTGRESQL:
		if IsAllLowercase(objectName) && !IsReservedKeywordPG(objectName) {
			return objectName
		} else {
			return `"` + objectName + `"`
		}
	case MYSQL:
		return objectName
	case ORACLE:
		if IsAllUppercase(objectName) && !IsReservedKeywordOracle(objectName) {
			return objectName
		} else {
			return `"` + objectName + `"`
		}
	default:
		panic("invalid source db type")
	}
}
