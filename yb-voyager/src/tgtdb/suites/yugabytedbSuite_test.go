//go:build unit

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
// package for tgtdb value converter suite
package tgtdbsuite

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/schemareg"
)

func TestStringConversionWithFormattingWithDoubleQuotes(t *testing.T) {
	// Given
	value := "abc\"def"
	// When we convert with formatIfRequired is true
	result, err := YBValueConverterSuite["STRING"](value, true, nil)
	assert.NoError(t, err)
	// Then
	assert.Equal(t, "'abc\"def'", result)
}

func TestStringConversionWithFormattingWithSingleQuotesEscaped(t *testing.T) {
	// Given
	value := "abc'def"
	// When we convert with formatIfRequired is true
	result, err := YBValueConverterSuite["STRING"](value, true, nil)
	assert.NoError(t, err)
	// Then
	assert.Equal(t, "'abc''def'", result)
}

func TestJsonConversionWithFormattingWithDoubleQuotes(t *testing.T) {
	// Given
	value := `{"key":"value"}`
	// When we convert with formatIfRequired is true
	result, err := YBValueConverterSuite["io.debezium.data.Json"](value, true, nil)
	assert.NoError(t, err)
	// Then
	assert.Equal(t, `'{"key":"value"}'`, result)
}

func TestJsonConversionWithFormattingWithSingleQuotesEscaped(t *testing.T) {
	// Given
	value := `{"key":"value's"}`
	// When we convert with formatIfRequired is true
	result, err := YBValueConverterSuite["io.debezium.data.Json"](value, true, nil)
	assert.NoError(t, err)
	// Then
	assert.Equal(t, `'{"key":"value''s"}'`, result)
}

func TestEnumConversionWithFormattingWithDoubleQuotes(t *testing.T) {
	// Given
	value := `enum"Value`
	// When we convert with formatIfRequired is true
	result, err := YBValueConverterSuite["io.debezium.data.Enum"](value, true, nil)
	assert.NoError(t, err)
	// Then
	assert.Equal(t, `'enum"Value'`, result)
}

func TestEnumConversionWithFormattingWithSingleQuotesEscaped(t *testing.T) {
	// Given
	value := "enum'Value"
	// When we convert with formatIfRequired is true
	result, err := YBValueConverterSuite["io.debezium.data.Enum"](value, true, nil)
	assert.NoError(t, err)
	// Then
	assert.Equal(t, "'enum''Value'", result)
}

func TestUUIDConversionWithFormatting(t *testing.T) {
	// Given
	value := "123e4567-e89b-12d3-a456-426614174000"
	// When we convert with formatIfRequired is true
	result, err := YBValueConverterSuite["io.debezium.data.Uuid"](value, true, nil)
	assert.NoError(t, err)
	// Then
	assert.Equal(t, `'123e4567-e89b-12d3-a456-426614174000'`, result)
}

func TestHstoreValueConversion(t *testing.T) {
	colSchema := &schemareg.ColumnSchema{
		Parameters: map[string]string{
			"__debezium.source.column.type": "HSTORE",
		},
	}
	value := `{"key1":"value1","key2":"value2"}`
	result, err := YBValueConverterSuite["io.debezium.data.Json"](value, false, colSchema)
	assert.NoError(t, err)
	assert.Equal(t, `"key1"=>"value1","key2"=>"value2"`, result)

	result, err = YBValueConverterSuite["io.debezium.data.Json"](value, true, colSchema)
	assert.NoError(t, err)
	assert.Equal(t, `'"key1"=>"value1","key2"=>"value2"'`, result)

	result, err = YBValueConverterSuite["io.debezium.data.Json"]("{}", false, colSchema)
	assert.NoError(t, err)
	assert.Equal(t, "", result)

	result, err = YBValueConverterSuite["io.debezium.data.Json"]("{}", true, colSchema)
	assert.NoError(t, err)
	assert.Equal(t, "''", result)

	value = `{"\"{\"\"key1\"\":\"\"value1\"\",\"\"key2\"\":\"\"value2\"\"}\"":"{\\\"key1=value1, key2={\\\"key1=value1, key2=value2\\\"}\\\"}"}`
	result, err = YBValueConverterSuite["io.debezium.data.Json"](value, false, colSchema)
	assert.NoError(t, err)
	assert.Equal(t, `"\"{\"\"key1\"\":\"\"value1\"\",\"\"key2\"\":\"\"value2\"\"}\""=>"{\"key1=value1, key2={\"key1=value1, key2=value2\"}\"}"`, result)

	result, err = YBValueConverterSuite["io.debezium.data.Json"](value, true, colSchema)
	assert.NoError(t, err)
	assert.Equal(t, `'"\"{\"\"key1\"\":\"\"value1\"\",\"\"key2\"\":\"\"value2\"\"}\""=>"{\"key1=value1, key2={\"key1=value1, key2=value2\"}\"}"'`, result)
}
