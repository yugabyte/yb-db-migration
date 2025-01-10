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
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils/schemareg"
)

var BIT_VARYING_MAX_LEN = 2147483647 // max len for datatype like bit varying without n

// value converter Function type
type ConverterFn func(v string, formatIfRequired bool, dbzmSchema *schemareg.ColumnSchema) (string, error)

func quoteValueIfRequired(value string, formatIfRequired bool, _ *schemareg.ColumnSchema) (string, error) {
	if formatIfRequired {
		return fmt.Sprintf("'%s'", value), nil
	}
	return value, nil
}

func quoteValueIfRequiredWithEscaping(value string, formatIfRequired bool, _ *schemareg.ColumnSchema) (string, error) {
	if formatIfRequired {
		formattedColumnValue := strings.Replace(value, "'", "''", -1)
		return fmt.Sprintf("'%s'", formattedColumnValue), nil
	} else {
		return value, nil
	}
}

func hstoreValueConverter(columnValue string, formatIfRequired bool, dbzmSchema *schemareg.ColumnSchema) (string, error) {
	//e.g. val - "{""key1"":""value1"",""key2"":""value2""}"  and for empty string val - {}
	columnValue = fmt.Sprintf(`%s`, columnValue)

	// for the cases where value "{""{\""key1=value1, key2=value2\""}"":""{\\\""key1=value1, key2={\\\""key1=value1, key2=value2\\\""}\\\""}""}"
	// escaping the \ -> \\ to preserve the escaping while unmarshalling
	columnValue = strings.Replace(columnValue, `\"`, `\\\"`, -1)

	// unescaping the cases which are already escaped {\\\\\" -> {\\\"
	columnValue = strings.Replace(columnValue, `\\\\\"`, `\\\"`, -1)

	// Initialize a map to hold the parsed data
	var result map[string]interface{}

	// Parse the JSON string into the map
	err := json.Unmarshal([]byte(columnValue), &result)
	if err != nil {
		return "", fmt.Errorf("error converting the value to map: %v", err)
	}
	// Create a map to store the parsed key-value pairs
	var transformedMapValue string

	//sorting the keys so that result always have keys in an order
	keys := lo.Keys(result)
	slices.Sort(keys)

	// Access key-value pairs to format the string as - "\"{\"\"key1\"\":\"\"value1\"\",\"\"key2\"\":\"\"value2\"\"}\""=>"{\"key1=value1, key2={\"key1=value1, key2=value2\"}\"}"
	for _, key := range keys {
		value := result[key]
		transformedMapValue = transformedMapValue + fmt.Sprintf(`"%s"=>"%s",`, key, value)
	}

	if len(transformedMapValue) > 1 {
		transformedMapValue = transformedMapValue[:len(transformedMapValue)-1] //remove last comma and
	}
	return quoteValueIfRequired(transformedMapValue, formatIfRequired, dbzmSchema) // add quotes if required
}

var YBValueConverterSuite = map[string]ConverterFn{
	"io.debezium.data.Json": func(columnValue string, formatIfRequired bool, dbzmSchema *schemareg.ColumnSchema) (string, error) {
		if dbzmSchema != nil {
			colType, ok := dbzmSchema.Parameters["__debezium.source.column.type"]
			if !ok || colType != "HSTORE" {
				return quoteValueIfRequiredWithEscaping(columnValue, formatIfRequired, dbzmSchema)
			}
			return hstoreValueConverter(columnValue, formatIfRequired, dbzmSchema)
		}
		return quoteValueIfRequiredWithEscaping(columnValue, formatIfRequired, dbzmSchema)
	},
	"io.debezium.data.Enum":     quoteValueIfRequiredWithEscaping,
	"io.debezium.time.Interval": quoteValueIfRequired,
	"io.debezium.data.Uuid":     quoteValueIfRequired,
	"io.debezium.time.Date": func(columnValue string, formatIfRequired bool, dbzmSchema *schemareg.ColumnSchema) (string, error) {
		epochDays, err := strconv.ParseInt(columnValue, 10, 64)
		if err != nil {
			return columnValue, fmt.Errorf("parsing epoch seconds: %v", err)
		}
		epochSecs := epochDays * 24 * 60 * 60
		date := time.Unix(int64(epochSecs), 0).UTC().Format(time.DateOnly)
		return quoteValueIfRequired(date, formatIfRequired, dbzmSchema)
	},
	"io.debezium.time.Timestamp": func(columnValue string, formatIfRequired bool, dbzmSchema *schemareg.ColumnSchema) (string, error) {
		epochMilliSecs, err := strconv.ParseInt(columnValue, 10, 64)
		if err != nil {
			return columnValue, fmt.Errorf("parsing epoch milliseconds: %v", err)
		}
		epochSecs := epochMilliSecs / 1000
		timestamp := time.Unix(epochSecs, 0).UTC().Format(time.DateTime)
		return quoteValueIfRequired(timestamp, formatIfRequired, dbzmSchema)
	},
	"io.debezium.time.MicroTimestamp": func(columnValue string, formatIfRequired bool, dbzmSchema *schemareg.ColumnSchema) (string, error) {
		epochMicroSecs, err := strconv.ParseInt(columnValue, 10, 64)
		if err != nil {
			return columnValue, fmt.Errorf("parsing epoch microseconds: %v", err)
		}
		epochSeconds := epochMicroSecs / 1000000
		epochNanos := (epochMicroSecs % 1000000) * 1000
		timestamp := time.Unix(epochSeconds, epochNanos).UTC().Format("2006-01-02T15:04:05.999999")
		return quoteValueIfRequired(timestamp, formatIfRequired, dbzmSchema)
	},
	"io.debezium.time.NanoTimestamp": func(columnValue string, formatIfRequired bool, dbzmSchema *schemareg.ColumnSchema) (string, error) {
		epochNanoSecs, err := strconv.ParseInt(columnValue, 10, 64)
		if err != nil {
			return columnValue, fmt.Errorf("parsing epoch nanoseconds: %v", err)
		}
		epochSeconds := epochNanoSecs / 1000000000
		epochNanos := epochNanoSecs % 1000000000
		timestamp := time.Unix(epochSeconds, epochNanos).UTC().Format("2006-01-02T15:04:05.999999999")
		return quoteValueIfRequired(timestamp, formatIfRequired, dbzmSchema)
	},
	"io.debezium.time.ZonedTimestamp": quoteValueIfRequired,
	"io.debezium.time.Time": func(columnValue string, formatIfRequired bool, dbzmSchema *schemareg.ColumnSchema) (string, error) {
		epochMilliSecs, err := strconv.ParseInt(columnValue, 10, 64)
		if err != nil {
			return columnValue, fmt.Errorf("parsing epoch milliseconds: %v", err)
		}
		epochSecs := epochMilliSecs / 1000
		timeValue := time.Unix(epochSecs, 0).UTC().Format(time.TimeOnly)
		return quoteValueIfRequired(timeValue, formatIfRequired, dbzmSchema)
	},
	"io.debezium.time.MicroTime": func(columnValue string, formatIfRequired bool, dbzmSchema *schemareg.ColumnSchema) (string, error) {
		epochMicroSecs, err := strconv.ParseInt(columnValue, 10, 64)
		if err != nil {
			return columnValue, fmt.Errorf("parsing epoch microseconds: %v", err)
		}
		epochSeconds := epochMicroSecs / 1000000
		epochNanos := (epochMicroSecs % 1000000) * 1000
		MICRO_TIME_FORMAT := "15:04:05.000000"
		timeValue := time.Unix(epochSeconds, epochNanos).UTC().Format(MICRO_TIME_FORMAT)
		return quoteValueIfRequired(timeValue, formatIfRequired, dbzmSchema)
	},
	"io.debezium.data.Bits": func(columnValue string, formatIfRequired bool, dbzmSchema *schemareg.ColumnSchema) (string, error) {
		bytes, err := base64.StdEncoding.DecodeString(columnValue)
		if err != nil {
			return columnValue, fmt.Errorf("decoding variable scale decimal in base64: %v", err)
		}
		var data uint64
		if len(bytes) >= 8 {
			data = binary.LittleEndian.Uint64(bytes[:8])
		} else {
			for i, b := range bytes {
				data |= uint64(b) << (8 * i)
			}
		}
		quote := lo.Ternary(formatIfRequired, "'", "")
		strLength, ok := dbzmSchema.Parameters["length"]
		length := lo.Ternary(ok, lo.Must(strconv.Atoi(strLength)), BIT_VARYING_MAX_LEN)
		if length == BIT_VARYING_MAX_LEN {
			return fmt.Sprintf("%s%b%s", quote, data, quote), nil
		} else {
			return fmt.Sprintf("%s%0*b%s", quote, length, data, quote), nil
		}
	},
	"io.debezium.data.geometry.Point": func(columnValue string, formatIfRequired bool, _ *schemareg.ColumnSchema) (string, error) {
		// TODO: figure out if we want to represent it as a postgres native point or postgis point.
		return columnValue, nil
	},
	"io.debezium.data.geometry.Geometry": func(columnValue string, formatIfRequired bool, _ *schemareg.ColumnSchema) (string, error) {
		// TODO: figure out if we want to represent it as a postgres native point or postgis geometry point.
		return columnValue, nil
	},
	"io.debezium.data.geometry.Geography": func(columnValue string, formatIfRequired bool, _ *schemareg.ColumnSchema) (string, error) {
		//TODO: figure out if we want to represent it as a postgres native geography or postgis geometry geography.
		return columnValue, nil
	},
	"org.apache.kafka.connect.data.Decimal": func(columnValue string, formatIfRequired bool, _ *schemareg.ColumnSchema) (string, error) {
		return columnValue, nil //handled in exporter plugin
	},
	"io.debezium.data.VariableScaleDecimal": func(columnValue string, formatIfRequired bool, _ *schemareg.ColumnSchema) (string, error) {
		return columnValue, nil //handled in exporter plugin
	},
	"BYTES": func(columnValue string, formatIfRequired bool, _ *schemareg.ColumnSchema) (string, error) {
		//decode base64 string to bytes
		decodedBytes, err := base64.StdEncoding.DecodeString(columnValue) //e.g.`////wv==` -> `[]byte{0x00, 0x00, 0x00, 0x00}`
		if err != nil {
			return columnValue, fmt.Errorf("decoding base64 string: %v", err)
		}
		//convert bytes to hex string e.g. `[]byte{0x00, 0x00, 0x00, 0x00}` -> `\\x00000000`
		hexString := ""
		for _, b := range decodedBytes {
			hexString += fmt.Sprintf("%02x", b)
		}
		hexValue := ""
		if formatIfRequired {
			hexValue = fmt.Sprintf("'\\x%s'", hexString) // in insert statement no need of escaping the backslash and add quotes
		} else {
			hexValue = fmt.Sprintf("\\x%s", hexString) // in data file need to escape the backslash
		}
		return string(hexValue), nil
	},
	"MAP": func(columnValue string, formatIfRequired bool, dbzmSchema *schemareg.ColumnSchema) (string, error) {
		fmt.Println(columnValue)
		return quoteValueIfRequiredWithEscaping(columnValue, formatIfRequired, dbzmSchema) //handled in exporter plugin
	},
	"STRING": quoteValueIfRequiredWithEscaping,
}
