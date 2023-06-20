/*
Copyright (c) YugaByte, Inc.

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
package utils

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/yosssi/gohtml"
	"golang.org/x/exp/slices"
)

var DoNotPrompt bool

func Wait(args ...string) {
	var successMsg, failureMsg string
	if len(args) > 0 {
		successMsg = args[0]
	}
	if len(args) > 1 {
		failureMsg = args[1]
	}

	chars := [4]byte{'|', '/', '-', '\\'}
	var i = 0
	for {
		i++
		select {
		case channelCode := <-WaitChannel:
			fmt.Print("\b ")
			if channelCode == 0 {
				fmt.Printf("%s", successMsg)
			} else if channelCode == 1 {
				fmt.Printf("%s", failureMsg)
			}
			WaitChannel <- -1
			return
		default:
			fmt.Printf("\b" + string(chars[i%4]))
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func Readline(r *bufio.Reader) (string, error) {
	var (
		isPrefix bool  = true
		err      error = nil
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return string(ln), err
}

func AskPrompt(args ...string) bool {
	if DoNotPrompt {
		return true
	}
	var input string
	var argsLen int = len(args)

	for i := 0; i < argsLen; i++ {
		if i != argsLen-1 {
			fmt.Printf("%s ", args[i])
		} else {
			fmt.Printf("%s", args[i])
		}

	}
	fmt.Printf("? [Y/N]: ")

	_, err := fmt.Scan(&input)

	if err != nil {
		panic(err)
	}

	input = strings.TrimSpace(input)
	input = strings.ToUpper(input)

	if input == "Y" || input == "YES" {
		return true
	}
	return false
}

func GetSchemaObjectList(sourceDBType string) []string {
	var requiredList []string
	switch sourceDBType {
	case "oracle":
		requiredList = oracleSchemaObjectList
	case "postgresql":
		requiredList = postgresSchemaObjectList
	case "mysql":
		requiredList = mysqlSchemaObjectList
	default:
		ErrExit("Unsupported %q source db type\n", sourceDBType)
	}
	return requiredList
}

func IsDirectoryEmpty(pathPattern string) bool {
	files, _ := filepath.Glob(pathPattern + "/*")
	return len(files) == 0
}

func FileOrFolderExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		} else {
			ErrExit("check if %q exists: %s", path, err)
		}
	} else {
		return true
	}
	panic("unreachable")
}

func CleanDir(dir string) {
	if FileOrFolderExists(dir) {
		files, _ := filepath.Glob(dir + "/*")
		log.Infof("cleaning directory: %s", dir)
		for _, file := range files {
			err := os.RemoveAll(file)
			if err != nil {
				ErrExit("clean dir %q: %s", dir, err)
			}
		}
	}
}

func PrintIfTrue(message string, args ...bool) {
	for i := 0; i < len(args); i++ {
		if !args[i] {
			return
		}
	}
	fmt.Printf("%s", message)
}

func ParseJsonFromString(jsonString string) Report {
	byteJson := []byte(jsonString)
	var report Report
	err := json.Unmarshal(byteJson, &report)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
	}
	return report
}

func GetObjectNameListFromReport(report Report, objType string) []string {
	var objectList []string
	for _, dbObject := range report.Summary.DBObjects {
		if dbObject.ObjectType == objType {
			rawObjectList := strings.Trim(dbObject.ObjectNames, ", ")
			objectList = strings.Split(rawObjectList, ", ")
			break
		}
	}
	sort.Strings(objectList)
	return objectList
}

func PrettifyHtmlString(htmlStr string) string {
	return gohtml.Format(htmlStr)
}

func PrettifyJsonString(jsonStr string) string {
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, []byte(jsonStr), "", "    "); err != nil {
		panic(err)
	}
	return prettyJSON.String()
}

func GetObjectDirPath(schemaDirPath string, objType string) string {
	var requiredPath string
	if objType == "INDEX" {
		requiredPath = filepath.Join(schemaDirPath, "tables")
	} else {
		requiredPath = filepath.Join(schemaDirPath, strings.ToLower(objType)+"s")
	}
	return requiredPath
}

func GetObjectFilePath(schemaDirPath string, objType string) string {
	var requiredPath string
	if objType == "INDEX" || objType == "UNIQUE INDEX" {
		requiredPath = filepath.Join(schemaDirPath, "tables", "INDEXES_table.sql")
	} else if objType == "FTS_INDEX" {
		requiredPath = filepath.Join(schemaDirPath, "tables", "FTS_INDEXES_table.sql")
	} else if objType == "PARTITION_INDEX" {
		requiredPath = filepath.Join(schemaDirPath, "partitions", "PARTITION_INDEXES_partition.sql")
	} else {
		requiredPath = filepath.Join(schemaDirPath, strings.ToLower(objType)+"s",
			strings.ToLower(objType)+".sql")
	}
	return requiredPath
}

func GetObjectFileName(schemaDirPath string, objType string) string {
	return filepath.Base(GetObjectFilePath(schemaDirPath, objType))
}

func IsQuotedString(str string) bool {
	if len(str) == 0 {
		return false
	}
	return str[0] == '"' && str[len(str)-1] == '"'
}

func GetSortedKeys(tablesProgressMetadata map[string]*TableProgressMetadata) []string {
	var keys []string

	for key := range tablesProgressMetadata {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys
}

func SetDifference(includeList []string, excludeList []string) []string {
	if len(includeList) == 0 || len(excludeList) == 0 {
		return includeList
	}
	var finalList []string
	for _, object := range includeList {
		if slices.Contains(excludeList, object) {
			continue
		}
		finalList = append(finalList, object)
	}
	return finalList
}

func CsvStringToSlice(str string) []string {
	result := []string{}
	for _, s := range strings.Split(str, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			result = append(result, s)
		}
	}
	return result
}

func LookupIP(name string) []string {
	var result []string

	ips, err := net.LookupIP(name)
	if err != nil {
		log.Infof("Error Resolving name=%s: %v", name, err)
		return result
	}

	for _, ip := range ips {
		result = append(result, ip.String())
	}
	return result
}

func InsensitiveSliceContains(slice []string, s string) bool {
	for i := 0; i < len(slice); i++ {
		if strings.Contains(strings.ToLower(s), strings.ToLower(slice[i])) {
			return true
		}
	}
	return false
}

func ToCaseInsensitiveNames(names []string) []string {
	for i, object := range names {
		object = strings.Trim(object, "\"")
		names[i] = strings.ToLower(object)
	}
	return names
}

func GetRedactedURLs(urlList []string) []string {
	result := []string{}
	for _, u := range urlList {
		obj, err := url.Parse(u)
		if err != nil {
			ErrExit("invalid URL: %q", u)
		}
		result = append(result, obj.Redacted())
	}
	return result
}

func GetSqlStmtToPrint(stmt string) string {
	if len(stmt) < 80 {
		return stmt
	} else {
		return fmt.Sprintf("%s ...", stmt[:80])
	}
}

func PrintSqlStmtIfDDL(stmt string, fileName string) {
	setOrSelectStmt := strings.HasPrefix(strings.ToUpper(stmt), "SET ") ||
		strings.HasPrefix(strings.ToUpper(stmt), "SELECT ")
	if !setOrSelectStmt {
		fmt.Printf("%s: %s\n", fileName, GetSqlStmtToPrint(stmt))
	}
}

func Uniq(slice []string) []string {
	keys := make(map[string]bool)
	var list []string
	for _, entry := range slice {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func HumanReadableByteCount(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB",
		float64(bytes)/float64(div), "KMGTPE"[exp])
}

// https://yourbasic.org/golang/generate-random-string/
func GenerateRandomString(length int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	digits := "0123456789"
	specials := "~=+%^*/()[]{}/!@#$?|"
	all := "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" +
		digits + specials
	buf := make([]byte, length)
	buf[0] = digits[r.Intn(len(digits))]
	buf[1] = specials[r.Intn(len(specials))]
	for i := 2; i < length; i++ {
		buf[i] = all[r.Intn(len(all))]
	}
	r.Shuffle(len(buf), func(i, j int) {
		buf[i], buf[j] = buf[j], buf[i]
	})
	return string(buf)
}
