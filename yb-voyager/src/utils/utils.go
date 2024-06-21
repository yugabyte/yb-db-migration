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
package utils

import (
	"bufio"
	"database/sql"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
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
	log.Infof("get schema object list for %q", sourceDBType)
	var requiredList []string
	switch sourceDBType {
	case "oracle":
		requiredList = oracleSchemaObjectList
	case "postgresql", "yugabytedb":
		requiredList = postgresSchemaObjectList
	case "mysql":
		requiredList = mysqlSchemaObjectList
	default:
		ErrExit("Unsupported %q source db type\n", sourceDBType)
	}
	return requiredList
}

func GetExportSchemaObjectList(sourceDBType string) []string {
	log.Infof("get schema object list for %q", sourceDBType)
	var requiredList []string
	switch sourceDBType {
	case "oracle":
		requiredList = oracleSchemaObjectListForExport
	case "postgresql", "yugabytedb":
		requiredList = postgresSchemaObjectListForExport
	case "mysql":
		requiredList = mysqlSchemaObjectListForExport
	default:
		ErrExit("Unsupported %q source db type\n", sourceDBType)
	}
	return requiredList
}

func ContainsString(list []string, str string) bool {
	for _, object := range list {
		if strings.EqualFold(object, str) {
			return true
		}
	}
	return false
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

func FileOrFolderExistsWithGlobPattern(path string) bool {
	files, err := filepath.Glob(path)
	if err != nil {
		ErrExit("Error while reading %q: %s", path, err)
	}
	return len(files) > 0
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

func GetObjectNameListFromReport(report SchemaReport, objType string) []string {
	var objectList []string
	for _, dbObject := range report.SchemaSummary.DBObjects {
		if dbObject.ObjectType == objType {
			rawObjectList := strings.Trim(dbObject.ObjectNames, ", ")
			objectList = strings.Split(rawObjectList, ", ")
			break
		}
	}
	sort.Strings(objectList)
	return objectList
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

func ContainsAnySubstringFromSlice(slice []string, s string) bool {
	for i := 0; i < len(slice); i++ {
		if strings.Contains(strings.ToLower(s), strings.ToLower(slice[i])) {
			return true
		}
	}
	return false
}

func WaitForLineInLogFile(filePath string, message string, timeoutDuration time.Duration) error {
	// Wait for log file to be created
	timeout := time.After(timeoutDuration)
	for {
		_, err := os.Stat(filePath)
		if err == nil {
			break
		}
		select {
		case <-timeout:
			return fmt.Errorf("timeout while waiting for log file %q", filePath)
		default:
			time.Sleep(1 * time.Second)
		}
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file %s: %v", filePath, err)
	}

	defer file.Close()

	for {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, message) {
				return nil
			}
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error scanning file %s: %v", filePath, err)
		}

		select {
		case <-timeout:
			return fmt.Errorf("timeout while waiting for %q in %q", message, filePath)
		default:
			time.Sleep(1 * time.Second)
		}
	}
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

func ForEachMatchingLineInFile(filePath string, re *regexp.Regexp, callback func(matches []string) bool) error {
	return ForEachLineInFile(filePath, func(line string) bool {
		matches := re.FindStringSubmatch(line)
		if len(matches) > 0 {
			return callback(matches)
		}
		return true // Continue with next line.
	})
}

func ForEachLineInFile(filePath string, callback func(line string) bool) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file %s: %v", filePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		cont := callback(scanner.Text())
		if !cont {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file %s: %v", filePath, err)
	}
	return nil
}

func GetEnvAsInt(key string, fallback int) int {
	valueStr, exists := os.LookupEnv(key)
	if !exists {
		return fallback
	}
	valueInt, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		PrintAndLog("Couldn't interpret env var %v=%v. Defaulting to %v", key, valueStr, fallback)
		return fallback
	}
	return int(valueInt)
}

func GetMapKeysSorted(m map[string]*string) []string {
	keys := lo.Keys(m)
	sort.Strings(keys)
	return keys
}

func GetFSUtilizationPercentage(path string) (int, error) {
	var stats syscall.Statfs_t
	err := syscall.Statfs(path, &stats)
	if err != nil {
		return -1, fmt.Errorf("error while getting disk stats for %q: %v", path, err)
	}

	percUtilization := 100 - int((stats.Bavail*100)/stats.Blocks)
	return percUtilization, nil
}

// read the file and return slice of csv
func ReadTableNameListFromFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file %s: %v", filePath, err)
	}
	defer file.Close()
	var list []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) > 0 { //ignore empty lines
			list = append(list, CsvStringToSlice(line)...)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file %s: %v", filePath, err)
	}
	return list, nil
}

func GetLogMiningFlushTableName(migrationUUID uuid.UUID) string {
	// SQL tables doesn't support '-' in the name
	convertedMigUUID := strings.Replace(migrationUUID.String(), "-", "_", -1)
	return fmt.Sprintf("VOYAGER_LOG_MINING_FLUSH_%s", strings.ToUpper(convertedMigUUID))
}

func ConvertStringSliceToInterface(slice []string) []interface{} {
	return lo.Map(slice, func(s string, _ int) interface{} {
		return s
	})
}

func GetRelativePathFromCwd(fullPath string) string {
	cwd, err := os.Getwd()
	if err != nil {
		return fullPath
	}
	relativePath, err := filepath.Rel(cwd, fullPath)
	if err != nil {
		return fullPath
	}
	return relativePath
}

func ConnectToSqliteDatabase(dbPath string) (*sql.DB, error) {
	// Connect to the database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Check if the connection is successful
	// TODO: add retry logic
	err = db.Ping()
	if err != nil {
		err := db.Close()
		if err != nil {
			return nil, err
		}
		return nil, err
	}

	return db, nil
}

/*
BytesToGB function converts the size of the source object from bytes to GB as it is required for further calculation
Parameters:

	sizeInBytes: size of source object in bytes

Returns:

	sizeInGB: size of source object in gigabytes
*/
func BytesToGB(sizeInBytes float64) float64 {
	sizeInGB := sizeInBytes / (1024 * 1024 * 1024)
	// any value less than a 0.1 MB is considered as 0
	if sizeInGB < 0.0001 {
		return 0
	}
	return sizeInGB
}

func SafeDereferenceInt64(ptr *int64) int64 {
	if ptr != nil {
		return *ptr
	}
	return 0
}
