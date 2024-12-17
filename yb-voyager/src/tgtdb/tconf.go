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
	"net/url"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

type TargetConf struct {
	TargetDBType         string        `json:"target_db_type"`
	Host                 string        `json:"host"`
	Port                 int           `json:"port"`
	User                 string        `json:"user"`
	Password             string        `json:"password"`
	DBName               string        `json:"db_name"`
	Schema               string        `json:"schema"`
	SSLMode              string        `json:"ssl_mode"`
	SSLCertPath          string        `json:"ssl_cert_path"`
	SSLKey               string        `json:"ssl_key"`
	SSLRootCert          string        `json:"ssl_root_cert"`
	SSLCRL               string        `json:"ssl_crl"`
	SSLQueryString       string        `json:"ssl_query_string"`
	DBSid                string        `json:"db_sid"`
	TNSAlias             string        `json:"tns_alias"`
	OracleHome           string        `json:"oracle_home"`
	Uri                  string        `json:"uri"`
	ContinueOnError      utils.BoolStr `json:"continue_on_error"`
	IgnoreIfExists       utils.BoolStr `json:"ignore_if_exists"`
	TableList            string        `json:"table_list"`
	ExcludeTableList     string        `json:"exclude_table_list"`
	ImportMode           bool          `json:"import_mode"`
	ImportObjects        string        `json:"import_objects"`
	ExcludeImportObjects string        `json:"exclude_import_objects"`
	DBVersion            string        `json:"db_version"`
	RunGuardrailsChecks  utils.BoolStr `json:"run_guardrails_checks"`

	TargetEndpoints             string        `json:"target_endpoints"`
	UsePublicIP                 utils.BoolStr `json:"use_public_ip"`
	EnableUpsert                utils.BoolStr `json:"enable_upsert"`
	DisableTransactionalWrites  utils.BoolStr `json:"disable_transactional_writes"`
	Parallelism                 int           `json:"parallelism"`
	EnableYBAdaptiveParallelism utils.BoolStr `json:"enable_adaptive_parallelism"`
	MaxParallelism              int           `json:"max_parallelism"` // in case adaptive parallelism is enabled.
}

func (t *TargetConf) Clone() *TargetConf {
	clone := *t
	return &clone
}

func (t *TargetConf) GetConnectionUri() string {
	if t.Uri == "" {
		hostAndPort := fmt.Sprintf("%s:%d", t.Host, t.Port)
		targetUrl := &url.URL{
			Scheme:   "postgresql",
			User:     url.UserPassword(t.User, t.Password),
			Host:     hostAndPort,
			Path:     t.DBName,
			RawQuery: generateSSLQueryStringIfNotExists(t),
		}

		t.Uri = targetUrl.String()
	}

	return t.Uri
}

// this function is only triggered when t.Uri==""
func generateSSLQueryStringIfNotExists(t *TargetConf) string {
	SSLQueryString := ""
	if t.SSLMode == "" {
		t.SSLMode = "prefer"
	}
	if t.SSLQueryString == "" {

		if t.SSLMode == "disable" || t.SSLMode == "allow" || t.SSLMode == "prefer" || t.SSLMode == "require" || t.SSLMode == "verify-ca" || t.SSLMode == "verify-full" {
			SSLQueryString = "sslmode=" + t.SSLMode
			if t.SSLMode == "require" || t.SSLMode == "verify-ca" || t.SSLMode == "verify-full" {

				if t.SSLCertPath != "" {
					SSLQueryString += "&sslcert=" + t.SSLCertPath
				}
				if t.SSLKey != "" {
					SSLQueryString += "&sslkey=" + t.SSLKey
				}
				if t.SSLRootCert != "" {
					SSLQueryString += "&sslrootcert=" + t.SSLRootCert
				}
				if t.SSLCRL != "" {
					SSLQueryString += "&sslcrl=" + t.SSLCRL
				}
			}
		} else {
			fmt.Println("Invalid sslmode entered")
		}
	} else {
		SSLQueryString = t.SSLQueryString
	}
	return SSLQueryString
}

func GetRedactedTargetConf(t *TargetConf) *TargetConf {
	redacted := *t
	redacted.Uri = utils.GetRedactedURLs(t.Uri)[0]
	redacted.Password = "XXX"
	return &redacted
}
