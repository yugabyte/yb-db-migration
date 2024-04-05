#!/bin/bash
#   Copyright (c) YugabyteDB, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#	    http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

SCRIPT_DIR=$(dirname $0)
SCRIPT_NAME=$(basename $0)

HELP_TEXT="
Usage: $SCRIPT_NAME <pg_connection_string> <schema_list> <assessment_data_dir>

Collects PostgreSQL database statistics and schema information.
Note: The order of the arguments is important and must be followed.

Arguments:
  pg_connection_string   PostgreSQL connection string in the format:
                         'postgresql://username:password@hostname:port/dbname'
                         Ensure this string is properly quoted to avoid shell interpretation issues.

  schema_list            Comma-separated list of schemas for which statistics are to be collected.
                         Example: 'public,sales,inventory'

  assessment_data_dir    The directory path where the assessment data will be stored.
                         This script will attempt to create the directory if it does not exist.

Example:
  yb-voyager-gather-assessment-data.sh 'postgresql://user:pass@localhost:5432/mydatabase' 'public,sales' '/path/to/assessment/data'

Please ensure to replace the placeholders with actual values suited to your environment.
"

# Check for the --help option
if [ "$1" == "--help" ]; then
    echo "$HELP_TEXT"
    exit 0
fi

# Check if all required arguments are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $SCRIPT_NAME <pg_connection_string> <schema_list> <assessment_data_dir>"
    exit 1
fi

pg_connection_string=$1
schema_list=$2
assessment_data_dir=$3

echo "Assessment data collection started"
echo "Collecting table sizes..."
psql $pg_connection_string -f $SCRIPT_DIR/table-sizes.sql -v schema_list=$schema_list

echo "Collecting table iops stats..."
psql $pg_connection_string -f $SCRIPT_DIR/table-iops.sql -v schema_list=$schema_list

# TODO: finalize the query, approx count or exact count(any optimization also if possible)
echo "Collecting table row counts..."
psql $pg_connection_string -f $SCRIPT_DIR/table-row-counts.sql -v schema_list=$schema_list

# TODO: Test and handle(if required) the queries for case-sensitive and reserved keywords cases

# check for pg_dump version
pg_dump_version=$(pg_dump --version | awk '{print $3}' | awk -F. '{print $1}')
if [ "$pg_dump_version" -lt 14 ]; then
    echo "pg_dump version is less than 14. Please upgrade to version 14 or higher."
    exit 1
fi

echo "Collect schema information"
pg_dump $pg_connection_string --schema-only --schema=$schema_list --extension="*" --no-comments --no-owner --no-privileges --no-tablespaces --load-via-partition-root --file="$assessment_data_dir/schema/schema.sql"

echo "Assessment data collection completed"