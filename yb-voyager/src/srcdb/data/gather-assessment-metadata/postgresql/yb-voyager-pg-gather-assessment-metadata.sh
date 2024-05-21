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

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE:-$0}" )" &> /dev/null && pwd ) 
SCRIPT_NAME=$(basename $0)

HELP_TEXT="
Usage: $SCRIPT_NAME <pg_connection_string> <schema_list> <assessment_metadata_dir>

Collects PostgreSQL database statistics and schema information.
Note: The order of the arguments is important and must be followed.

Arguments:
  pg_connection_string        PostgreSQL connection string in the format:
                             'postgresql://username@hostname:port/dbname'
                              Ensure this string is properly quoted to avoid shell interpretation issues.

  schema_list                 Pipe-separated list of schemas for which statistics are to be collected.
                              Example: 'public|sales|inventory'

  assessment_metadata_dir     The directory path where the assessment metadata will be stored.
                              This script will attempt to create the directory if it does not exist.

  iops_capture_interval   This argument is used to configure the interval for measuring the IOPS 
                               metadata on source (in seconds). (Default 120)
                            

Example:
  PGPASSWORD=<password> $SCRIPT_NAME 'postgresql://user@localhost:5432/mydatabase' 'public|sales' '/path/to/assessment/metadata' '60'

Please ensure to replace the placeholders with actual values suited to your environment.
"

# Check for the --help option
if [ "$1" == "--help" ]; then
    echo "$HELP_TEXT"
    exit 0
fi

# Check if all required arguments are provided
if [ "$#" -lt 3 ]; then
    echo "Usage: $0 <pg_connection_string> <schema_list> <assessment_metadata_dir> [iops_capture_interval]"
    exit 1
elif [ "$#" -gt 4 ]; then
    echo "Usage: $0 <pg_connection_string> <schema_list> <assessment_metadata_dir> [iops_capture_interval]"
    exit 1
fi

# Set default iops interval
iops_capture_interval=120

# Override default sleep interval if a fourth argument is provided
if [ "$#" -eq 4 ]; then
    iops_capture_interval=$4
    echo "Using sleep interval: $iops_capture_interval seconds"
fi


pg_connection_string=$1
schema_list=$2
assessment_metadata_dir=$3


# check if assessment_metadata_dir exists, if not exit 1
if [ ! -d "$assessment_metadata_dir" ]; then
    echo "Directory $assessment_metadata_dir does not exist. Please create the directory and try again."
    exit 1
fi

# Switch to assessment_metadata_dir and remember the current directory
pushd "$assessment_metadata_dir" > /dev/null || exit

if [ -z "$PGPASSWORD" ]; then 
    echo -n "Enter PostgreSQL password: "
    read -s PGPASSWORD
    echo
    export PGPASSWORD
fi


track_counts_on=$(psql $pg_connection_string -tAqc "SELECT setting FROM pg_settings WHERE name = 'track_counts';")
if [ "$track_counts_on" != "on" ]; then
    echo "Warning: track_counts is not enabled in the PostgreSQL configuration."
    echo "It's required for calculating reads/writes per second stats of tables/indexes. Do you still want to continue? (Y/N): "
    read continue_execution
    continue_execution=$(echo "$continue_execution" | tr '[:upper:]' '[:lower:]') # converting to lower case for easier comparison
    if [ "$continue_execution" != "yes" ] && [ "$continue_execution" != "y" ]; then
        echo "Exiting..."
        exit 2
    fi
fi


echo "Assessment metadata collection started"

# TODO: Test and handle(if required) the queries for case-sensitive and reserved keywords cases
for script in $SCRIPT_DIR/*.psql; do
    script_name=$(basename "$script" .psql)
    script_action=$(basename "$script" .psql | sed 's/-/ /g')
    echo "Collecting $script_action..."
    if [ $script_name == "table-index-iops" ]; then
        psql -q $pg_connection_string -f $script -v schema_list=$schema_list -v ON_ERROR_STOP=on -v measurement_type=initial
        mv table-index-iops.csv table-index-iops-initial.csv
        
        # sleeping to calculate the iops reading two different time intervals, to calculate reads_per_second and writes_per_second
        sleep $iops_capture_interval 
        
        psql -q $pg_connection_string -f $script -v schema_list=$schema_list -v ON_ERROR_STOP=on -v measurement_type=final -v filename=$script_name-initial.csv
        mv table-index-iops.csv table-index-iops-final.csv
    else
        psql -q $pg_connection_string -f $script -v schema_list=$schema_list -v ON_ERROR_STOP=on
    fi
done

# check for pg_dump version
pg_dump_version=$(pg_dump --version | awk '{print $3}' | awk -F. '{print $1}')
if [ "$pg_dump_version" -lt 14 ]; then
    echo "pg_dump version is less than 14. Please upgrade to version 14 or higher."
    exit 1
fi

mkdir -p schema
echo "Collecting schema information..."
pg_dump $pg_connection_string --schema-only --schema=$schema_list --extension="*" --no-comments --no-owner --no-privileges --no-tablespaces --load-via-partition-root --file="schema/schema.sql"

# Return to the original directory after operations are done
popd > /dev/null

echo "Assessment metadata collection completed"