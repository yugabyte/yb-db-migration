#!/usr/bin/env bash

set -e
set -m

if [ $# -ne 1 ]
then
	echo "Usage: $0 TEST_NAME"
	exit 1
fi

set -x

export TEST_NAME=$1

export REPO_ROOT="${PWD}"
export SCRIPTS="${REPO_ROOT}/migtests/scripts"
export TESTS_DIR="${REPO_ROOT}/migtests/tests"
export TEST_DIR="${TESTS_DIR}/${TEST_NAME}"
export EXPORT_DIR=${EXPORT_DIR:-"${TEST_DIR}/export-dir"}

export PYTHONPATH="${REPO_ROOT}/migtests/lib"

# Order of env.sh import matters.
source ${TEST_DIR}/env.sh

if [ "${SOURCE_DB_TYPE}" = "oracle" ]
then
	source ${SCRIPTS}/${SOURCE_DB_TYPE}/live_env.sh
	source ${SCRIPTS}/${SOURCE_DB_TYPE}/ff_env.sh
else
	source ${SCRIPTS}/${SOURCE_DB_TYPE}/env.sh
fi

source ${SCRIPTS}/yugabytedb/env.sh

source ${SCRIPTS}/functions.sh

main() {

	echo "Deleting the parent export-dir present in the test directory"
	rm -rf ${EXPORT_DIR}	
	echo "Creating export-dir in the parent test directory"
	mkdir -p ${EXPORT_DIR}
	echo "Assigning permissions to the export-dir to execute init-db, cleanup-db scripts"
	chmod +x ${TEST_DIR}/init-db ${TEST_DIR}/cleanup-db

	step "START: ${TEST_NAME}"
	print_env

	pushd ${TEST_DIR}

	step "Initialise source database."
	./init-db

	step "Grant source database user permissions for live migration"	
	grant_permissions_for_live_migration

	step "Check the Voyager version installed"
	yb-voyager version

	step "Export schema."
	export_schema
	find ${EXPORT_DIR}/schema -name '*.sql' -printf "'%p'\n"| xargs grep -wh CREATE

	step "Analyze schema."
	analyze_schema
	tail -20 ${EXPORT_DIR}/reports/schema_analysis_report.txt

	step "Fix schema."
	if [ -x "${TEST_DIR}/fix-schema" ]
	then
		 "${TEST_DIR}/fix-schema"
	fi

	step "Analyze schema."
	analyze_schema
	tail -20 ${EXPORT_DIR}/reports/schema_analysis_report.txt

	step "Create target database."
	run_ysql yugabyte "DROP DATABASE IF EXISTS ${TARGET_DB_NAME};"
	run_ysql yugabyte "CREATE DATABASE ${TARGET_DB_NAME}"

	if [ -x "${TEST_DIR}/add-pk-from-alter-to-create" ]
	then
		"${TEST_DIR}/add-pk-from-alter-to-create"
	fi

	step "Import schema."
	import_schema
	run_ysql ${TARGET_DB_NAME} "\dt"

	step "Export data."
	# false if exit code of export_data is non-zero
	export_data --export-type "snapshot-and-changes" || { 
		tail_log_file "yb-voyager-export-data.log"
		tail_log_file "debezium-source_db_exporter.log"
		exit 1
	} &

	# Storing the pid for the export data command
	exp_pid=$!

	# Killing the export process in case of failure
	trap "kill_process -${exp_pid} ; exit 1" SIGINT SIGTERM EXIT SIGSEGV SIGHUP

	# Waiting for snapshot to complete
	timeout 100 bash -c -- 'while [ ! -f ${EXPORT_DIR}/metainfo/dataFileDescriptor.json ]; do sleep 3; done'

	ls -l ${EXPORT_DIR}/data
	cat ${EXPORT_DIR}/data/export_status.json || echo "No export_status.json found."
	cat ${EXPORT_DIR}/metainfo/dataFileDescriptor.json

	step "Import data."
	import_data || { 
		tail_log_file "yb-voyager-import-data.log"
		exit 1
	} &

	# Storing the pid for the import data command
	imp_pid=$!

	# Updating the trap command to include the importer
	trap "kill_process -${exp_pid} ; kill_process -${imp_pid} ; exit 1" SIGINT SIGTERM EXIT SIGSEGV SIGHUP

	step "Archive Changes."
	archive_changes &

	sleep 30 

	step "Import remaining schema (FK, index, and trigger) and Refreshing MViews if present."
	import_schema --post-snapshot-import true --refresh-mviews=true

	step "Run snapshot validations."
	"${TEST_DIR}/validate" --live_migration 'true' --ff_enabled 'false' --fb_enabled 'true'

	step "Inserting new events"
	run_sql_file source_delta.sql

	sleep 120

	# Resetting the trap command
	trap - SIGINT SIGTERM EXIT SIGSEGV SIGHUP

	step "Setup Fall Back environment"
	setup_fallback_environment

	step "Initiating cutover"
	yb-voyager initiate cutover to target --export-dir ${EXPORT_DIR} --prepare-for-fall-back true --yes

	for ((i = 0; i < 15; i++)); do
    if [ "$(yb-voyager cutover status --export-dir "${EXPORT_DIR}" | grep "cutover to target status" | cut -d ':'  -f 2 | tr -d '[:blank:]')" != "COMPLETED" ]; then
        echo "Waiting for cutover to be COMPLETED..."
        sleep 20
        if [ "$i" -eq 14 ]; then
            tail_log_file "yb-voyager-export-data.log"
            tail_log_file "yb-voyager-import-data.log"
			tail_log_file "debezium-source_db_exporter.log"
			exit 1
        fi
    else
        break
    fi
	done
	
	sleep 60

	step "Inserting new events to YB"
	ysql_import_file ${TARGET_DB_NAME} target_delta.sql

	sleep 60

	step "Resetting the trap command"
	trap - SIGINT SIGTERM EXIT SIGSEGV SIGHUP

	step "Initiating cutover to source"
	yb-voyager initiate cutover to source --export-dir ${EXPORT_DIR} --yes

	for ((i = 0; i < 10; i++)); do
    if [ "$(yb-voyager cutover status --export-dir "${EXPORT_DIR}" | grep "cutover to source status" | cut -d ':'  -f 2 | tr -d '[:blank:]')"  != "COMPLETED" ]; then
        echo "Waiting for switchover to be COMPLETED..."
        sleep 20
        if [ "$i" -eq 9 ]; then
            tail_log_file "yb-voyager-import-data-to-source.log"
            tail_log_file "yb-voyager-export-data-from-target.log"
			tail_log_file "debezium-target_db_exporter_fb.log"
			exit 1
        fi
    else
        break
    fi
	done

	run_ysql ${TARGET_DB_NAME} "\di"
	run_ysql ${TARGET_DB_NAME} "\dft" 

	step "Run final validations."
	if [ -x "${TEST_DIR}/validateAfterChanges" ]
	then
	"${TEST_DIR}/validateAfterChanges" --fb_enabled 'true' --ff_enabled 'false'
	fi

	step "End Migration: clearing metainfo about state of migration from everywhere"
	end_migration --yes

	step "Clean up"
	./cleanup-db
	rm -rf "${EXPORT_DIR}/*"
	run_ysql yugabyte "DROP DATABASE IF EXISTS ${TARGET_DB_NAME};"
}

main
