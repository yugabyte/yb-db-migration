#!/usr/bin/env bash

# The script takes up to two arguments:
# 1. TEST_NAME: The name of the test to run (mandatory).
# 2. env.sh: An optional environment setup script to source before running the test. If not provided, this file will be picked from the ${TESTS_DIR}

set -e

if [ $# -gt 2 ]
then
	echo "Usage: $0 TEST_NAME [env.sh]"
	exit 1
fi

set -x

export YB_VOYAGER_SEND_DIAGNOSTICS=false
export TEST_NAME=$1

export REPO_ROOT="${PWD}"
export SCRIPTS="${REPO_ROOT}/migtests/scripts"
export TESTS_DIR="${REPO_ROOT}/migtests/tests"
export TEST_DIR="${TESTS_DIR}/${TEST_NAME}"
export EXPORT_DIR=${EXPORT_DIR:-"${TEST_DIR}/export-dir"}

export PYTHONPATH="${REPO_ROOT}/migtests/lib"

# Order of env.sh import matters.
if [ $2 != "" ] #if env.sh is passed as an argument, source it
then
    if [ ! -f "${TEST_DIR}/$2" ]
	then
		echo "$2 file not found in the test directory"
		exit 1
	fi
	source ${TEST_DIR}/$2
else
	source ${TEST_DIR}/env.sh
fi
source ${SCRIPTS}/${SOURCE_DB_TYPE}/env.sh

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

	step "Grant source database user permissions"
	grant_permissions ${SOURCE_DB_NAME} ${SOURCE_DB_TYPE} ${SOURCE_DB_SCHEMA}

	step "Check the Voyager version installed"
	yb-voyager version

	step "Assess Migration"
	assess_migration
	
	step "Validate Assessment Reports"
	# Checking if the assessment reports were created
	if [ -f "${EXPORT_DIR}/assessment/reports/assessmentReport.html" ] && [ -f "${EXPORT_DIR}/assessment/reports/assessmentReport.json" ]; then
		echo "Assessment reports created successfully."
		echo "Checking for Failures"
		validate_failure_reasoning "${EXPORT_DIR}/assessment/reports/assessmentReport.json"
		echo "Comparing Report contents"
        expected_file="${TEST_DIR}/expectedAssessmentReport.json"
        actual_file="${EXPORT_DIR}/assessment/reports/assessmentReport.json"
	    compare_assessment_reports ${expected_file} ${actual_file}
	else
		echo "Error: Assessment reports were not created successfully."
		cat_log_file "yb-voyager-assess-migration.log"
		exit 1
	fi

	step "End Migration: clearing metainfo about state of migration from everywhere."
	end_migration --yes
    # check if backup-dir has assessment report or not
	if [ -f "${EXPORT_DIR}/backup-dir/reports/assessmentReport.html" ] && [ -f "${EXPORT_DIR}/backup-dir/reports/assessmentReport.json" ]; then
		echo "End Migration saved Assessment Reports successfully."
	else
		echo "Assessment Reports were not saved by End Migration!"
		exit 1
	fi

	step "Clean up"
	./cleanup-db
	rm -rf "${EXPORT_DIR}/*"
}

main
