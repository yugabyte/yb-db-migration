#!/usr/bin/env bash

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

if [ "${SOURCE_DB_TYPE}" != "" ]; then
	source ${SCRIPTS}/${SOURCE_DB_TYPE}/env.sh
fi

source ${SCRIPTS}/yugabytedb/env.sh
source ${SCRIPTS}/functions.sh

main() {
	echo "Deleting the parent export-dir present in the test directory"
	rm -rf ${EXPORT_DIR}	
	echo "Creating export-dir in the parent test directory"
	mkdir -p ${EXPORT_DIR}
	echo "Assigning permissions to the export-dir to execute init-db script"

	for script in init-db init-target-db generate_config.py; do
	  if [ -f "${TEST_DIR}/${script}" ]; then
		chmod +x "${TEST_DIR}/${script}"
	  fi
	done

	step "START: ${TEST_NAME}"
	print_env

	pushd ${TEST_DIR}

	step "Check the Voyager version installed"
	yb-voyager version

	step "Initialise databases"

	for script in init-db init-target-db; do
	  if [ -f "${TEST_DIR}/${script}" ]; then
	    "${TEST_DIR}/${script}"
	  fi
	done

	step "Run additional steps in case of offline"
	if [ "${SOURCE_DB_TYPE}" != "" ]; then
		step "Grant source database user permissions"
		grant_permissions ${SOURCE_DB_NAME} ${SOURCE_DB_TYPE} ${SOURCE_DB_SCHEMA}

		step "Export data."
		# false if exit code of export_data is non-zero
		export_data || { 
			cat_log_file "yb-voyager-export-data.log"
			cat_log_file "debezium-source_db_exporter.log"
			exit 1
		}
	fi

	step "Generate the YAML file"
	if [ -f "${TEST_DIR}/generate_config.py" ]; then
	  ./generate_config.py
	fi

	step "Run import with resumptions"

	${SCRIPTS}/resumption.py config.yaml

	step "Clean up"
	rm -rf "${EXPORT_DIR}"
	if [ -f "${TEST_DIR}/generate_config.py" ]; then
	  rm config.yaml
	fi
	if [ -n "${SOURCE_DB_NAME}" ]; then
		run_psql postgres "DROP DATABASE ${SOURCE_DB_NAME};"
	fi
	run_ysql yugabyte "DROP DATABASE IF EXISTS ${TARGET_DB_NAME};"
}

main
