#!/usr/bin/env bash

set -e
set -x

if [ $# -gt 3 ]; then
    echo "Usage: $0 TEST_TYPE TEST_NAME [env.sh]"
    exit 1
fi

export YB_VOYAGER_SEND_DIAGNOSTICS=false
export TEST_TYPE=$1
export TEST_NAME=$2

export REPO_ROOT="${PWD}"
export SCRIPTS="${REPO_ROOT}/migtests/scripts"
export TESTS_DIR="${REPO_ROOT}/migtests/tests"
export TEST_TYPE_DIR="${TESTS_DIR}/upgrade-tests/${TEST_TYPE}"
export TEST_DIR="${TESTS_DIR}/${TEST_NAME}"
export EXPORT_DIR=${EXPORT_DIR:-"${TEST_DIR}/export-dir"}
export PYTHONPATH="${REPO_ROOT}/migtests/lib"
export LAST_BREAKING_RELEASE=${LAST_BREAKING_RELEASE:-"1.8.5"}
export RELEASE_TO_UPGRADE_TO=${RELEASE_TO_UPGRADE_TO:-"local"}

# Load environment configurations
if [ $3 != "" ]; then
    if [ ! -f "${TEST_DIR}/$3" ]; then
        echo "$3 file not found in the test directory"
        exit 1
    fi
    source ${TEST_DIR}/$3
else
    source ${TEST_DIR}/env.sh
fi
source ${SCRIPTS}/${SOURCE_DB_TYPE}/env.sh
source ${SCRIPTS}/yugabytedb/env.sh
source ${SCRIPTS}/functions.sh

if [ -n "${SOURCE_DB_SSL_MODE}" ]; then
   export EXPORT_DIR="${EXPORT_DIR}_ssl"
   export SOURCE_DB_NAME="${SOURCE_DB_NAME}_ssl"
   export TARGET_DB_NAME="${TARGET_DB_NAME}_ssl"
fi

main() {
    echo ${REPO_ROOT}

    step "START: ${TEST_NAME}"
    print_env

    pushd ${TEST_DIR}

    # yes | ${REPO_ROOT}/installer_scripts/install-yb-voyager --version ${RELEASE_TO_UPGRADE_TO}
    # source ~/.bashrc

    step "Check Voyager version after upgrade."
    yb-voyager version
    verify_voyager_version ${RELEASE_TO_UPGRADE_TO}

    # Run after steps
    run_script "${TEST_TYPE_DIR}/after"

    # Clean up
    step "Cleaning up."
    ./cleanup-db
    rm -rf "${EXPORT_DIR}"
    run_ysql yugabyte "DROP DATABASE IF EXISTS ${TARGET_DB_NAME};"
}

main