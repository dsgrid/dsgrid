#!/bin/bash

# HPC-only script. Uses Apptainer.
# Imports the archived simple_standard_scenarios database from your local dsgrid-test-data repo
# into an ArangoDB instance.
#
# Assumes that the database is running on the current system. You can run this from a login node;
# in that case, specify the compute node's hostname that is running ArangoDB.

if [ -z $1 ]; then
    echo "Usage: bash restore_simple_standard_scenarios.sh PATH_TO_DSGRID_TEST_DATA [DB_HOSTNAME]"
    exit 1
fi

DSGRID_TEST_DATA=$(readlink -f $1)
if [ -z $2 ]; then
    DB_HOSTNAME=localhost
else
    DB_HOSTNAME=$2
fi
ARANGODB3_DIR=/scratch/$USER/arangodb3
ARANGODB3_APPS_DIR=/scratch/$USER/arangodb3-apps
ARANGODB_CONTAINER=/projects/dsgrid/containers/arangodb.sif

module load apptainer
echo "${DSGRID_TEST_DATA} ${ARANGODB3_DIR} ${ARANGODB3_APPS_DIR} ${ARANGODB_CONTAINER}"
apptainer run \
    -B ${DSGRID_TEST_DATA}:/dsgrid-test-data \
    -B ${ARANGODB3_DIR}:/var/lib/arangodb3 \
    -B ${ARANGODB3_APPS_DIR}:/var/lib/arangodb3-apps \
    ${ARANGODB_CONTAINER} \
    arangorestore \
    --create-database \
    --input-directory /dsgrid-test-data/filtered_registries/simple_standard_scenarios/dump/ \
    --include-system-collections true \
    --server.database simple-standard-scenarios \
    --server.endpoint="http+tcp://${DB_HOSTNAME}:8529"
