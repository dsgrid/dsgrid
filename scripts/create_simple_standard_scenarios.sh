#!/bin/bash

# Creates a simple-standard-scenarios registry using a dsgrid registry
# database containing the StandardScenarios project and data as well as
# files from the dsgrid and dsgrid-project-StandardScenarios repositories.
#
# You'll need to adjust these environment variables to match your registry
# and local repository paths.
export DSGRID_REGISTRY_DATABASE_URL=sqlite:///main-registry.db
export REPO_BASE=${HOME}/repos
export DSGRID_REPO=${REPO_BASE}/dsgrid
export SS_REPO=${REPO_BASE}/dsgrid-project-StandardScenarios

export SPARK_CLUSTER=spark://$(hostname):7077
export SPARK_CONF_DIR=$(pwd)/conf
export DSGRID_REGISTRY_SIMPLE_DB_URL=sqlite:///simple_standard_scenarios_registry.db
export SIMPLE_SS_DATA=$(pwd)/simple_standard_scenarios_data

rm -rf ${SIMPLE_SS_DATA} ${DUMP_DIR}

dsgrid-admin \
    --url http://${DSGRID_REGISTRY_DATABASE_URL} \
    make-filtered-registry \
    --src-database-url ${DSGRID_REGISTRY_DATABASE_URL} \
    --dst-database-url ${DSGRID_REGISTRY_SIMPLE_DB_URL} \
    ${SIMPLE_SS_DATA} \
    ${DSGRID_REPO}/dsgrid-test-data/filtered_registries/simple_standard_scenarios.json5
if [[ $? -ne 0 ]]; then
    echo "Failed to create the filtered registry"
    exit 1
fi

read -r -d "" USAGE << EOM

Created a registry database called ${DSGRID_REGISTRY_SIMPLE_DB_URL} with filtered StandardScenarios data.
The load data for the registry is at ${SIMPLE_SS_DATA}.
Run 'python tests/simple_standard_scenarios_datasets.py' to unpivot the ComStock datasets and
generate the summary of dataset stats for tests.
The changed files need to be committed in git and pushed back to GitHub.
Overwrite the relevant files in ${DSGRID_REPO}/dsgrid-test-data/filtered_registries/simple_standard_scenarios/ and open a pull request.
EOM
