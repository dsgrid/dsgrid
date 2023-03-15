#!/bin/bash
export SPARK_CLUSTER=spark://$(hostname):7077
export SPARK_CONF_DIR=$(pwd)/conf
export REPO_BASE=${HOME}/repos
export DSGRID_REPO=${REPO_BASE}/dsgrid-db
export SS_REPO=${REPO_BASE}/dsgrid-project-StandardScenarios
export DSGRID_REGISTRY_DB_NAME=simple-standard-scenarios
export SIMPLE_SS_DATA=$(pwd)/simple_standard_scenarios-data
export QUERY_OUTPUT=$(pwd)/query_output
export COMSTOCK_DD=$(pwd)/comstock_projected_derived_dataset
export RESSTOCK_DD=$(pwd)/resstock_projected-derived_dataset
export TEMPO_DD=$(pwd)/tempo_mapped_derived_dataset
export DUMP_DIR=$(pwd)/simple_standard_scenarios_dump

rm -rf ${QUERY_OUTPUT} ${SIMPLE_SS_DATA} ${COMSTOCK_DD} ${RESSTOCK_DD} ${TEMPO_DD} ${DUMP_DIR}

dsgrid-admin make-filtered-registry \
    --src-db-name standard-scenarios \
    --dst-db-name ${DSGRID_REGISTRY_DB_NAME} \
    --url http://localhost:8529 \
    ${SIMPLE_SS_DATA} \
    ${DSGRID_REPO}/dsgrid-test-data/filtered_registries/simple_standard_scenarios.json
if [[ $? -ne 0 ]]; then
    echo "Failed to create the filtered registry"
    exit 1
fi

# Create derived datasets and submit them.
spark-submit \
    --master=${SPARK_CLUSTER} \
    $(which dsgrid-cli.py) \
    query project run \
    --db-name=${DSGRID_REGISTRY_DB_NAME} \
    --offline \
    ${SS_REPO}/dsgrid_project/derived_datasets/comstock_conus_2022_projected.json5 \
    -o ${QUERY_OUTPUT}
if [[ $? -ne 0 ]]; then
    echo "Failed to create comstock projected."
    exit 1
fi

spark-submit \
    --master=${SPARK_CLUSTER} \
    $(which dsgrid-cli.py) \
    query project run \
    --db-name=${DSGRID_REGISTRY_DB_NAME} \
    --offline \
    ${SS_REPO}/dsgrid_project/derived_datasets/resstock_conus_2022_projected.json5 \
    -o ${QUERY_OUTPUT}
if [[ $? -ne 0 ]]; then
    echo "Failed to create resstock projected."
    exit 1
fi

spark-submit \
    --master=${SPARK_CLUSTER} \
    $(which dsgrid-cli.py) \
    query project run \
    --db-name=${DSGRID_REGISTRY_DB_NAME} \
    --offline \
    ${SS_REPO}/dsgrid_project/derived_datasets/tempo_conus_2022_mapped.json5 \
    -o ${QUERY_OUTPUT}
if [[ $? -ne 0 ]]; then
    echo "Failed to create tempo mapped."
    exit 1
fi

# Create derived-dataset configurations.
spark-submit \
    --master=${SPARK_CLUSTER} \
    $(which dsgrid-cli.py) \
    query project create-derived-dataset-config \
    --db-name=${DSGRID_REGISTRY_DB_NAME} \
    --offline \
    ${QUERY_OUTPUT}/comstock_conus_2022_projected \
    ${COMSTOCK_DD}
if [[ $? -ne 0 ]]; then
    echo "Failed to create derived dataset config comstock."
    exit 1
fi

spark-submit \
    --master=${SPARK_CLUSTER} \
    $(which dsgrid-cli.py) \
    query project create-derived-dataset-config \
    --db-name=${DSGRID_REGISTRY_DB_NAME} \
    --offline \
    ${QUERY_OUTPUT}/resstock_conus_2022_projected \
    ${RESSTOCK_DD}
if [[ $? -ne 0 ]]; then
    echo "Failed to create derived dataset config resstock."
    exit 1
fi

spark-submit \
    --master=${SPARK_CLUSTER} \
    $(which dsgrid-cli.py) \
    query project create-derived-dataset-config \
    --db-name=${DSGRID_REGISTRY_DB_NAME} \
    --offline \
    ${QUERY_OUTPUT}/tempo_conus_2022_mapped \
    ${TEMPO_DD}
if [[ $? -ne 0 ]]; then
    echo "Failed to create derived dataset config tempo."
    exit 1
fi

# Register and submit the datasets.
spark-submit \
    --master=${SPARK_CLUSTER} \
    $(which dsgrid-cli.py) \
    registry \
    --db-name=${DSGRID_REGISTRY_DB_NAME} \
    --offline \
    datasets \
    register \
    ${COMSTOCK_DD}/dataset.json5 \
    ${QUERY_OUTPUT}/comstock_conus_2022_projected \
    -l "Register comstock_conus_2022_projected"
if [[ $? -ne 0 ]]; then
    echo "Failed to register comstock."
    exit 1
fi

spark-submit \
    --master=${SPARK_CLUSTER} \
    $(which dsgrid-cli.py) \
    registry \
    --db-name=${DSGRID_REGISTRY_DB_NAME} \
    --offline \
    projects \
    submit-dataset \
    -p dsgrid_conus_2022 \
    -d comstock_conus_2022_projected \
    -r ${COMSTOCK_DD}/dimension_mapping_references.json5 \
    -l "Submit comstock_conus_2022_projected"
if [[ $? -ne 0 ]]; then
    echo "Failed to submit comstock."
    exit 1
fi

spark-submit \
    --master=${SPARK_CLUSTER} \
    $(which dsgrid-cli.py) \
    registry \
    --offline \
    --db-name=${DSGRID_REGISTRY_DB_NAME} \
    datasets \
    register \
    ${RESSTOCK_DD}/dataset.json5 \
    ${QUERY_OUTPUT}/resstock_conus_2022_projected \
    -l "Register resstock_conus_2022_projected"
if [[ $? -ne 0 ]]; then
    echo "Failed to register resstock."
    exit 1
fi

spark-submit \
    --master=${SPARK_CLUSTER} \
    $(which dsgrid-cli.py) \
    registry \
    --db-name=${DSGRID_REGISTRY_DB_NAME} \
    --offline \
    projects \
    submit-dataset \
    -p dsgrid_conus_2022 \
    -d resstock_conus_2022_projected \
    -r ${RESSTOCK_DD}/dimension_mapping_references.json5 \
    -l "Submit resstock_conus_2022_projected"
if [[ $? -ne 0 ]]; then
    echo "Failed to submit resstock."
    exit 1
fi

spark-submit \
    --master=${SPARK_CLUSTER} \
    $(which dsgrid-cli.py) \
    registry \
    --offline \
    --db-name=${DSGRID_REGISTRY_DB_NAME} \
    datasets \
    register \
    ${TEMPO_DD}/dataset.json5 \
    ${QUERY_OUTPUT}/tempo_conus_2022_mapped \
    -l "Register tempo_conus_2022_mapped"
if [[ $? -ne 0 ]]; then
    echo "Failed to register tempo."
    exit 1
fi

spark-submit \
    --master=${SPARK_CLUSTER} \
    $(which dsgrid-cli.py) \
    registry \
    --offline \
    --db-name=${DSGRID_REGISTRY_DB_NAME} \
    projects \
    submit-dataset \
    -p dsgrid_conus_2022 \
    -d tempo_conus_2022_mapped \
    -r ${TEMPO_DD}/dimension_mapping_references.json5 \
    -l "Submit tempo_conus_2022_mapped"
if [[ $? -ne 0 ]]; then
    echo "Failed to submit tempo."
    exit 1
fi

module load singularity
singularity run \
    -B /scratch:/scratch \
    /projects/dsgrid/containers/arangodb.sif \
    arangodump --server.database \
    --db-name=${DSGRID_REGISTRY_DB_NAME} \
    --server.password openSesame \
    --output-directory $(pwd)/$(DUMP_DIR) \
    --compress-output false \
    --include-system-collections true

echo ""
echo "Created a registry database called ${DSGRID_REGISTRY_DB_NAME} with filtered StandardScenarios data."
echo "The registry dumped in JSON format is at ${DUMP_DIR}."
echo "The load data for the registry is at ${SIMPLE_SS_DATA}."
echo "The changed files need to be committed in git and pushed back to GitHub."
echo "Overwrite the relevant files in ${DSGRID_REPO}/dsgrid-test-data/filtered_registries/simple_standard_scenarios/ and open a pull request."
