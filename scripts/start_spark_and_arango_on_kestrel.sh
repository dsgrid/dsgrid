#!/bin/bash
#SBATCH --account=dsgrid
#SBATCH --job-name=dsgrid_infra
#SBATCH --time=01:00:00
#SBATCH --output=dsgrid_infra_%j.o
#SBATCH --error=dsgrid_infra_%j.e
#SBATCH --nodes=1
#SBATCH --partition=debug

ARANGODB3_DIR=/scratch/$USER/arangodb3
ARANGODB3_APPS_DIR=/scratch/$USER/arangodb3-apps
ARANGODB_CONTAINER=/projects/dsgrid/containers/arangodb.sif

if [ ! -d ${ARANGODB3_DIR} ]; then
    mkdir ${ARANGODB3_DIR}
fi
if [ ! -d ${ARANGODB3_APPS_DIR} ]; then
    mkdir ${ARANGODB3_APPS_DIR}
fi

if [ -z ${1} ]; then
    HPC_REPO_DIR=${HOME}/repos/HPC
else
    HPC_REPO_DIR=${1}
fi

SCRIPT_DIR=${HPC_REPO_DIR}/applications/spark/spark_scripts
${SCRIPT_DIR}/configure_and_start_spark.sh -M 10 -c /datasets/images/apache_spark/spark351_py312.sif

printf "\nThe Spark cluster is running at spark://$(hostname):7077 from a configuration at $(pwd)/conf\n\n"
printf "Run this command in your environment to use the same configuration:\n\n"
printf "export SPARK_CONF_DIR=$(pwd)/conf\n\n"
printf "Starting ArangoDB\n\n"

module load apptainer
apptainer run \
    -B ${ARANGODB3_DIR}:/var/lib/arangodb3 \
    -B ${ARANGODB3_APPS_DIR}:/var/lib/arangodb3-apps \
    --network-args "portmap=8529:8529" \
    --env "ARANGO_ROOT_PASSWORD=openSesame" \
    ${ARANGODB_CONTAINER}
