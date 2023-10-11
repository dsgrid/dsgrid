#!/bin/bash
#SBATCH --account=dsgrid
#SBATCH --job-name=arangodb
#SBATCH --time=01:00:00
#SBATCH --output=arangodb_%j.o
#SBATCH --error=arangodb_%j.e
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

module load apptainer
apptainer run \
    -B ${ARANGODB3_DIR}:/var/lib/arangodb3 \
    -B ${ARANGODB3_APPS_DIR}:/var/lib/arangodb3-apps \
    --network-args "portmap=8529:8529" \
    --env "ARANGO_ROOT_PASSWORD=openSesame" \
    ${ARANGODB_CONTAINER}
