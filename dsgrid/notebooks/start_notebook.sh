#!/bin/bash
unset XDG_RUNTIME_DIR
export SPARK_CLUSTER=$1
export DSGRID_LOG_FILE_PATH=`pwd`/$2
echo "Spark cluster is running at ${SPARK_CLUSTER}" >&2
echo "JADE output directory is ${DSGRID_LOG_FILE_PATH}" >&2
mkdir -p $DSGRID_LOG_FILE_PATH
jupyter notebook --no-browser --ip=0.0.0.0 --port 8889 &
sleep 10
echo "Create an ssh tunnel with this command: ssh -L 8889:${HOSTNAME}:8889 ${USER}@el1.hpc.nrel.gov" >&2
wait
