# This container only provides Spark. At some point when we stop churning so much we will make one
# that includes dsgrid software.

# USAGE:
# Do not run this while connected to the VPN. You may get a certificate error while downloading spark.
# If you have a Mac with Apple silicon (not Intel CPUs), set this environment variable so that the
# container will run on Intel CPUs.
#   $ export DOCKER_DEFAULT_PLATFORM=linux/amd64
# Build the container:
#   $ docker build --tag spark_py311 --build-arg VERSION=x.y.z .

# This container can be converted to an Apptainer container on Kestrel with these commands:
# Save and upload the docker image to Kestrel.
# $ docker save -o spark_py311_vx.y.z.tar spark_py311
# $ scp spark_py311_vx.y.z.tar <username>@kestrel.hpc.nrel.gov:/projects/dsgrid/containers
# Acquire a compute node with local storage (salloc --tmp=1600G).
# $ export APPTAINER_CACHEDIR=$TMPDIR
# $ module load apptainer
# Create writable image for testing and development or read-only image for production.
# Writable
# $ apptainer build --sandbox spark_py311 docker-archive://spark_py311_v0.1.0.tar
# Read-only
# $ apptainer build spark_py311_v0.1.0.sif docker-archive://spark_py311_v0.1.0.tar

# Note: Apache provides a container with Spark and Python installed, but as of now it is Python 3.9
# and dsgrid requires 3.10+. Whenever they have a newer Python, we can simplify this.
# The Apache container is
# FROM apache/spark-py

FROM python:3.11-slim
USER root

ARG VERSION
ARG SPARK_VERSION=4.0.0

RUN if [ -z "$VERSION" ]; then echo "VERSION must be specified"; exit 1; fi
ENV CONTAINER_VERSION ${VERSION}

# Install OpenSSH to communicate between containers, and dropbear SSH server
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y ca-certificates jq git nano default-jdk procps sysstat build-essential \
    tini tmux tree unzip vim wget openssh-client dropbear locales \
    && rm -rf /var/lib/apt/lists/*

# This prevents bash warnings on Kestrel.
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8
ENV LC_ALL en_US.UTF-8

# Required for dropbear
ENV SPARK_SSH_OPTS="-p 2222 -o StrictHostKeyChecking=no"

# Set Dropbear port to 2222 (or whatever port was selected above)
RUN sed -i -e 's@\(DROPBEAR_PORT=\).*@\12222@' /etc/default/dropbear

RUN mkdir /data
RUN mkdir /datasets
RUN mkdir /nopt
RUN mkdir /projects
RUN mkdir /scratch

RUN echo "$VERSION" > /opt/version.txt

WORKDIR /opt
ENV SPARK_HOME=/opt/spark

RUN wget --no-check-certificate https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
	&& tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz \
	&& rm spark-${SPARK_VERSION}-bin-hadoop3.tgz \
	&& mv spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
	&& cp ${SPARK_HOME}/conf/spark-defaults.conf.template ${SPARK_HOME}/conf/spark-defaults.conf \
	&& cp ${SPARK_HOME}/conf/spark-env.sh.template ${SPARK_HOME}/conf/spark-env.sh \
	&& cp ${SPARK_HOME}/kubernetes/dockerfiles/spark/entrypoint.sh /opt/entrypoint.sh \
	&& chmod +x ${SPARK_HOME}/conf/spark-env.sh

COPY postgresql-42.7.4.jar ${SPARK_HOME}/jars

RUN wget --no-check-certificate https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip \
    && rm duckdb_cli-linux-amd64.zip \
    && mv duckdb /usr/local/bin

RUN pip install ipython jupyter pandas pyarrow duckdb

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV PYSPARK_DRIVER_PYTHON=ipython

RUN touch $HOME/.profile \
    && rm -rf $HOME/.cache

ENTRYPOINT [ "/opt/entrypoint.sh" ]
