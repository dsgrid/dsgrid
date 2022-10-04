# USAGE:
# Do not run this while connected to the VPN. You may get a certificate error while downloading spark.
# docker build --tag dsgrid --build-arg VERSION=x.y.z .

# This container can be converted to a Singularity container on Eagle with these commands:
# Save and upload the docker image to Eagle.
# $ docker save -o dsgrid_vx.y.z.tar dsgrid
# $ scp dsgrid_vx.y.z.tar <username>@eagle.hpc.nrel.gov:/projects/dsgrid/containers
# Acquire a compute node.
# $ export SINGULARITY_TMPDIR=/tmp/scratch
# $ module load singularity-container
# Create writable image for testing and development or read-only image for production.
# Writable
# $ singularity build --sandbox dsgrid docker-archive://dsgrid_v0.1.0.tar
# Read-only
# $ singularity build dsgrid_v0.1.0.sif docker-archive://disco_v0.1.0.tar

FROM python:3.10-slim

ARG VERSION
ARG SPARK_VERSION=3.3.0
ARG HADOOP_VERSION=3
ARG FULL_STR=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}

RUN if [ -z "$VERSION" ]; then echo "VERSION must be specified"; exit 1; fi
ENV CONTAINER_VERSION ${VERSION}

USER root

RUN apt-get update \
    && apt-get install -y ca-certificates jq git nano default-jdk procps sysstat tmux tree vim wget zsh \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /data
RUN mkdir /nopt
RUN mkdir /projects
RUN mkdir /scratch

COPY docker/vimrc $HOME/.vimrc
COPY docker/vimrc /data/vimrc

RUN echo "$VERSION" > /opt/version.txt

WORKDIR /opt
RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${FULL_STR}.tgz \
	&& tar -xzf ${FULL_STR}.tgz \
	&& rm ${FULL_STR}.tgz \
	&& cp /opt/${FULL_STR}/conf/spark-defaults.conf.template /opt/${FULL_STR}/conf/spark-defaults.conf \
	&& cp /opt/${FULL_STR}/conf/spark-env.sh.template /opt/${FULL_STR}/conf/spark-env.sh \
	&& chmod +x /opt/${FULL_STR}/conf/spark-env.sh

RUN pip install ipython jupyter

ENV SPARK_HOME=/opt/${FULL_STR}
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin


RUN touch $HOME/.profile \
    && rm -rf $HOME/.cache

WORKDIR /data
CMD [ "bash" ]
