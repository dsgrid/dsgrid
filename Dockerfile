# USAGE:
# docker build --secret id=access_token,env=ACCESS_TOKEN --tag dsgrid --build-arg VERSION=x.y.z .
# where ACCESS_TOKEN is a GitHub token that can clone the dsgrid repository

# This container can be converted to a Singularity container on Eagle with these commands:
# Acquire a compute node.
# $ export SINGULARITY_TMPDIR=/tmp/scratch
# $ module load singularity-container
# Writable image for testing and development:
# $ singularity build --sandbox dsgrid docker-archive://dsgrid_v0.1.0.tar
# Read-only image for production:
# $ singularity build dsgrid_v0.1.0.sif docker-archive://disco_v0.1.0.tar

FROM python:3.8-slim

ARG VERSION
ARG SPARK_VERSION=3.2.0
ARG HADOOP_VERSION=3.2
ARG FULL_STR=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ARG SPARK_HOME=/repos/${FULL_STR}

RUN if [ -z "$VERSION" ]; then echo "VERSION must be specified"; exit 1; fi

USER root

RUN apt-get update \
    && apt-get install -y jq git nano openjdk-11-jdk sysstat tmux tree vim wget zsh \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /data
RUN mkdir /nopt
RUN mkdir /projects
RUN mkdir /repos
RUN mkdir /scratch

COPY docker/vimrc $HOME/.vimrc
COPY docker/vimrc /data/vimrc

RUN echo "$VERSION" > /repos/version.txt

RUN --mount=type=secret,id=access_token ACCESS_TOKEN=$(cat /run/secrets/access_token) \
    && git config --global url."https://${ACCESS_TOKEN}:@github.com/".insteadOf "https://github.com/"

WORKDIR /repos
# TODO: Find out how to install proper certificates.
RUN wget --no-check-certificate https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${FULL_STR}.tgz \
	&& tar -xzf ${FULL_STR}.tgz \
	&& rm ${FULL_STR}.tgz \
	&& echo "export PATH=$PATH:/repos/${FULL_STR}/bin:/repos/${FULL_STR}/sbin" >> /root/.bashrc \
	&& echo "export SPARK_HOME=/repos/${FULL_STR}" >> /root/.bashrc \
	&& cp /repos/${FULL_STR}/conf/spark-defaults.conf.template /repos/${FULL_STR}/conf/spark-defaults.conf \
	&& cp /repos/${FULL_STR}/conf/spark-env.sh.template /repos/${FULL_STR}/conf/spark-env.sh \
	&& chmod +x /repos/${FULL_STR}/conf/spark-env.sh

RUN git clone https://github.com/dsgrid/dsgrid.git
RUN pip install -e /repos/dsgrid
RUN pip install ipython

RUN touch $HOME/.profile \
    && rm -rf $HOME/.cache

WORKDIR /data
CMD [ "bash" ]
