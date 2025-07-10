FROM quay.io/jupyter/base-notebook:python-3.12.8

USER root
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root

# Spark dependencies
# Default values can be overridden at build time
# (ARGS are in lowercase to distinguish them from ENV)
ARG openjdk_version="17"

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    "openjdk-${openjdk_version}-jdk" \
    ca-certificates-java && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# If spark_version is not set, latest Spark will be installed
ARG spark_version="3.5.6"
ARG hadoop_version="3"
# If scala_version is not set, Spark without Scala will be installed
ARG scala_version="2.12"
# URL to use for Spark downloads
ARG spark_download_url="https://archive.apache.org/dist/spark/"

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt
USER jovyan
