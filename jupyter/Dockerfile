FROM jupyter/base-notebook:python-3.11

USER root

# Install Java (JDK 11)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk build-essential gcc && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install PyFlink and extras
RUN pip install apache-flink boto3 pandas

USER jovyan