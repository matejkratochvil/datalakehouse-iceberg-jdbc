FROM flink:1.20.1-scala_2.12-java11

ARG ICEBERG_VERSION=1.9.0

RUN mkdir -p /opt/flink/usrlib \
 && wget -P /opt/flink/usrlib \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
 && wget -P /opt/flink/lib \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
 && rm -f /opt/flink/lib/aws-*.jar /opt/flink/lib/awssdk-*.jar

COPY flink-conf.yaml /opt/flink/conf/
