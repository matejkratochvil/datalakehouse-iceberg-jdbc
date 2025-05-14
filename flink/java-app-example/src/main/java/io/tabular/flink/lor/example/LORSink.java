/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.iceberg.flink.lor.example;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

public class LORSink {
  private static final Logger LOGGER = LoggerFactory.getLogger(LORSink.class);

  public static void main(String[] args) throws Exception {
    InputStream inputStream = Thread.currentThread()
    .getContextClassLoader()
    .getResourceAsStream("config.properties");

    if (inputStream == null) {
        throw new FileNotFoundException("config.properties not found in classpath");
    }

    ParameterTool fileParams = ParameterTool.fromPropertiesFile(inputStream);
    ParameterTool cliParams = ParameterTool.fromArgs(args);
    ParameterTool parameters = fileParams.mergeWith(cliParams);

    System.out.println("Parsed parameters:");
    parameters.toMap().forEach((k, v) -> System.out.println(k + " = " + v));

    Configuration hadoopConf = new Configuration();

    // Set AWS credentials (must beset using System properties, check why)
    System.setProperty("aws.region", parameters.get("aws-region"));
    System.setProperty("aws.accessKeyId", parameters.get("aws-access-key"));
    System.setProperty("aws.secretAccessKey", parameters.get("aws-secret-key"));

    Map<String, String> catalogProperties = new HashMap<>();

    // JDBC Catalog settings
    catalogProperties.put("type", parameters.get("type"));
    catalogProperties.put("catalog-type", parameters.get("catalog-type"));
    catalogProperties.put("catalog-name", parameters.get("catalog-name"));
    catalogProperties.put("uri", parameters.get("uri"));
    catalogProperties.put("jdbc.user", parameters.get("jdbc-user"));
    catalogProperties.put("jdbc.password", parameters.get("jdbc-password"));
    catalogProperties.put("warehouse", parameters.get("warehouse"));

    // S3 and MinIO settings
    catalogProperties.put("io-impl", parameters.get("io-impl"));
    catalogProperties.put("aws.region", parameters.get("aws-region"));
    catalogProperties.put("aws.access-key-id", parameters.get("aws-access-key"));
    catalogProperties.put("aws.secret-access-key", parameters.get("aws-secret-key"));
    catalogProperties.put("s3.endpoint", parameters.get("s3-endpoint"));
    catalogProperties.put("s3.path-style-access", parameters.get("s3-path-style-access"));

    CatalogLoader catalogLoader = CatalogLoader.custom(
        parameters.get("catalog-name"),
        catalogProperties,
        hadoopConf,
        parameters.get("catalog-impl"));
    Schema schema = new Schema(
        Types.NestedField.required(1, "character", Types.StringType.get()),
        Types.NestedField.required(2, "location", Types.StringType.get()),
        Types.NestedField.required(3, "event_time", Types.TimestampType.withZone()));
    Catalog catalog = catalogLoader.loadCatalog();
    String databaseName = parameters.get("database", "lor");
    String tableName = parameters.get("table","character_sightings");
    TableIdentifier outputTable = TableIdentifier.of(
        databaseName,
        tableName);
    if (!catalog.tableExists(outputTable)) {
      catalog.createTable(outputTable, schema, PartitionSpec.unpartitioned());
    }
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(Integer.parseInt(parameters.get("checkpoint", "10000")));

    FakerLORSource source = new FakerLORSource();
    source.setEventInterval(Float.parseFloat(parameters.get("event_interval", "5000")));
    DataStream<Row> stream = env.addSource(source)
        .returns(TypeInformation.of(Map.class)).map(s -> {
          Row row = new Row(3);
          row.setField(0, s.get("character"));
          row.setField(1, s.get("location"));
          row.setField(2, s.get("event_time"));
          return row;
        });
    // Configure row-based append
    FlinkSink.forRow(stream, FlinkSchemaUtil.toSchema(schema))
        .tableLoader(TableLoader.fromCatalog(catalogLoader, outputTable))
        .distributionMode(DistributionMode.HASH)
        .writeParallelism(2)
        .append();
    // Execute the flink app
    env.execute();
  }
}
