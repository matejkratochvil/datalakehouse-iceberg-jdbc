# Very basic POC of minio-iceberg-trino-flink-(spark) infra

## Start everything up

```shell
docker compose up
```

Connect to the Trino controller to execute some SQL:

```shell
docker compose exec controller trino
```

## Progress

### what's done

- `docker-compose` with minio, trino with iceberg connector and jdbc (`postgresql`) as an iceberg catalog

### TODO
  
0. trino
   - a. provide example DDL/DML trino statements for creating/populating iceberg tables (played with it locally a bit, will just take that and put it in some markdown or whatever)
   - b. add example iceberg statements (compaction etc.)
1. add flink to docker-compose
   - a. config iceberg with flink - DONE
   - b. add example flink job
      - create simple streaming source (like folder watcher or sth., mock kafka if there is an easy way?)
      - read the stream, write in minio as iceberg table
      - (read the iceberg table from minio in streaming fashion?)
   - c. add flink python-api example usage, similar to b.
2. add spark master/worker to docker-compose
   - a. setup iceberg as spark catalog
   - b. examples of read/write and iceberg operations
3. install jupyterlab (either as another service in docker-compose or in some image from steps 0., 1., 2.)
4. pyiceberg examples (load_catalog, etc.) and read/write with pyarrow/pandas
