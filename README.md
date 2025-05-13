# Datalakehouse with Iceberg, MinIO, PostgreSQL, Trino, Spark, Flink, and JupyterLab

This project demonstrates a Docker Compose setup for a datalakehouse using Apache Iceberg.

## Phase 1: Core Services**

* **MinIO:** S3-compatible object storage for Iceberg table data.
* **PostgreSQL:** Stores Iceberg metadata using the JDBC catalog.
* **Trino:** Query engine to interact with Iceberg tables.
* **JupyterLab:** For running example notebooks (Python, Trino CLI via Python).

## Prerequisites

* Docker
* Docker Compose

### Setup (Phase 1)

1. **Clone the repository (or create the files as described).**
2. **Start the services:**

```bash
docker-compose up -d
```

3. **Access Services:**
    * **MinIO Console:** [http://localhost:9001](http://localhost:9001) (Credentials: `admin`/`password`)
        * You should see the `iceberg-warehouse` bucket created.
    * **Trino UI:** [http://localhost:8080](http://localhost:8080)
    * **JupyterLab:** [http://localhost:8888](http://localhost:8888) (Token: `icebergrocks`)
        * Navigate to the `work/` directory to find the notebooks.
    * **PostgreSQL:** Can be accessed on `localhost:5432` (Credentials: `iceberg`/`icebergpassword`,
      Database: `iceberg_catalog`)
    * **Flink UI:** [http://localhost:8081](http://localhost:8081)

4. **Run the Jupyter Notebook:**
    * Open `01-trino-iceberg-getting-started.ipynb` in JupyterLab.
    * Execute the cells to create schemas, tables, insert data, and query using Trino.

## Teardown

```bash
docker-compose down -v # -v removes volumes including MinIO data and PostgreSQL data
```

## Run Flink Example Applications

### Java Application

This example demonstrates how to run a Java Flink application that generates random Lord of the Rings records and
streams them into an Iceberg table using Flink. More information in `flink/java-app-example/README.md`.

1. **Build the Flink application:**
   ```bash
   cd flink/java-app-example
   ./gradlew clean shadowJar
   cd ../..
   ```
   This will create a fat jar in `build/libs/java-app-example-0.0.1-all.jar` (Prerequisite is installed Java 8+ (e.g.,
   OpenJDK 11)).
2. **Connect to the Trino service to run sql**
    ```bash
    docker exec -it trino-coordinator trino
    ```
3. **Using Trino, create the target DB**
    ```sql
    trino> CREATE SCHEMA IF NOT EXISTS iceberg.lor WITH (location = 's3a://iceberg-warehouse/lor/');
    ```
4. **Deploy the application to Flink cluster**
    ```bash
    docker cp ./flink/java-app-example/build/libs/java-app-example-0.0.1-all.jar flink-jobmanager:/opt/flink/java-app-example.jar
    docker exec -it flink-jobmanager bash -c "flink run /opt/flink/java-app-example.jar"
    ```
5. **Check UI if the app is running**
    * [http://localhost:8081/#/job/running](http://localhost:8081/#/job/running)
6. **See MinIO that the data and metadata are being produced**
    * [http://localhost:9001/browser/iceberg-warehouse/lor%2Fcharacter_sightings%2F](http://localhost:9001/browser/iceberg-warehouse/lor%2Fcharacter_sightings%2F)
7. **Check the data in Trino**
    ```sql
    trino> SELECT * FROM iceberg.lor.character_sightings LIMIT 10;
    ```
8. **Stop the Flink job (via cmd line or UI)**
    ```bash
    docker exec -it flink-jobmanager bash -c "flink list"
    docker exec -it flink-jobmanager bash -c "flink cancel f5467dbf8459b4f7f5c0df52ecbc4aa3"
    ```

Next Steps

* Add Spark service with Iceberg configuration.
* Add Flink service with Iceberg configuration.
* Develop more advanced examples for each engine.

---
