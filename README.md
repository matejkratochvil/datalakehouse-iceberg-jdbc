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
   * **PostgreSQL:** Can be accessed on `localhost:5432` (Credentials: `iceberg`/`icebergpassword`, Database: `iceberg_catalog`)

4. **Run the Jupyter Notebook:**
   * Open `01-trino-iceberg-getting-started.ipynb` in JupyterLab.
   * Execute the cells to create schemas, tables, insert data, and query using Trino.

## Teardown

```bash
docker-compose down -v # -v removes volumes including MinIO data and PostgreSQL data
```

Next Steps

* Add Spark service with Iceberg configuration.
* Add Flink service with Iceberg configuration.
* Develop more advanced examples for each engine.

---
