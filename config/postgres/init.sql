-- config/postgres/init.sql
-- No special DDL needed here for Iceberg JDBC catalog itself,
-- the Iceberg library will create the necessary tables (e.g., iceberg_tables, iceberg_namespaces)
-- within the 'iceberg_catalog' database when it's first used.
-- This script primarily ensures the database and user exist, which is handled by
-- POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB environment variables in docker-compose.

SELECT 'Database iceberg_catalog and user iceberg are expected to be created by Docker entrypoint based on environment variables.';
-- You could add: CREATE SCHEMA IF NOT EXISTS iceberg; -- if you want a specific schema for catalog tables
-- but Iceberg usually manages this within its default "public" or based on connection string.
