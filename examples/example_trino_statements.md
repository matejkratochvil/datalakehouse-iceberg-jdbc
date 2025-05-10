```sql
trino> help

Supported commands:
QUIT
EXIT
CLEAR
EXPLAIN [ ( option [, ...] ) ] <query>
    options: FORMAT { TEXT | GRAPHVIZ | JSON }
             TYPE { LOGICAL | DISTRIBUTED | VALIDATE | IO }
DESCRIBE <table>
SHOW COLUMNS FROM <table>
SHOW FUNCTIONS
SHOW CATALOGS [LIKE <pattern>]
SHOW SCHEMAS [FROM <catalog>] [LIKE <pattern>]
SHOW TABLES [FROM <schema>] [LIKE <pattern>]
USE [<catalog>.]<schema>
```

```sql
trino> show catalogs;
 Catalog 
---------
 example 
 system  
(2 rows)

Query 20250506_170112_00001_taeah, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0.50 [0 rows, 0B] [0 rows/s, 0B/s]
```

```sql
trino> create schema example.example_schema with (location = 's3://warehouse/example_schema');
CREATE SCHEMA
```

```sql
trino> create table example.example_schema.example_table(c1 INTEGER,
    ->     c2 DATE,
    ->     c3 DOUBLE
    -> )
    -> WITH (
    ->     format = 'PARQUET',
    ->     partitioning = ARRAY['c1', 'c2'],
    ->     sorted_by = ARRAY['c3'],
    ->     location = 's3://warehouse/example_schema/example_table');
CREATE TABLE
```

```sql
trino> insert into example.example_schema.example_table values (1, to_date('2025-05-07','yyyy-mm-dd'),5.5);
INSERT: 1 row

Query 20250506_170824_00007_taeah, FINISHED, 1 node
Splits: 42 total, 42 done (100.00%)
0.73 [0 rows, 0B] [0 rows/s, 0B/s]
```

```sql
trino> insert into example.example_schema.example_table values (2, to_date('2025-05-06','yyyy-mm-dd'),5.8);
INSERT: 1 row

Query 20250506_170917_00008_taeah, FINISHED, 1 node
Splits: 42 total, 42 done (100.00%)
0.19 [0 rows, 0B] [0 rows/s, 0B/s]
```

```sql
trino> select * from example.example_schema.example_table;
 c1 |     c2     | c3  
----+------------+-----
  2 | 2025-05-06 | 5.8 
  1 | 2025-05-07 | 5.5 
(2 rows)

Query 20250506_170948_00009_taeah, FINISHED, 1 node
Splits: 2 total, 2 done (100.00%)
0.25 [2 rows, 904B] [8 rows/s, 3.55KiB/s]
```

```sql
trino> create table example.example_schema.example_joined_table as select a.c1 a_c1, a.c2 a_c2, a.c3 a_c3 ,b.* from example.example_schema.example_table a inner join example.example_sc
hema.example_table b on 1=1;
CREATE TABLE: 4 rows

Query 20250506_171325_00011_taeah, FINISHED, 1 node
Splits: 62 total, 62 done (100.00%)
0.31 [6 rows, 1.94KiB] [19 rows/s, 6.35KiB/s]
```

```sql
trino> select * from example.example_schema.example_joined_table;
 a_c1 |    a_c2    | a_c3 | c1 |     c2     | c3  
------+------------+------+----+------------+-----
    1 | 2025-05-07 |  5.5 |  2 | 2025-05-06 | 5.8 
    2 | 2025-05-06 |  5.8 |  2 | 2025-05-06 | 5.8 
    2 | 2025-05-06 |  5.8 |  1 | 2025-05-07 | 5.5 
    1 | 2025-05-07 |  5.5 |  1 | 2025-05-07 | 5.5 
(4 rows)

Query 20250506_171401_00012_taeah, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
0.17 [4 rows, 1.05KiB] [23 rows/s, 6.32KiB/s]
```

```sql
trino> SELECT * FROM example.example_schema.example_joined_table TABLESAMPLE bernoulli (75);
 a_c1 |    a_c2    | a_c3 | c1 |     c2     | c3  
------+------------+------+----+------------+-----
    1 | 2025-05-07 |  5.5 |  2 | 2025-05-06 | 5.8 
    2 | 2025-05-06 |  5.8 |  2 | 2025-05-06 | 5.8 
    1 | 2025-05-07 |  5.5 |  1 | 2025-05-07 | 5.5 
(3 rows)
```
