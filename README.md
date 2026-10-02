# Scalable E-commerce Big Data Pipeline

An end-to-end Big Data Engineering project that simulates a production-style e-commerce data platform using **PostgreSQL, Apache Hadoop HDFS, Apache Spark / PySpark, Parquet, Avro, Apache Hive, Docker, Python, and SQL**.

The project generates tens of millions of synthetic e-commerce records, ingests data from multiple source formats, stores raw data in HDFS, cleans and standardizes the datasets with Spark, builds analytical fact and dimension tables in a Gold Layer, and exposes the final data through Hive External Tables for SQL analytics.

---

# Project Overview

The goal of this project is to design and implement a complete Big Data pipeline that demonstrates the major stages of a modern data engineering workflow:

```text
Data Generation
      ↓
PostgreSQL + CSV + Avro
      ↓
Bronze Layer / HDFS Landing
      ↓
Apache Spark
      ↓
Validation + Cleaning + Standardization
      ↓
Silver Layer / Parquet
      ↓
Spark Transformations + Joins
      ↓
Gold Layer
      ↓
Fact & Dimension Tables
      ↓
Apache Hive External Tables
      ↓
SQL Analytics
```

The project was intentionally designed to work with a large enough dataset to demonstrate distributed storage, Spark processing, JDBC ingestion, Parquet optimization, dimensional modeling, and Hive analytics.

---

# Architecture

```text
                         ┌──────────────────────┐
                         │      PostgreSQL      │
                         │                      │
                         │ customers            │
                         │ suppliers            │
                         │ products             │
                         │ orders               │
                         │ order_items          │
                         └──────────┬───────────┘
                                    │
                                  JDBC
                                    │
                                    ▼

CSV Files ───────────────► ┌──────────────────────┐ ◄────────────── Avro Files
payments                   │     BRONZE LAYER     │                 web_events
shipments                  │        HDFS          │
                           └──────────┬───────────┘
                                      │
                                      ▼
                           ┌──────────────────────┐
                           │    Apache Spark      │
                           │                      │
                           │ Validation           │
                           │ Cleaning             │
                           │ Standardization      │
                           │ Type Conversion      │
                           │ Rejected Records     │
                           └──────────┬───────────┘
                                      │
                                      ▼
                           ┌──────────────────────┐
                           │     SILVER LAYER     │
                           │       Parquet        │
                           └──────────┬───────────┘
                                      │
                                      ▼
                           ┌──────────────────────┐
                           │    Apache Spark      │
                           │                      │
                           │ Joins                │
                           │ Business Logic       │
                           │ Dimensional Modeling │
                           └──────────┬───────────┘
                                      │
                                      ▼
                           ┌──────────────────────┐
                           │      GOLD LAYER      │
                           │                      │
                           │ Dimensions           │
                           │ Facts                │
                           └──────────┬───────────┘
                                      │
                                      ▼
                           ┌──────────────────────┐
                           │    Apache Hive       │
                           │   External Tables    │
                           └──────────┬───────────┘
                                      │
                                      ▼
                           ┌──────────────────────┐
                           │    SQL Analytics     │
                           └──────────────────────┘
```

---

# Technology Stack

## Programming

- Python
- PySpark
- SQL
- Scala / Spark Shell for schema inspection

## Big Data

- Apache Hadoop
- HDFS
- Apache Spark 4.1.1
- Apache Hive 4.2.1
- Apache Tez

## Data Formats

- CSV
- Avro
- Parquet

## Database

- PostgreSQL

## Infrastructure

- Docker
- Docker Compose
- Linux
- VMware Virtual Machine
- Windows host machine

## Connectivity

- JDBC
- TCP networking between the Spark VM and PostgreSQL running on the Windows host

---

# Dataset Scale

The data generator produces a large synthetic e-commerce dataset.

## Core Dataset

| Dataset | Approximate Records |
|---|---:|
| Customers | 1,000,000 |
| Suppliers | 5,000 |
| Products | 50,000 |
| Orders | 5,000,000 |
| Order Items | 15,004,502 |
| Payments | ~4.4 million |
| Shipments | ~3.6 million |
| Web Events | ~17 million |

The project processes **more than 40 million logical records** across multiple sources.

---

# Synthetic Data Generator

A custom Python data generator was developed to create realistic and internally consistent e-commerce data.

The generator creates data for three different source types.

## PostgreSQL

The following operational tables are generated directly into PostgreSQL:

```text
customers
suppliers
products
orders
order_items
```

## CSV

External batch-style data is generated as CSV:

```text
payments
shipments
```

## Avro

Web activity and behavioral event data is generated as Avro:

```text
web_events
```

---

# Data Generator Configuration

The generator was configured approximately as follows:

```yaml
project:
  name: ecommerce_source_generator
  output_dir: output

runtime:
  seed: 42
  batch_size: 20000
  file_rows: 500000
  reset_database: true

scale:
  customers: 1000000
  suppliers: 5000
  products: 50000
  orders: 5000000

generation:
  min_items_per_order: 1
  max_items_per_order: 5
  min_quantity: 1
  max_quantity: 5

noise:
  enabled: true
  whitespace_rate: 0.015
  casing_rate: 0.020
  optional_null_rate: 0.010
  duplicate_event_rate: 0.005
  late_event_rate: 0.010
```

---

# Data Consistency

The generator was designed to preserve relationships between datasets.

Examples include:

- Every valid order references an existing customer.
- Every order item references an existing order.
- Every order item references an existing product.
- Products reference valid suppliers.
- Payment records reference valid orders.
- Shipment records reference valid eligible orders.
- Web events reference valid customers and products where required.
- Order totals are derived from order item values.
- Payment amounts are linked to the corresponding order totals.

---

# Controlled Data Quality Noise

The project intentionally introduces controlled data-quality issues to simulate realistic ingestion scenarios.

Examples include:

- Leading or trailing whitespace
- Inconsistent casing
- Optional null fields
- Duplicate web events
- Late-arriving events

This allows the Spark pipeline to demonstrate actual cleaning and validation rather than processing perfectly clean data.

---

# Source File Generation

The generated external source files included:

```text
payments:
9 CSV files

shipments:
8 CSV files

web_events:
34 Avro files
```

Each large output file is rolled after approximately:

```text
500,000 rows
```

This avoids producing a single extremely large source file.

---

# Bronze Layer

The Bronze Layer represents the raw / landing area in HDFS.

The external CSV and Avro files are copied to HDFS with minimal modification.

Example structure:

```text
/data/ecommerce/landing/

├── csv/
│   ├── payments/
│   └── shipments/
│
└── avro/
    └── web_events/
```

Data is uploaded using commands similar to:

```bash
hdfs dfs -put output/csv/payments/* \
  /data/ecommerce/landing/csv/payments/

hdfs dfs -put output/csv/shipments/* \
  /data/ecommerce/landing/csv/shipments/

hdfs dfs -put output/avro/web_events/* \
  /data/ecommerce/landing/avro/web_events/
```

---

# Silver Layer

The Silver Layer contains cleaned and standardized data stored in **Parquet**.

Location:

```text
/data/ecommerce/standardized/
```

The following datasets were created:

```text
customers
suppliers
products
orders
order_items
payments
shipments
web_events
```

---

# Silver Layer Storage Size

The resulting Parquet datasets had approximately the following HDFS sizes:

| Dataset | Size |
|---|---:|
| customers | 44.5 MB |
| suppliers | 139.9 KB |
| products | 1.5 MB |
| orders | 165.7 MB |
| order_items | 254.7 MB |
| payments | 274.0 MB |
| shipments | 133.9 MB |
| web_events | 411.6 MB |

Total Silver Layer size:

```text
~1.29 GB
```

The reduction in size compared with raw source data demonstrates the storage benefits of using a columnar format such as Parquet.

---

# Spark Data Cleaning

Spark performs multiple cleaning and standardization tasks.

Examples include:

## String Cleaning

```python
trim(...)
upper(...)
```

Used for fields such as:

```text
currency
status
payment_method
payment_status
carrier
shipment_status
```

## Timestamp Conversion

Raw timestamps are converted into proper Spark timestamp types using:

```python
to_timestamp(...)
```

## Validation

Important keys are checked for null values.

Examples:

```text
payment_id
order_id
customer_id
shipment_id
event_id
product_id
timestamp columns
```

Invalid rows can be separated into rejected datasets for quality monitoring.

---

# PostgreSQL to Spark JDBC Ingestion

One of the major engineering tasks in this project was connecting Spark running inside a Linux VM to PostgreSQL running inside Docker on the Windows host.

Environment:

```text
Windows Host
192.168.161.1

Spark / Hadoop VM
192.168.161.128

PostgreSQL Port
5433
```

The network connection was tested using:

```bash
nc -zv 192.168.161.1 5433
```

Successful output confirmed that the VM could reach PostgreSQL.

---

# JDBC Connection

Spark connects using:

```text
jdbc:postgresql://192.168.161.1:5433/ecommerce
```

A PostgreSQL JDBC driver is loaded using Spark packages:

```bash
spark-submit \
  --packages org.postgresql:postgresql:42.7.7 \
  postgres_ingestion.py
```

The connection was validated successfully by reading the complete customer table.

Result:

```text
CUSTOMERS COUNT
1000000
```

---

# Parallel JDBC Ingestion

Reading millions of database rows through a single JDBC connection is inefficient.

For large tables, Spark JDBC partitioning was implemented.

For example:

```python
.option("partitionColumn", "order_id")
.option("lowerBound", "1")
.option("upperBound", "5000000")
.option("numPartitions", "8")
```

This allows Spark to split the database read into multiple ranges.

Conceptually:

```text
5,000,000 orders
        ↓
order_id partitioning
        ↓
8 JDBC partitions
        ↓
Parallel Spark reads
```

A similar strategy was used for the much larger `order_items` dataset:

```python
.option("partitionColumn", "order_item_id")
.option("lowerBound", "1")
.option("upperBound", "20000000")
.option("numPartitions", "12")
```

This demonstrates scalable database ingestion instead of loading very large tables through a single JDBC stream.

---

# Parquet

Parquet is used as the main analytical storage format for the Silver and Gold layers.

Reasons include:

- Columnar storage
- Compression
- Efficient analytical scans
- Predicate pushdown
- Compatibility with Spark
- Compatibility with Hive
- Reduced storage compared with raw formats

---

# Avro Integration

Web event data is generated in Avro format.

Spark reads Avro using:

```python
spark.read.format("avro").load(...)
```

Spark's Avro module is loaded using:

```bash
spark-submit \
  --packages org.apache.spark:spark-avro_2.13:4.1.1 \
  ingestion.py
```

The web event pipeline successfully processed Avro source files and converted standardized output to Parquet.

---

# Gold Layer

The Gold Layer contains business-ready analytical datasets.

Location:

```text
/data/ecommerce/gold/
```

The Gold Layer includes:

## Dimensions

```text
dim_customer
dim_product
dim_supplier
dim_date
```

## Facts

```text
fact_sales
fact_payments
fact_shipments
fact_events
```

---

# Gold Layer Storage Size

| Dataset | Size |
|---|---:|
| dim_customer | 46.9 MB |
| dim_date | 6.1 KB |
| dim_product | 1.6 MB |
| dim_supplier | 139.8 KB |
| fact_events | 429.9 MB |
| fact_payments | 278.8 MB |
| fact_sales | 403.2 MB |
| fact_shipments | 137.8 MB |

---

# Dimensional Model

The Gold Layer follows a dimensional modeling approach.

Simplified model:

```text
                    dim_customer
                         │
                         │ customer_id
                         │
                         ▼
dim_product ───────► fact_sales ◄─────── dim_date
     │                   │
     │                   │
     ▼                   ▼
dim_supplier          Measures
```

Additional fact tables include:

```text
fact_payments
fact_shipments
fact_events
```

---

# fact_sales

`fact_sales` is created by joining operational datasets such as:

```text
orders
+
order_items
+
products
```

Important fields include:

```text
order_item_id
order_id
customer_id
product_id
supplier_id
date_key
order_ts
quantity
unit_price
line_total
order_status
currency
```

`line_total` is calculated using:

```text
quantity × unit_price
```

---

# fact_sales Scale

Hive successfully queried:

```text
15,004,502 rows
```

from `fact_sales`.

Example validation:

```sql
SELECT COUNT(*)
FROM fact_sales;
```

Result:

```text
15004502
```

This confirms that the Gold Layer and Hive integration successfully operate over more than 15 million sales fact records.

---

# dim_customer

Schema:

```text
customer_id BIGINT
full_name STRING
email STRING
country STRING
city STRING
created_at TIMESTAMP
updated_at TIMESTAMP
```

---

# dim_supplier

Schema:

```text
supplier_id BIGINT
supplier_name STRING
country STRING
created_at TIMESTAMP
```

---

# dim_product

Schema:

```text
product_id BIGINT
supplier_id BIGINT
sku STRING
product_name STRING
category STRING
unit_price DECIMAL(12,2)
unit_cost DECIMAL(12,2)
active BOOLEAN
```

---

# dim_date

Schema:

```text
date DATE
date_key INT
year INT
quarter INT
month INT
day INT
day_of_week INT
is_weekend BOOLEAN
```

The date key uses the format:

```text
YYYYMMDD
```

Example:

```text
2026-09-20
↓
20260920
```

---

# fact_payments

Includes:

```text
payment_id
order_id
customer_id
payment_ts
amount
currency
payment_method
payment_status
transaction_ref
date_key
```

---

# fact_shipments

Includes:

```text
shipment_id
order_id
customer_id
carrier
tracking_number
shipped_ts
delivered_ts
shipment_status
date_key
```

---

# fact_events

Includes:

```text
event_id
event_ts
event_type
customer_id
order_id
product_id
device
session_id
date_key
```

---

# Apache Hive

Hive is used as the SQL serving layer over the Gold Layer.

Hive version:

```text
Apache Hive 4.2.1
```

HiveServer2 is used with Beeline.

Connection:

```text
jdbc:hive2://localhost:10000
```

---

# Hive Database

A dedicated Hive database was created:

```sql
CREATE DATABASE ecommerce_dw;
```

Then:

```sql
USE ecommerce_dw;
```

---

# Hive External Tables

Hive External Tables are created directly over the Parquet files stored in the Gold Layer.

Example:

```sql
CREATE EXTERNAL TABLE fact_sales (
    order_item_id BIGINT,
    order_id BIGINT,
    customer_id BIGINT,
    product_id BIGINT,
    supplier_id BIGINT,
    date_key INT,
    order_ts TIMESTAMP,
    quantity INT,
    unit_price DECIMAL(12,2),
    line_total DECIMAL(23,2),
    order_status STRING,
    currency STRING
)
STORED AS PARQUET
LOCATION '/data/ecommerce/gold/fact_sales';
```

Hive does not duplicate the data.

Instead, Hive stores metadata describing the existing Parquet files.

---

# Hive Tables Created

The following eight tables were successfully registered:

```text
dim_customer
dim_date
dim_product
dim_supplier
fact_events
fact_payments
fact_sales
fact_shipments
```

---

# Example Analytics Query

Revenue, units sold, and orders by product category:

```sql
SELECT
    p.category,
    ROUND(SUM(f.line_total), 2) AS total_revenue,
    SUM(f.quantity) AS units_sold,
    COUNT(DISTINCT f.order_id) AS orders_count
FROM fact_sales f
JOIN dim_product p
    ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;
```

---

# Data Lake Layer Mapping

The project uses a Medallion-style architecture.

```text
Bronze
=
Raw / Landing data

Silver
=
Cleaned and standardized data

Gold
=
Business-ready analytical datasets
```

Project paths:

```text
/data/ecommerce/landing
→ Bronze Layer

/data/ecommerce/standardized
→ Silver Layer

/data/ecommerce/gold
→ Gold Layer
```

---

# HDFS

HDFS is used as the distributed storage layer.

Main project paths:

```text
/data/ecommerce/landing
/data/ecommerce/standardized
/data/ecommerce/rejected
/data/ecommerce/gold
```

---

# Spark Processing

Spark is responsible for:

- Reading CSV
- Reading Avro
- Reading PostgreSQL via JDBC
- Schema inference / conversion
- Data cleaning
- Validation
- Standardization
- Parquet writing
- Parallel JDBC ingestion
- Joins
- Business transformations
- Fact table construction
- Dimension table construction
- Date dimension generation

---

# Major Engineering Challenges Solved

## 1. Multi-source Data Ingestion

The project combines:

```text
PostgreSQL
CSV
Avro
```

inside one analytical pipeline.

---

## 2. Large PostgreSQL Tables

A basic JDBC read worked for smaller tables but was not appropriate for multi-million-row tables.

Parallel JDBC partitioning was introduced for:

```text
orders
order_items
```

This improved scalability and demonstrated Spark database ingestion best practices.

---

## 3. PostgreSQL Running Outside the VM

Spark runs inside a Linux VM while PostgreSQL runs in Docker on Windows.

Network connectivity between the environments had to be configured and tested.

Architecture:

```text
Spark VM
192.168.161.128
      │
      │ JDBC
      ▼
Windows Host
192.168.161.1:5433
      │
      ▼
Docker PostgreSQL
```

---

## 4. PostgreSQL Port Conflict

Another PostgreSQL environment was already using port `5432`.

The e-commerce PostgreSQL container was therefore exposed using:

```text
5433:5432
```

The project then connected through host port:

```text
5433
```

---

## 5. Spark Avro Dependency

Spark required the external Avro module.

The pipeline was corrected by adding:

```text
org.apache.spark:spark-avro_2.13:4.1.1
```

during Spark execution.

---

## 6. Parquet Optimization

Raw CSV / Avro / database data was converted into Parquet to improve storage efficiency and analytical performance.

---

## 7. Dimensional Modeling

Operational datasets were transformed into analytics-focused:

```text
dimensions
+
fact tables
```

instead of querying normalized source tables directly.

---

## 8. Hive External Tables

Hive External Tables were used so the SQL layer could query the Gold Layer without duplicating data.

---

# Validation Results

Major successful validation points include:

```text
PostgreSQL network connectivity: PASSED

Spark JDBC connectivity: PASSED

Customer count:
1,000,000

CSV ingestion: PASSED

Avro ingestion: PASSED

Silver Parquet generation: PASSED

Gold Layer generation: PASSED

HiveServer2 connectivity: PASSED

Hive database creation: PASSED

8 Hive external tables created: PASSED

fact_sales Hive row count:
15,004,502
```

---

# Repository Structure

Recommended repository structure:

```text
ecommerce-bigdata-pipeline/
│
├── README.md
├── .gitignore
│
├── data-generator/
│   ├── config/
│   ├── docker/
│   ├── sql/
│   ├── src/
│   ├── requirements.txt
│   ├── run.py
│   └── validate_generated_data.py
│
├── spark/
│   ├── ingestion.py
│   ├── postgres_ingestion.py
│   ├── postgres_large_tables.py
│   └── gold_transform.py
│
├── hive/
│   ├── create_gold_tables.hql
│   └── analytics_queries.hql
│
├── sample-data/
│   ├── payments/
│   ├── shipments/
│   └── web_events/
│
├── docs/
│   ├── architecture.md
│   ├── data-model.md
│   └── screenshots/
│
└── diagrams/
    └── architecture.png
```

---

# Running the Project

## 1. Start Hadoop

```bash
start-dfs.sh
start-yarn.sh
```

Verify:

```bash
jps
```

Expected services include:

```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

---

# 2. Create HDFS Bronze Directories

```bash
hdfs dfs -mkdir -p /data/ecommerce/landing/csv/payments

hdfs dfs -mkdir -p /data/ecommerce/landing/csv/shipments

hdfs dfs -mkdir -p /data/ecommerce/landing/avro/web_events
```

---

# 3. Upload Batch Files

```bash
hdfs dfs -put output/csv/payments/* \
  /data/ecommerce/landing/csv/payments/

hdfs dfs -put output/csv/shipments/* \
  /data/ecommerce/landing/csv/shipments/

hdfs dfs -put output/avro/web_events/* \
  /data/ecommerce/landing/avro/web_events/
```

---

# 4. Run CSV and Avro Spark Ingestion

```bash
spark-submit \
  --packages org.apache.spark:spark-avro_2.13:4.1.1 \
  ingestion.py
```

---

# 5. Run PostgreSQL JDBC Ingestion

```bash
spark-submit \
  --packages org.postgresql:postgresql:42.7.7 \
  postgres_ingestion.py
```

---

# 6. Run Large Table Parallel JDBC Ingestion

```bash
spark-submit \
  --driver-memory 2g \
  --executor-memory 2g \
  --packages org.postgresql:postgresql:42.7.7 \
  postgres_large_tables.py
```

---

# 7. Build Gold Layer

```bash
spark-submit \
  --driver-memory 2g \
  --executor-memory 2g \
  gold_transform.py
```

Verify:

```bash
hdfs dfs -du -h /data/ecommerce/gold
```

---

# 8. Start HiveServer2

```bash
/home/hadoop/hive/bin/hiveserver2
```

---

# 9. Connect Using Beeline

```bash
/home/hadoop/hive/bin/beeline
```

Then:

```text
!connect jdbc:hive2://localhost:10000
```

---

# 10. Create Hive Database

```sql
CREATE DATABASE ecommerce_dw;

USE ecommerce_dw;
```

---

# 11. Create External Tables

Run:

```bash
/home/hadoop/hive/bin/beeline \
  -u jdbc:hive2://localhost:10000 \
  -n hadoop \
  -f hive/create_gold_tables.hql
```

---

# 12. Verify Tables

```sql
SHOW TABLES;
```

Expected:

```text
dim_customer
dim_date
dim_product
dim_supplier
fact_events
fact_payments
fact_sales
fact_shipments
```

---

# Example HDFS Output

Silver Layer:

```text
/data/ecommerce/standardized/

customers
suppliers
products
orders
order_items
payments
shipments
web_events
```

Gold Layer:

```text
/data/ecommerce/gold/

dim_customer
dim_supplier
dim_product
dim_date
fact_sales
fact_payments
fact_shipments
fact_events
```

---

# Portfolio Highlights

This project demonstrates practical experience with:

- Big Data architecture
- Data lake design
- Bronze / Silver / Gold architecture
- Multi-source ingestion
- PostgreSQL
- JDBC
- Parallel JDBC reads
- Hadoop HDFS
- Apache Spark
- PySpark
- CSV processing
- Avro processing
- Parquet
- Data validation
- Data cleansing
- Dimensional modeling
- Fact tables
- Dimension tables
- Apache Hive
- HiveServer2
- Beeline
- Hive External Tables
- SQL analytics
- Docker
- Linux
- Virtual machine networking
- Large-scale synthetic data generation
- Processing tens of millions of records

---

# Key Achievement

The final pipeline successfully processes tens of millions of records from multiple data sources and exposes more than:

```text
15,004,502 sales fact rows
```

through Hive for SQL analytics.

The complete workflow demonstrates:

```text
Generate
→ Ingest
→ Store
→ Validate
→ Clean
→ Standardize
→ Transform
→ Model
→ Serve
→ Analyze
```

---

# Future Improvements

Possible future improvements include:

- Incremental ingestion
- CDC
- Watermark-based processing
- Spark Structured Streaming
- Kafka integration
- Airflow orchestration
- Data quality framework
- Schema registry
- Automated pipeline monitoring
- Partitioned Gold tables
- File compaction
- Iceberg / Delta Lake / Hudi
- BI dashboard integration
- Cloud deployment
- Object storage such as S3
- CI/CD pipeline
- Data lineage
- Metadata catalog
- Unit and integration testing

---

# Author

Big Data Engineering Portfolio Project

Built as an end-to-end implementation of a scalable e-commerce data platform using Hadoop, Spark, PostgreSQL, Hive, Parquet, Avro, Python, SQL, Docker, and Linux.
