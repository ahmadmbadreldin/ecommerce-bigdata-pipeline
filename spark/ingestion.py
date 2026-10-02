from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    to_timestamp
)

spark = (
    SparkSession.builder
    .appName("Ecommerce-Ingestion")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# HDFS PATHS
# =========================================================

PAYMENTS_PATH = "hdfs:///data/ecommerce/landing/csv/payments"
SHIPMENTS_PATH = "hdfs:///data/ecommerce/landing/csv/shipments"
EVENTS_PATH = "hdfs:///data/ecommerce/landing/avro/web_events"

STANDARDIZED_BASE = "hdfs:///data/ecommerce/standardized"
REJECTED_BASE = "hdfs:///data/ecommerce/rejected"


# =========================================================
# 1. PAYMENTS CSV
# =========================================================

payments = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(PAYMENTS_PATH)
)

print("PAYMENTS RAW SCHEMA")
payments.printSchema()

print("PAYMENTS SAMPLE")
payments.show(5, truncate=False)


# Clean text noise
payments_clean = (
    payments
    .withColumn("payment_method", upper(trim(col("payment_method"))))
    .withColumn("payment_status", upper(trim(col("payment_status"))))
    .withColumn("currency", upper(trim(col("currency"))))
    .withColumn(
        "payment_ts",
        to_timestamp(col("payment_ts"))
    )
)


# Validation
payments_valid = payments_clean.filter(
    col("payment_id").isNotNull()
    & col("order_id").isNotNull()
    & col("customer_id").isNotNull()
    & (col("amount") > 0)
    & col("payment_ts").isNotNull()
)

payments_rejected = payments_clean.filter(
    col("payment_id").isNull()
    | col("order_id").isNull()
    | col("customer_id").isNull()
    | (col("amount") <= 0)
    | col("payment_ts").isNull()
)


# =========================================================
# 2. SHIPMENTS CSV
# =========================================================

shipments = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(SHIPMENTS_PATH)
)

print("SHIPMENTS RAW SCHEMA")
shipments.printSchema()

shipments_clean = (
    shipments
    .withColumn("carrier", upper(trim(col("carrier"))))
    .withColumn("shipment_status", upper(trim(col("shipment_status"))))
    .withColumn(
        "shipped_ts",
        to_timestamp(col("shipped_ts"))
    )
    .withColumn(
        "delivered_ts",
        to_timestamp(col("delivered_ts"))
    )
)

shipments_valid = shipments_clean.filter(
    col("shipment_id").isNotNull()
    & col("order_id").isNotNull()
    & col("customer_id").isNotNull()
    & col("shipped_ts").isNotNull()
)

shipments_rejected = shipments_clean.filter(
    col("shipment_id").isNull()
    | col("order_id").isNull()
    | col("customer_id").isNull()
    | col("shipped_ts").isNull()
)


# =========================================================
# 3. AVRO WEB EVENTS
# =========================================================

events = (
    spark.read
    .format("avro")
    .load(EVENTS_PATH)
)

print("WEB EVENTS RAW SCHEMA")
events.printSchema()

print("WEB EVENTS SAMPLE")
events.show(5, truncate=False)


events_clean = (
    events
    .withColumn("event_type", trim(col("event_type")))
    .withColumn("device", trim(col("device")))
    .withColumn(
        "event_ts",
        to_timestamp(col("event_ts"))
    )
)


events_valid = events_clean.filter(
    col("event_id").isNotNull()
    & col("customer_id").isNotNull()
    & col("product_id").isNotNull()
    & col("event_ts").isNotNull()
)

events_rejected = events_clean.filter(
    col("event_id").isNull()
    | col("customer_id").isNull()
    | col("product_id").isNull()
    | col("event_ts").isNull()
)


# =========================================================
# COUNTS
# =========================================================

print("================ COUNTS ================")

print("Payments RAW:", payments.count())
print("Payments VALID:", payments_valid.count())
print("Payments REJECTED:", payments_rejected.count())

print("Shipments RAW:", shipments.count())
print("Shipments VALID:", shipments_valid.count())
print("Shipments REJECTED:", shipments_rejected.count())

print("Events RAW:", events.count())
print("Events VALID:", events_valid.count())
print("Events REJECTED:", events_rejected.count())


# =========================================================
# WRITE STANDARDIZED DATA AS PARQUET
# =========================================================

payments_valid.write \
    .mode("overwrite") \
    .parquet(f"{STANDARDIZED_BASE}/payments")

shipments_valid.write \
    .mode("overwrite") \
    .parquet(f"{STANDARDIZED_BASE}/shipments")

events_valid.write \
    .mode("overwrite") \
    .parquet(f"{STANDARDIZED_BASE}/web_events")


# =========================================================
# WRITE REJECTED DATA
# =========================================================

payments_rejected.write \
    .mode("overwrite") \
    .parquet(f"{REJECTED_BASE}/payments")

shipments_rejected.write \
    .mode("overwrite") \
    .parquet(f"{REJECTED_BASE}/shipments")

events_rejected.write \
    .mode("overwrite") \
    .parquet(f"{REJECTED_BASE}/web_events")


print("========================================")
print("INGESTION COMPLETED SUCCESSFULLY")
print("========================================")

spark.stop()
