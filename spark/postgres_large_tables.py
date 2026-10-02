from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper

spark = (
    SparkSession.builder
    .appName("Postgres-Large-Tables-Ingestion")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

jdbc_url = "jdbc:postgresql://192.168.161.1:5433/ecommerce"

STANDARDIZED_BASE = "hdfs:///data/ecommerce/standardized"

common_options = {
    "url": jdbc_url,
    "user": "bigdata",
    "password": "bigdata",
    "driver": "org.postgresql.Driver",
    "fetchsize": "10000"
}


# ==================================================
# ORDERS - 5,000,000 rows
# ==================================================

print("Reading ORDERS from PostgreSQL...")

orders = (
    spark.read
    .format("jdbc")
    .options(**common_options)
    .option("dbtable", "orders")
    .option("partitionColumn", "order_id")
    .option("lowerBound", "1")
    .option("upperBound", "5000000")
    .option("numPartitions", "8")
    .load()
)

orders_clean = (
    orders
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("currency", upper(trim(col("currency"))))
)

print("Writing ORDERS to HDFS...")

(
    orders_clean.write
    .mode("overwrite")
    .parquet(f"{STANDARDIZED_BASE}/orders")
)

print("ORDERS completed.")


# ==================================================
# ORDER ITEMS
# ==================================================

print("Reading ORDER_ITEMS from PostgreSQL...")

order_items = (
    spark.read
    .format("jdbc")
    .options(**common_options)
    .option("dbtable", "order_items")
    .option("partitionColumn", "order_item_id")
    .option("lowerBound", "1")
    .option("upperBound", "20000000")
    .option("numPartitions", "12")
    .load()
)

print("Writing ORDER_ITEMS to HDFS...")

(
    order_items.write
    .mode("overwrite")
    .parquet(f"{STANDARDIZED_BASE}/order_items")
)

print("ORDER_ITEMS completed.")

spark.stop()
