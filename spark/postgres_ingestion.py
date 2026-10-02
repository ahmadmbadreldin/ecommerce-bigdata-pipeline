from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper

spark = (
    SparkSession.builder
    .appName("Postgres-Full-Ingestion")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

jdbc_url = "jdbc:postgresql://192.168.161.1:5433/ecommerce"

properties = {
    "user": "bigdata",
    "password": "bigdata",
    "driver": "org.postgresql.Driver"
}

STANDARDIZED_BASE = "hdfs:///data/ecommerce/standardized"


def read_table(table_name):
    return (
        spark.read
        .jdbc(
            url=jdbc_url,
            table=table_name,
            properties=properties
        )
    )


# customers
customers = read_table("customers")

customers_clean = (
    customers
    .withColumn("full_name", trim(col("full_name")))
    .withColumn("email", trim(col("email")))
    .withColumn("country", trim(col("country")))
    .withColumn("city", trim(col("city")))
)

customers_clean.write \
    .mode("overwrite") \
    .parquet(f"{STANDARDIZED_BASE}/customers")


# suppliers
suppliers = read_table("suppliers")

suppliers_clean = (
    suppliers
    .withColumn("supplier_name", trim(col("supplier_name")))
    .withColumn("country", trim(col("country")))
)

suppliers_clean.write \
    .mode("overwrite") \
    .parquet(f"{STANDARDIZED_BASE}/suppliers")


# products
products = read_table("products")

products_clean = (
    products
    .withColumn("sku", trim(col("sku")))
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", trim(col("category")))
)

products_clean.write \
    .mode("overwrite") \
    .parquet(f"{STANDARDIZED_BASE}/products")


# orders
orders = read_table("orders")

orders_clean = (
    orders
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("currency", upper(trim(col("currency"))))
)

orders_clean.write \
    .mode("overwrite") \
    .parquet(f"{STANDARDIZED_BASE}/orders")


# order_items
order_items = read_table("order_items")

order_items.write \
    .mode("overwrite") \
    .parquet(f"{STANDARDIZED_BASE}/order_items")


print("====================================")
print("POSTGRES INGESTION COMPLETED")
print("====================================")

print("customers:", customers.count())
print("suppliers:", suppliers.count())
print("products:", products.count())
print("orders:", orders.count())
print("order_items:", order_items.count())

spark.stop()
