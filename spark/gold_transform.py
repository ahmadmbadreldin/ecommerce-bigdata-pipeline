from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    date_format,
    year,
    month,
    dayofmonth,
    quarter,
    dayofweek,
    when,
    lit
)

# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("Ecommerce-Gold-Layer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Paths
# ============================================================

SILVER = "hdfs:///data/ecommerce/standardized"
GOLD = "hdfs:///data/ecommerce/gold"


# ============================================================
# Read Silver Layer Data
# ============================================================

print("Reading Silver Layer datasets...")

customers = spark.read.parquet(f"{SILVER}/customers")
suppliers = spark.read.parquet(f"{SILVER}/suppliers")
products = spark.read.parquet(f"{SILVER}/products")
orders = spark.read.parquet(f"{SILVER}/orders")
order_items = spark.read.parquet(f"{SILVER}/order_items")

payments = spark.read.parquet(f"{SILVER}/payments")
shipments = spark.read.parquet(f"{SILVER}/shipments")
events = spark.read.parquet(f"{SILVER}/web_events")


# ============================================================
# DIM CUSTOMER
# ============================================================

print("Building dim_customer...")

dim_customer = (
    customers
    .dropDuplicates(["customer_id"])
)

(
    dim_customer.write
    .mode("overwrite")
    .parquet(f"{GOLD}/dim_customer")
)


# ============================================================
# DIM SUPPLIER
# ============================================================

print("Building dim_supplier...")

dim_supplier = (
    suppliers
    .dropDuplicates(["supplier_id"])
)

(
    dim_supplier.write
    .mode("overwrite")
    .parquet(f"{GOLD}/dim_supplier")
)


# ============================================================
# DIM PRODUCT
# ============================================================

print("Building dim_product...")

dim_product = (
    products
    .dropDuplicates(["product_id"])
)

(
    dim_product.write
    .mode("overwrite")
    .parquet(f"{GOLD}/dim_product")
)


# ============================================================
# DIM DATE
# ============================================================

print("Building dim_date...")

order_dates = (
    orders
    .select(
        to_date(col("order_ts")).alias("date")
    )
)

payment_dates = (
    payments
    .select(
        to_date(col("payment_ts")).alias("date")
    )
)

shipment_dates = (
    shipments
    .select(
        to_date(col("shipped_ts")).alias("date")
    )
)

event_dates = (
    events
    .select(
        to_date(col("event_ts")).alias("date")
    )
)

all_dates = (
    order_dates
    .union(payment_dates)
    .union(shipment_dates)
    .union(event_dates)
    .filter(col("date").isNotNull())
    .distinct()
)

dim_date = (
    all_dates

    .withColumn(
        "date_key",
        date_format(col("date"), "yyyyMMdd").cast("int")
    )

    .withColumn(
        "year",
        year(col("date"))
    )

    .withColumn(
        "quarter",
        quarter(col("date"))
    )

    .withColumn(
        "month",
        month(col("date"))
    )

    .withColumn(
        "day",
        dayofmonth(col("date"))
    )

    .withColumn(
        "day_of_week",
        dayofweek(col("date"))
    )

    .withColumn(
        "is_weekend",
        when(
            dayofweek(col("date")).isin(1, 7),
            lit(True)
        ).otherwise(lit(False))
    )
)

(
    dim_date.write
    .mode("overwrite")
    .parquet(f"{GOLD}/dim_date")
)


# ============================================================
# FACT SALES
# ============================================================

print("Building fact_sales...")

sales = (
    order_items.alias("oi")

    .join(
        orders.alias("o"),
        col("oi.order_id") == col("o.order_id"),
        "inner"
    )

    .join(
        products.alias("p"),
        col("oi.product_id") == col("p.product_id"),
        "left"
    )
)

fact_sales = (
    sales
    .select(

        col("oi.order_item_id"),
        col("oi.order_id"),

        col("o.customer_id"),
        col("oi.product_id"),
        col("p.supplier_id"),

        date_format(
            to_date(col("o.order_ts")),
            "yyyyMMdd"
        ).cast("int").alias("date_key"),

        col("o.order_ts"),

        col("oi.quantity"),
        col("oi.unit_price"),

        (
            col("oi.quantity") *
            col("oi.unit_price")
        ).alias("line_total"),

        col("o.status").alias("order_status"),
        col("o.currency")
    )
)

(
    fact_sales.write
    .mode("overwrite")
    .parquet(f"{GOLD}/fact_sales")
)


# ============================================================
# FACT PAYMENTS
# ============================================================

print("Building fact_payments...")

fact_payments = (
    payments
    .withColumn(
        "date_key",
        date_format(
            to_date(col("payment_ts")),
            "yyyyMMdd"
        ).cast("int")
    )
)

(
    fact_payments.write
    .mode("overwrite")
    .parquet(f"{GOLD}/fact_payments")
)


# ============================================================
# FACT SHIPMENTS
# ============================================================

print("Building fact_shipments...")

fact_shipments = (
    shipments
    .withColumn(
        "date_key",
        date_format(
            to_date(col("shipped_ts")),
            "yyyyMMdd"
        ).cast("int")
    )
)

(
    fact_shipments.write
    .mode("overwrite")
    .parquet(f"{GOLD}/fact_shipments")
)


# ============================================================
# FACT EVENTS
# ============================================================

print("Building fact_events...")

fact_events = (
    events
    .withColumn(
        "date_key",
        date_format(
            to_date(col("event_ts")),
            "yyyyMMdd"
        ).cast("int")
    )
)

(
    fact_events.write
    .mode("overwrite")
    .parquet(f"{GOLD}/fact_events")
)


# ============================================================
# Finished
# ============================================================

print("==========================================")
print("GOLD LAYER COMPLETED")
print("==========================================")

print("dim_customer created")
print("dim_supplier created")
print("dim_product created")
print("dim_date created")
print("fact_sales created")
print("fact_payments created")
print("fact_shipments created")
print("fact_events created")

spark.stop()
