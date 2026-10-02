USE ecommerce_dw;

CREATE EXTERNAL TABLE IF NOT EXISTS dim_customer (
    customer_id BIGINT,
    full_name STRING,
    email STRING,
    country STRING,
    city STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION '/data/ecommerce/gold/dim_customer';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_supplier (
    supplier_id BIGINT,
    supplier_name STRING,
    country STRING,
    created_at TIMESTAMP
)
STORED AS PARQUET
LOCATION '/data/ecommerce/gold/dim_supplier';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_product (
    product_id BIGINT,
    supplier_id BIGINT,
    sku STRING,
    product_name STRING,
    category STRING,
    unit_price DECIMAL(12,2),
    unit_cost DECIMAL(12,2),
    active BOOLEAN
)
STORED AS PARQUET
LOCATION '/data/ecommerce/gold/dim_product';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
    `date` DATE,
    date_key INT,
    `year` INT,
    quarter INT,
    `month` INT,
    `day` INT,
    day_of_week INT,
    is_weekend BOOLEAN
)
STORED AS PARQUET
LOCATION '/data/ecommerce/gold/dim_date';

CREATE EXTERNAL TABLE IF NOT EXISTS fact_sales (
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

CREATE EXTERNAL TABLE IF NOT EXISTS fact_payments (
    payment_id STRING,
    order_id INT,
    customer_id INT,
    payment_ts TIMESTAMP,
    amount DOUBLE,
    currency STRING,
    payment_method STRING,
    payment_status STRING,
    transaction_ref STRING,
    date_key INT
)
STORED AS PARQUET
LOCATION '/data/ecommerce/gold/fact_payments';

CREATE EXTERNAL TABLE IF NOT EXISTS fact_shipments (
    shipment_id STRING,
    order_id INT,
    customer_id INT,
    carrier STRING,
    tracking_number STRING,
    shipped_ts TIMESTAMP,
    delivered_ts TIMESTAMP,
    shipment_status STRING,
    date_key INT
)
STORED AS PARQUET
LOCATION '/data/ecommerce/gold/fact_shipments';

CREATE EXTERNAL TABLE IF NOT EXISTS fact_events (
    event_id STRING,
    event_ts TIMESTAMP,
    event_type STRING,
    customer_id BIGINT,
    order_id BIGINT,
    product_id BIGINT,
    device STRING,
    session_id STRING,
    date_key INT
)
STORED AS PARQUET
LOCATION '/data/ecommerce/gold/fact_events';

SHOW TABLES;
