# ============================================================
# Retail Sales Analytics using PySpark
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, max,
    dense_rank, when
)
from pyspark.sql.window import Window

# ------------------------------------------------------------
# 1. Spark Session with Hive Support
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("RetailSalesAnalytics") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------
# 2. Input Paths (Update as per your environment)
# ------------------------------------------------------------
base_path = "/data/olist"     # HDFS or local path
output_path = "/warehouse/retail_analytics"

# ------------------------------------------------------------
# 3. Load Data
# ------------------------------------------------------------
customers_df = spark.read.csv(f"{base_path}/olist_customers_dataset.csv", header=True, inferSchema=True)
orders_df = spark.read.csv(f"{base_path}/olist_orders_dataset.csv", header=True, inferSchema=True)
order_items_df = spark.read.csv(f"{base_path}/olist_order_items_dataset.csv", header=True, inferSchema=True)
order_payments_df = spark.read.csv(f"{base_path}/olist_order_payments_dataset.csv", header=True, inferSchema=True)
products_df = spark.read.csv(f"{base_path}/olist_products_dataset.csv", header=True, inferSchema=True)
sellers_df = spark.read.csv(f"{base_path}/olist_sellers_dataset.csv", header=True, inferSchema=True)
order_reviews_df = spark.read.csv(f"{base_path}/olist_order_reviews_dataset.csv", header=True, inferSchema=True)

# ------------------------------------------------------------
# 4. Basic EDA – Row Counts
# ------------------------------------------------------------
tables = {
    "customers": customers_df,
    "orders": orders_df,
    "order_items": order_items_df,
    "payments": order_payments_df,
    "products": products_df,
    "sellers": sellers_df,
    "reviews": order_reviews_df
}

print("===== Row Counts =====")
for name, df in tables.items():
    print(f"{name}: {df.count()}")

# ------------------------------------------------------------
# 5. NULL Value Analysis
# ------------------------------------------------------------
def null_count(df, name):
    print(f"\nNULL count for {name}")
    df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).show(truncate=False)

null_count(products_df, "products")
null_count(orders_df, "orders")
null_count(order_reviews_df, "reviews")

# ------------------------------------------------------------
# 6. Data Cleaning
# ------------------------------------------------------------

# Remove duplicate records
orders_df = orders_df.dropDuplicates(["order_id"])
order_items_df = order_items_df.dropDuplicates()

# Handle missing product categories
products_df = products_df.fillna({"product_category_name": "unknown"})

# ------------------------------------------------------------
# 7. Build Fact Table (Core Analytics Table)
# ------------------------------------------------------------
fact_orders_df = (
    order_items_df
    .join(orders_df, "order_id", "inner")
    .join(products_df, "product_id", "left")
    .join(sellers_df, "seller_id", "left")
    .join(order_payments_df, "order_id", "left")
)

# ------------------------------------------------------------
# 8. Business Insights
# ------------------------------------------------------------

print("\n===== Total Revenue =====")
fact_orders_df.agg(
    sum("price").alias("total_revenue")
).show()

print("\n===== Top 10 Products by Revenue =====")
fact_orders_df.groupBy("product_id") \
    .agg(sum("price").alias("revenue")) \
    .orderBy(col("revenue").desc()) \
    .show(10)

print("\n===== Payment Type Distribution =====")
order_payments_df.groupBy("payment_type") \
    .agg(
        count("order_id").alias("orders"),
        sum("payment_value").alias("total_payment")
    ).orderBy(col("total_payment").desc()) \
    .show()

# ------------------------------------------------------------
# 9. Window Function – Top Products per Seller
# ------------------------------------------------------------
window_spec = Window.partitionBy("seller_id").orderBy(col("price").desc())

top_seller_products_df = (
    fact_orders_df
    .withColumn("rank", dense_rank().over(window_spec))
    .filter(col("rank") <= 5)
    .select("seller_id", "product_id", "price", "rank")
)

print("\n===== Top 5 Products per Seller =====")
top_seller_products_df.show(20, truncate=False)

# ------------------------------------------------------------
# 10. Customer Order Frequency
# ------------------------------------------------------------
print("\n===== Top Customers by Order Count =====")
orders_df.groupBy("customer_id") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10)

# ------------------------------------------------------------
# 11. Write Curated Data to HDFS in Parquet
# ------------------------------------------------------------
fact_orders_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(f"{output_path}/fact_orders")

print(f"\nData written to {output_path}/fact_orders")

# ------------------------------------------------------------
# 12. Create Hive External Table
# ------------------------------------------------------------
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS retail_fact_orders (
    order_id STRING,
    order_item_id INT,
    product_id STRING,
    seller_id STRING,
    price DOUBLE,
    freight_value DOUBLE,
    product_category_name STRING,
    payment_type STRING,
    payment_value DOUBLE
)
STORED AS PARQUET
LOCATION '/warehouse/retail_analytics/fact_orders'
""")

print("\nHive external table `retail_fact_orders` created.")

# ------------------------------------------------------------
# 13. Sample Hive Query Validation
# ------------------------------------------------------------
print("\n===== Revenue by Product Category =====")
spark.sql("""
SELECT product_category_name,
       SUM(price) AS revenue
FROM retail_fact_orders
GROUP BY product_category_name
ORDER BY revenue DESC
""").show()

# ------------------------------------------------------------
# 14. Stop Spark Session
# ------------------------------------------------------------
spark.stop()
