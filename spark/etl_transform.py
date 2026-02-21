from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum

# -------------------------------
# Spark Session
# -------------------------------

spark = (
    SparkSession.builder
    .appName("Delta ETL Transformation")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# -------------------------------
# Paths
# -------------------------------

DELTA_TABLE_PATH = "/opt/spark/delta-lake/customer_transactions/delta_table"
OUTPUT_PATH = "/opt/spark/delta-lake/customer_transactions/transformed"

# -------------------------------
# Read Delta
# -------------------------------

print("Reading delta table...")

df = spark.read.format("delta").load(DELTA_TABLE_PATH)

df.show(5)

# -------------------------------
# Step 1: Deduplication
# -------------------------------

print("Removing duplicates...")

df_dedup = df.dropDuplicates(["transaction_id"])

# -------------------------------
# Step 2: Data Validation
# -------------------------------

print("Filtering invalid amounts...")

df_valid = df_dedup.filter(col("amount") > 0)

# -------------------------------
# Step 3: Date Processing
# -------------------------------

print("Adding transaction_date column...")

df_processed = df_valid.withColumn(
    "transaction_date",
    to_date(col("timestamp"))
)

# -------------------------------
# Step 4: Aggregation
# -------------------------------

print("Aggregating daily totals...")

df_daily = (
    df_processed
    .groupBy("customer_id", "transaction_date")
    .agg(
        _sum("amount").alias("daily_total")
    )
)

df_daily.show(10)

# -------------------------------
# Save Output (Delta format)
# -------------------------------

df_daily.write.format("delta").mode("overwrite").save(OUTPUT_PATH)

print("Transformation complete!")

spark.stop()
