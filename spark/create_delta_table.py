from pyspark.sql import SparkSession

# -------------------------------
# Create Spark Session WITH Delta
# -------------------------------

spark = (
    SparkSession.builder
    .appName("Create Delta Table")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# -------------------------------
# Paths (inside container)
# -------------------------------

RAW_DATA_PATH = "/opt/spark/delta-lake/customer_transactions/raw_data.csv"
DELTA_TABLE_PATH = "/opt/spark/delta-lake/customer_transactions/delta_table"

# -------------------------------
# Read CSV
# -------------------------------

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(RAW_DATA_PATH)
)

print("Preview Raw Data:")
df.show(5)

# -------------------------------
# Write Delta Table
# -------------------------------

(
    df.write
    .format("delta")
    .mode("overwrite")
    .save(DELTA_TABLE_PATH)
)

print("Delta table created!")

# -------------------------------
# Verify Read
# -------------------------------

delta_df = spark.read.format("delta").load(DELTA_TABLE_PATH)

print("Preview Delta Data:")
delta_df.show(3)

# -------------------------------
# Show Delta History (IMPORTANT!)
# -------------------------------

spark.sql(
    f"DESCRIBE HISTORY delta.`{DELTA_TABLE_PATH}`"
).show(truncate=False)

spark.stop()
