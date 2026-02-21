from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Load to ScyllaDB")

    # Delta configs (already working)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Cassandra / Scylla configs
    .config("spark.cassandra.connection.host", "scylla")
    .config("spark.cassandra.connection.port", "9042")

    .getOrCreate()
)

# -------------------------------
# Paths
# -------------------------------

INPUT_PATH = "/opt/spark/delta-lake/customer_transactions/transformed"

# -------------------------------
# Read transformed delta data
# -------------------------------

df = spark.read.format("delta").load(INPUT_PATH)

df.show(5)

print("Data read from Delta Lake, now writing to ScyllaDB...")

(
    df.write
    .format("org.apache.spark.sql.cassandra")
    .mode("append")  # append = upsert
    .option("keyspace", "adsremedy")
    .option("table", "daily_customer_totals")
    .option("spark.cassandra.output.consistency.level", "LOCAL_ONE")
    .option("spark.cassandra.output.batch.size.rows", "100")
    .save()
)


print("Data loaded into ScyllaDB!")

spark.stop()