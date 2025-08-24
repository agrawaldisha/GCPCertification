from pyspark.sql import SparkSession

# Initialize Spark with Delta support
spark = SparkSession.builder \
    .appName("CSV to Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read CSV
df = spark.read.format("csv").option("header", "true").load("gs://tryingdeltacdf/banking.csv")

# Write as Delta
df.write.format("delta").mode("overwrite").save("gs://tryingdeltacdf/banking._delta/")

print("✅ CSV successfully written in Delta format!")

spark.stop()

