from pyspark.sql import SparkSession

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("MySQL to Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# JDBC URL without user and password
jdbc_url = "jdbc:mysql://35.193.6.179:3306/testingDelta?useSSL=false&allowPublicKeyRetrieval=true"

# Connection properties with user, password, and driver
connection_properties = {
    "user": "user",
    "password": "Dishu_192",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read the 'customer' table from MySQL
df = spark.read.jdbc(url=jdbc_url, table="customer", properties=connection_properties)

# Write the DataFrame as Delta format to GCS
df.write.format("delta") \
    .mode("overwrite") \
    .save("gs://tryingdeltacdf/delta/customer")

spark.stop()
