from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("StreamingApp") \
    .getOrCreate()

# Define the schema for the Kafka topic
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("highway_id", IntegerType(), True),
    StructField("ev", StringType(), True),
    StructField("plate", StringType(), True),    
    StructField("speed", DoubleType(), True),
])

# Read from Kafka topic
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker-1:9092") \
    .option("subscribe", "traffic-events") \
    .load()

# Parse the value from Kafka message
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Calculate the rolling average speed over a 5-minute window
windowed_df = parsed_df \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window(col("timestamp"), "5 minutes"), col("highway_id")) \
    .agg(avg("speed").alias("avg_speed"))

# Write the results to the console
query = windowed_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# Await termination
query.awaitTermination()
