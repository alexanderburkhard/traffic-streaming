from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max as max_
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Initialize SparkSession
spark = SparkSession \
    .builder \
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
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Apply watermark on the timestamp column
parsed_df = parsed_df.withWatermark("timestamp", "10 minutes")

# Define window duration and slide interval
window_duration = "2 minutes"
slide_duration = "30 seconds"

# Compute 5-minute moving average for speed per highway_id
moving_avg_df = parsed_df \
    .groupBy(window(col("timestamp"), window_duration, slide_duration), col("highway_id")) \
    .agg(avg("speed").alias("avg_speed"))

# Stream the moving average result to the console
query = moving_avg_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
