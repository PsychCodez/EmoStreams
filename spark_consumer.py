from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import requests

# Initialize Spark session with Kafka connector package
spark = SparkSession.builder \
    .appName("EmojiAggregator") \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Set log level to WARN to suppress unnecessary info/debug logs
spark.sparkContext.setLogLevel("WARN")

# Define schema for emoji data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
])

# Read emoji data from Kafka
emoji_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "emoji_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON data and apply the schema
parsed_df = emoji_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Define a window for micro-batching (2-second intervals) and aggregate the emojis
aggregated_df = parsed_df \
    .withWatermark("timestamp", "2 seconds") \
    .groupBy(
        window(col("timestamp"), "2 seconds"),
        col("emoji_type")
    ) \
    .agg(count("emoji_type").alias("emoji_count")) \
    .select(
        col("emoji_type"),
        expr("IF(emoji_count >= 1000, emoji_count, emoji_count)").alias("scaled_count")
    )

# Global variable to track the total count of emojis
total_emoji_count = 0

# Function to store representative emojis and count total emojis
representative_emoji_list = []  # List to store representative emojis

def store_representative_emoji(batch_df, batch_id):
    global representative_emoji_list, total_emoji_count
    # Remove duplicates by selecting distinct emoji_type
    distinct_batch_df = batch_df.select("emoji_type", "scaled_count").distinct()

    # Collect the distinct rows
    result = distinct_batch_df.collect()

    # Process each row and add representative emoji for every 20 emojis
    for row in result:
        emoji_count = row["scaled_count"]
        emoji_type = row["emoji_type"]

        # Add emoji count to the total count
        total_emoji_count += emoji_count

        # For every 20 emojis, store the emoji in the list
        for i in range(emoji_count // 20):  # For every 20 emojis
            representative_emoji_list.append(emoji_type)  # Store the representative emoji

    # Print the updated list and total emoji count for each batch
    print(f"Batch {batch_id} - Representative Emojis List: {representative_emoji_list}")
    print(f"Batch {batch_id} - Total Emoji Count: {total_emoji_count}")
    response = requests.post("http://localhost:3000/receive-representative-emojis", json={"message": representative_emoji_list})
    if response.status_code == 200:
        print(f"Batch {batch_id}: Sent 'gotcha' to server successfully.")
    else:
        print(f"Batch {batch_id}: Failed to send 'gotcha' to server. Status code: {response.status_code}")

# Start the streaming query using foreachBatch to handle batch processing
query1 = aggregated_df \
    .writeStream \
    .foreachBatch(store_representative_emoji) \
    .outputMode("update") \
    .trigger(processingTime="2 seconds") \
    .start()

# Start a separate query to output the aggregated table to the console for debugging
query2 = aggregated_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="2 seconds") \
    .start()

# Await termination of the streams
query1.awaitTermination()
query2.awaitTermination()
