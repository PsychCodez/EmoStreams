#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, from_json
from pyspark.sql.types import StructType, StructField, StringType
from confluent_kafka import Producer
import json

# Initialize Spark session with Kafka integration
spark = SparkSession.builder \
    .appName("Emoji Aggregator") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

# Set up the Kafka Producer for feedback
producer = Producer({
    'bootstrap.servers': 'localhost:9092',  # Kafka server
})

# Define the schema for the incoming JSON data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Read emoji data from Kafka (from emoji_topic)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "emoji_topic") \
    .load()

# Parse the 'value' column (which contains the message in binary format) as JSON
emoji_data = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.user_id", "data.emoji_type", "data.timestamp")

# Aggregation logic: Group by emoji type in 2-second windows
aggregated_data = emoji_data \
    .groupBy(window(col("timestamp"), "2 seconds"), "emoji_type") \
    .count()

# Function to send feedback to Kafka
def send_feedback_to_kafka(aggregated_df, epoch_id):
    feedback = aggregated_df.collect()

    # Log the microbatch creation
    print(f"Microbatch created at epoch {epoch_id}: {feedback}")

    for row in feedback:
        if row['count'] >= 50:  # Send feedback if threshold is met
            feedback_data = {
                "emoji_type": row['emoji_type'],
                "count": row['count'],
                "timestamp": str(row['window']['start'])
            }
            # Send feedback data to the Kafka 'emoji_feedback_topic'
            producer.produce('emoji_feedback_topic', value=json.dumps(feedback_data))
            producer.flush()  # Ensure the data is sent
            print(f"Feedback sent: {feedback_data}")  # Log the sent feedback

# Write the aggregated data as a stream to trigger the function
query = aggregated_data \
    .writeStream \
    .foreachBatch(send_feedback_to_kafka) \
    .outputMode("update") \
    .start()

query.awaitTermination()
