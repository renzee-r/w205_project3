#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


def user_event_schema():
    """
    root
    |-- Accept: string (nullable = true)
    |-- Host: string (nullable = true)
    |-- User-Agent: string (nullable = true)
    |-- event_type: string (nullable = true)
    |-- user_info: Struct (nullable = true)
    |-- Cookie: string (nullable = true)
    |-- timestamp: string (nullable = true)
    """
    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_info", user_info_schema(), True),
        StructField("Cookie", StringType(), True),
    ])


def user_info_schema():
    """
    root
    |-- password: string (nullable = true)
    |-- session_datetime: string (nullable = true)
    |-- inventory: array (nullable = true)
    """
    return StructType([
        StructField("password", StringType(), True),
        StructField("session_datetime", StringType(), True),
        StructField("inventory", ArrayType(StringType()), True),
    ])


@udf('boolean')
def is_user_event(event_as_json):
    event = json.loads(event_as_json)
    if (event['event_type'] == 'signup') or (event['event_type'] == 'login'):
        return True
    return False


def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .getOrCreate()

    raw_events = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .load()

    # Create a stream for user events
    user_events = raw_events \
        .filter(is_user_event(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          user_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')

    sink = user_events \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_user_events") \
        .option("path", "/tmp/events_user") \
        .trigger(processingTime="10 seconds") \
        .start()

    sink.awaitTermination()


if __name__ == "__main__":
    main()
