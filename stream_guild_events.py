#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType


def guild_event_schema():
    """
    root
    |-- Accept: string (nullable = true)
    |-- Host: string (nullable = true)
    |-- User-Agent: string (nullable = true)
    |-- event_type: string (nullable = true)
    |-- username: string (nullable = true)
    |-- Cookie: string (nullable = true)
    |-- guild_name: string (nullable = true)
    |-- timestamp: string (nullable = true)
    """
    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("username", StringType(), True),
        StructField("Cookie", StringType(), True),
        StructField("guild_name", StringType(), True),
    ])


@udf('boolean')
def is_guild_event(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'join_guild':
        return True
    return False


def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("StreamGuildEventsJob") \
        .getOrCreate()

    raw_events = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .load()

    # Create a stream for buy events
    guild_events = raw_events \
        .filter(is_guild_event(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          guild_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')

    sink = guild_events \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_guild_events") \
        .option("path", "/tmp/events_guild") \
        .trigger(processingTime="10 seconds") \
        .start()

    sink.awaitTermination()


if __name__ == "__main__":
    main()
