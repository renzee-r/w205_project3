#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf

@udf('boolean')
def is_item_event(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'buy_sword':
        return True
    return False


@udf('boolean')
def is_guild_event(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'join_guild':
        return True
    return False


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
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    extracted_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string'))

    extracted_events.show()

    extracted_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/events_all")

    # Create a separate parquet file for item-based events
    item_events = extracted_events \
        .filter(is_item_event('raw'))

    item_events = item_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()

    item_events.show()

    item_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/events_item")

    # Create a parquet file for guild-based events
    guild_events = extracted_events \
        .filter(is_guild_event('raw'))

    guild_events = guild_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()

    guild_events.show()

    guild_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/events_guild")

    # Create a parquet file for user-based events
    user_events = extracted_events \
        .filter(is_user_event('raw'))

    user_events = user_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()

    user_events.show()

    user_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/events_user")


if __name__ == "__main__":
    main()
