#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf


@udf('string')
def munge_event(event_as_json):
    event = json.loads(event_as_json)
    event['Cache-Control'] = "no-cache"
    return json.dumps(event)


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

    munged_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .withColumn('munged', munge_event('raw'))

    extracted_events = munged_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.munged))) \
        .toDF()

    extracted_events.show()

    extracted_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/events_all")

    # Create a separate parquet file for item-based events
    item_events = extracted_events \
        .filter(extracted_events.event_type == 'buy_sword')

    item_events.show()

    item_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/events_item")

    # Create a parquet file for guild-based events
    guild_events = extracted_events \
        .filter(extracted_events.event_type == 'join_guild')

    guild_events.show()

    guild_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/events_guild")

    # Create a parquet file for user-based events
    user_events = extracted_events \
        .filter((extracted_events.event_type == 'signup') | (extracted_events.event_type == 'login'))

    user_events.show()

    user_events \
        .write \
        .mode("overwrite") \
        .parquet("/tmp/events_user")


if __name__ == "__main__":
    main()
