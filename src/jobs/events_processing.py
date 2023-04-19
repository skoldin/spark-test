import re

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, LongType
from pyspark.sql.functions import from_json, explode, col, when, array, lit
import os
import sys
import json

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)
utils_dir = os.path.join(parent_dir, 'utils')
sys.path.append(utils_dir)

from validation import validate_event_data

RAW_EVENTS_DATA = os.path.join(grandparent_dir, 'data', 'raw', 'events', '*.jsonl')
PROCESSED_VALID_EVENTS_DATA = os.path.join(grandparent_dir, 'data', 'core', 'events')
PROCESSED_INVALID_EVENTS_DATA = os.path.join(grandparent_dir, 'data', 'invalid', 'events')

spark = SparkSession.builder \
    .appName('Spark test app') \
    .getOrCreate()

event_schema = StructType([
    StructField('user_id', IntegerType(), nullable=False),
    StructField('video_id', IntegerType(), nullable=False),
    StructField('event', StringType(), nullable=False),
    StructField('timestamp', LongType(), nullable=False),
    StructField('tags', StringType(), nullable=True),
    StructField('comment', StringType(), nullable=True)
])


def split_jsonl(content):
    stack = []
    current_object = ''
    json_objects = []

    for char in content:
        if char == '{':
            stack.append(char)
        elif char == '}':
            stack.pop()

        current_object += char

        if len(stack) == 0 and current_object.strip() != '':
            json_objects.append(current_object.strip())
            current_object = ''

    return json_objects


def extract_event(event_json):
    user_id = event_json['user_id']
    video_id = event_json['video_id']
    event = event_json['event']
    timestamp = event_json['timestamp']
    tags = event_json.get('tags', None)
    comment = event_json.get('comment', None)

    if tags:
        tags = ','.join(tags)

    return user_id, video_id, event, timestamp, tags, comment


def flatten_event(event):
    event_json = json.loads(event)

    if "events" not in event_json:
        event_tuple = extract_event(event_json)
        return [Row(*event_tuple)]

    return [Row(*extract_event(event)) for event in event_json['events']]


rdd = spark.sparkContext.wholeTextFiles(RAW_EVENTS_DATA)
jsonl_rdd = rdd.flatMap(lambda x: split_jsonl(x[1]))

flat_events_rdd = jsonl_rdd.flatMap(flatten_event)

flat_events = spark.createDataFrame(flat_events_rdd, schema=event_schema)

valid, invalid = validate_event_data(flat_events)

valid.write.mode('overwrite').parquet(PROCESSED_VALID_EVENTS_DATA)
invalid.write.mode('overwrite').parquet(PROCESSED_INVALID_EVENTS_DATA)

spark.stop()

