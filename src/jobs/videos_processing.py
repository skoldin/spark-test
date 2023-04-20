from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)

from src.utils.validation import validate_video_data

spark = SparkSession.builder \
    .appName('Spark test app') \
    .getOrCreate()


RAW_VIDEOS_DATA = os.path.join(grandparent_dir, 'data', 'raw', 'videos', '*.csv')
PROCESSED_VALID_VIDEOS_DATA = os.path.join(grandparent_dir, 'data', 'core', 'videos')
PROCESSED_INVALID_VIDEOS_DATA = os.path.join(grandparent_dir, 'data', 'invalid', 'videos')

schema = StructType([
    StructField('id', IntegerType(), nullable=False),
    StructField('name', StringType(), nullable=False),
    StructField('url', StringType(), nullable=False),
    StructField('creation_timestamp', StringType(), nullable=False),
    StructField('creator_id', IntegerType(), nullable=False),
    StructField('private', IntegerType(), nullable=True)
])

input = spark.read \
    .option('header', 'true') \
    .schema(schema) \
    .csv(str(RAW_VIDEOS_DATA))

valid, invalid = validate_video_data(input)

valid.write.mode('overwrite').parquet(PROCESSED_VALID_VIDEOS_DATA)
invalid.write.mode('overwrite').parquet(PROCESSED_INVALID_VIDEOS_DATA)

spark.stop()