from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)
utils_dir = os.path.join(parent_dir, 'utils')

sys.path.append(utils_dir)

from validation import validate_user_data


RAW_USERS_DATA = os.path.join(grandparent_dir, 'data', 'raw', 'users', '*.csv')
PROCESSED_VALID_USERS_DATA = os.path.join(grandparent_dir, 'data', 'core', 'users')
PROCESSED_INVALID_USERS_DATA = os.path.join(grandparent_dir, 'data', 'invalid', 'users')

schema = StructType([
    StructField('id', IntegerType(), nullable=False),
    StructField('fname', StringType(), nullable=False),
    StructField('lname', StringType(), nullable=False),
    StructField('email', StringType(), nullable=False),
    StructField('country', StringType(), nullable=False),
    StructField('subscription', StringType(), nullable=False),
    StructField('categories', StringType(), nullable=True),
    StructField('updated', StringType(), nullable=True),
])

spark = SparkSession.builder \
    .appName('Spark test app') \
    .getOrCreate()

input = spark.read \
    .option('header', 'true') \
    .schema(schema) \
    .csv(str(RAW_USERS_DATA))

valid, invalid = validate_user_data(input)

valid.write.mode('overwrite').partitionBy('country').parquet(PROCESSED_VALID_USERS_DATA)
invalid.write.mode('overwrite').parquet(PROCESSED_INVALID_USERS_DATA)

spark.stop()