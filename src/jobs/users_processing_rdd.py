from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)
utils_dir = os.path.join(parent_dir, 'utils')

sys.path.append(utils_dir)

from validation import validate_user_data_rdd

RAW_USERS_DATA = os.path.join(grandparent_dir, 'data', 'raw', 'users', 'users.csv')
PROCESSED_VALID_USERS_DATA = os.path.join(grandparent_dir, 'data', 'core', 'users')
PROCESSED_INVALID_USERS_DATA = os.path.join(grandparent_dir, 'data', 'invalid', 'users')


def parse_line(line):
    parts = line.split(',')
    parts[0] = int(parts[0])

    # fill missed values with None
    while len(parts) < len(schema):
        parts.append(None)

    return parts

# everything is nullable so that we could enforce schema on invalid records
schema = StructType([
    StructField('id', IntegerType(), nullable=True),
    StructField('fname', StringType(), nullable=True),
    StructField('lname', StringType(), nullable=True),
    StructField('email', StringType(), nullable=True),
    StructField('country', StringType(), nullable=True),
    StructField('subscription', StringType(), nullable=True),
    StructField('categories', StringType(), nullable=True),
    StructField('updated', StringType(), nullable=True),
])

spark = SparkSession.builder \
    .appName('Spark test app') \
    .getOrCreate()

sc = spark.sparkContext

data = sc.textFile(RAW_USERS_DATA)

# parse data without header and convert the id field to integer
header = data.first()
data = data.filter(lambda line: line != header) \
    .map(parse_line)

valid, invalid = validate_user_data_rdd(data)
valid_df = spark.createDataFrame(valid, schema=schema)

invalid_df = spark.createDataFrame(invalid, schema=schema)

valid_df.write.mode('overwrite').partitionBy('country').parquet(PROCESSED_VALID_USERS_DATA)
invalid_df.write.mode('overwrite').parquet(PROCESSED_INVALID_USERS_DATA)


