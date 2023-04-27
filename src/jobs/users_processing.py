from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)
processed_users_data_dir = os.path.join(grandparent_dir, 'data', 'processed', 'users')

from src.utils.validation import validate_user_data
from src.utils.process_by_file import process_by_file
from src.processor import Processor

RAW_USERS_DATA = os.path.join(grandparent_dir, 'data', 'raw', 'users')
PROCESSED_VALID_USERS_DATA = os.path.join(grandparent_dir, 'data', 'core', 'users')
PROCESSED_INVALID_USERS_DATA = os.path.join(grandparent_dir, 'data', 'invalid', 'users')


class UsersDataProcessor(Processor):
    def process_data(self, input_df, valid_output_dir, invalid_output_dir):
        valid, invalid = validate_user_data(input_df)

        valid.write.mode('overwrite').partitionBy('country').parquet(valid_output_dir)
        invalid.write.mode('overwrite').parquet(invalid_output_dir)


if __name__ == '__main__':
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

    data_processor = UsersDataProcessor()
    process_by_file(
        spark=spark,
        schema=schema,
        raw_data_path=RAW_USERS_DATA,
        valid_data_path=PROCESSED_VALID_USERS_DATA,
        invalid_data_path=PROCESSED_INVALID_USERS_DATA,
        processed_files_dir=processed_users_data_dir,
        data_processor=data_processor
    )

    spark.stop()
