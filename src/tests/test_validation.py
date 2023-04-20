import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField
from src.utils.validation import validate_user_data_rdd, validate_user_data, validate_video_data, validate_event_data
from src.jobs.users_processing import schema as user_schema

test_video_data_schema = StructType([
    StructField('id', StringType(), nullable=True),
    StructField('name', StringType(), nullable=True),
    StructField('url', StringType(), nullable=True),
    StructField('creation_timestamp', StringType(), nullable=True),
    StructField('creator_id', StringType(), nullable=True),
    StructField('private', StringType(), nullable=True)
])

user_data = [
    (0, 'John', 'Smith', 'John.Smith@jmamil.com', 'US', 1, 'animals;duck;cute;travel', '20.10.2023T12:00:00'),
    (0, 'John', 'Smith', 'John.Smith@jmamil.com', 'US', 1, 'animals;travel', '20.10.2023T11:00:00'),
    (7, 'Bob', 'Miller', 'Bob.Miller@jmamil.com', 'US', 1, 'animals;food;travel;bicycle', '20.10.2023T12:00:00'),
    (8, 'Mary', 'Anderson', 'Mary.Anderson@jmamil.com', 'UK', 0, 'animals;food;travel;bicycle',
     '20.10.2023T12:00:00'),
    (9, 'Bob', 'Anderson', 'xxx', 'xxx', 'xx', 'x', '20.10.2023T12:00:00')
]

video_data = [
    (110, 'Home', 'https://newtube.com/video?v=c8Ks', '1642765647', 6, 1),
    (999, 'hhh', 'sss', 'd', 's', None)
]

events_data = [
    {"user_id": 1, "video_id": 100, "event": "created", "timestamp": 1642663347, "tags": ["cute", "duck", "animals"]},
    {"user_id": 4, "video_id": 100, "event": "like", "timestamp": 1642943347}
]


class TestValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName('unittest') \
            .master('local[2]') \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_validate_user_data(self):
        df = self.spark.createDataFrame(user_data, schema=user_schema)

        valid, invalid = validate_user_data(df)

        self.assertEqual(valid.count(), 3)
        self.assertEqual(invalid.count(), 1)

    def test_validate_user_data_rdd(self):
        rdd = self.spark.sparkContext.parallelize(user_data)

        valid, invalid = validate_user_data_rdd(rdd)

        self.assertEqual(valid.count(), 3)
        self.assertEqual(invalid.count(), 1)

    def test_validate_video_data(self):
        df = self.spark.createDataFrame(video_data, schema=test_video_data_schema)

        valid, invalid = validate_video_data(df)

        self.assertEqual(valid.count(), 1)
        self.assertEqual(valid.count(), 1)

    def test_validate_event_data(self):
        df = self.spark.createDataFrame(events_data)

        valid, invalid = validate_event_data(df)

        self.assertEqual(valid.count(), 2)
        self.assertEqual(invalid.count(), 0)


