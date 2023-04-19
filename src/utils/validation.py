import re

from pyspark.sql.functions import col, when, lit
from pyspark.sql.functions import to_timestamp, desc, row_number
from pyspark.sql import Window
from datetime import datetime

email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
date_pattern = r'^\d{2}\.\d{2}\.\d{4}T\d{2}:\d{2}:\d{2}$'
# assuming we're working for newtube
url_pattern = r'^https:\/\/newtube\.com\/video\?v=[\da-zA-Z]{4,}$'

def validate_df(df, validation_rules):
    # introduce new is_valid column which we will use to sort the validated data
    df = df.withColumn('is_valid', lit(True))

    for condition, col_name in validation_rules:
        df = df.withColumn('is_valid',
                           when(col('is_valid'), condition).otherwise(False)
                           )

    valid = df.filter(col('is_valid'))
    invalid = df.filter(~col('is_valid'))

    valid = valid.drop('is_valid')
    invalid = invalid.drop('is_valid')

    return valid, invalid

# validate data and drop duplicates
def validate_user_data(df):
    # assume each column is required
    valid_countries = ['US', 'CA', 'UK']
    boolean_values = [0, 1]
    validation_rules = [
        (col('id').isNotNull(), 'id'),
        (col('fname').isNotNull() & (col('fname') != ''), 'fname'),
        (col('lname').isNotNull() & (col('lname') != ''), 'lname'),
        (col('email').rlike(email_pattern), 'email'),
        (col('country').isin(valid_countries), 'country'),
        (col('subscription').isin(boolean_values), 'subscription'),
        (col('updated').rlike(date_pattern), 'updated')
    ]

    valid, invalid = validate_df(df, validation_rules)

    # in case of duplicate user ids, keep the latest updated record
    valid = valid.withColumn('updated_ts', to_timestamp(col('updated'), "dd.MM.yyyy'T'HH:mm:ss"))
    window_spec = Window.partitionBy('id').orderBy(desc('updated_ts'))
    valid = valid.withColumn('row_number', row_number().over(window_spec)) \
        .filter(col('row_number') == 1) \
        .drop('row_number') \
        .drop('updated_ts')

    return valid, invalid


def validate_user_data_rdd(rdd):
    valid_countries = ['US', 'CA', 'UK']
    boolean_values = [0, 1]

    def is_valid(row):
        email_pattern_compiled = re.compile(email_pattern)
        date_pattern_compiled = re.compile(date_pattern)
        try:
            user_id, fname, lname, email, country, subscription, categories, updated = row
        except ValueError:
            return False

        return (isinstance(user_id, int) and fname and lname and email and country and subscription and updated
                and email_pattern_compiled.match(email) and date_pattern_compiled.match(updated)
                and country in valid_countries and int(subscription) in boolean_values)

    def parse_date(date_str):
        return datetime.strptime(date_str, '%d.%m.%YT%H:%M:%S')

    def get_user_id_and_update_date(row):
        user_id, _, _, _, _, _, _, updated = row
        return user_id, parse_date(updated)

    valid = rdd.filter(is_valid)
    invalid = rdd.filter(lambda row: not is_valid(row))

    # check for user id duplicates
    user_id_count = valid.map(lambda row: (row[0], 1)) \
        .reduceByKey(lambda a, b: a + b)
    duplicates_count = user_id_count.filter(lambda x: x[1] > 1).count()
    has_duplicates = duplicates_count > 0

    if has_duplicates:
        last_updated = valid.map(get_user_id_and_update_date) \
            .reduceByKey(lambda a, b: max(a, b)) \
            .collectAsMap()

        def is_latest_updated(row):
            user_id, _, _, _, _, _, _, updated = row
            return parse_date(updated) == last_updated[user_id]

        valid = valid.filter(is_latest_updated)

    return valid, invalid


def validate_video_data(df):
    boolean_values = [0, 1]
    validation_rules = [
        (col('id').isNotNull(), 'id'),
        (col('name').isNotNull() & (col('name') != ''), 'name'),
        (col('url').rlike(url_pattern), 'url'),
        (col('creation_timestamp').isNotNull() & (col('creation_timestamp') > 0), 'creation_timestamp'),
        (col('creator_id').isNotNull(), 'creator_id'),
        (col('private').isin(boolean_values), 'private')
    ]

    return validate_df(df, validation_rules)


def validate_event_data(df):
    valid_events = ['created', 'like', 'add_tags', 'remove_tags', 'commented']
    validation_rules = [
        (col('user_id').isNotNull(), 'user_id'),
        (col('video_id').isNotNull(), 'video_id'),
        (col('event').isNotNull() & (col('event').isin(valid_events)), 'event'),
        (col('timestamp').isNotNull() & (col('timestamp') > 0), 'timestamp'),
    ]

    return validate_df(df, validation_rules)