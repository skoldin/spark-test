import re

from pyspark.sql.functions import col, when, lit

email_pattern = re.compile(r'^[\w\.-]+@[\w\.-]+\.\w+$')
date_pattern = re.compile(r'^\d{2}\.\d{2}\.\d{4}T\d{2}:\d{2}:\d{2}$')


def validate_user_data(df):
    # assume each column is required
    valid_countries = ['US', 'CA', 'UK']
    boolean = [0, 1]
    validation_rules = [
        (col('id').isNotNull(), "id"),
        (col('fname').isNotNull() & (col('fname') != ''), 'fname'),
        (col('lname').isNotNull() & (col('lname') != ''), 'lname'),
        (col('email').rlike(email_pattern), 'email'),
        (col('country').isin(valid_countries), 'country'),
        (col('subscription').isin(boolean), 'subscription'),
        (col('updated').rlike(date_pattern), 'updated')
    ]

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


def validate_user_data_rdd(rdd):
    valid_countries = ['US', 'CA', 'UK']
    boolean = [0, 1]

    def is_valid(row):
        user_id, fname, lname, email, country, subscription, categories, updated = row

        return (user_id and fname and lname and email and country and subscription and updated
                and email_pattern.match(email) and date_pattern.match(updated)
                and country in valid_countries and str(subscription) in boolean)

    valid = rdd.filter(is_valid)
    invalid = rdd.filter(lambda row: not is_valid(row))

    return valid, invalid