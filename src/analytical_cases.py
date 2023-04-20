from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import desc, col, avg, explode, split, collect_list, array_distinct, udf
from pyspark.sql.types import ArrayType, StringType


def main(spark):
    events = spark.read.parquet('../data/core/events')
    users = spark.read.parquet('../data/core/users')
    videos = spark.read.parquet('../data/core/videos')

    def get_top_liked_videos():
        likes = events.filter(events['event'] == 'like')
        top_liked_videos = likes.groupBy('video_id').count().orderBy(desc('count')).limit(5)
        top_liked_videos.write.mode('overwrite').parquet('../data/datamart/top_liked_videos')

    def get_average_time_between_comment_and_like():
        likes = events.filter(events['event'] == 'like')
        comments = events.filter(events['event'] == 'commented')
        likes_and_comments = likes.alias('l').join(
            comments.alias('c'),
            (col('l.user_id') == col('c.user_id')) & (col('l.video_id') == col('c.video_id'))
        )

        time_diff = (
                col('l.timestamp').cast('long') - col('c.timestamp').cast('long')
        ).alias('time_diff')
        likes_and_comments = likes_and_comments.select('l.user_id', 'l.video_id', time_diff)
        avg_time_diff = likes_and_comments.groupBy('user_id', 'video_id').agg(avg('time_diff'))

        avg_time_diff.write.mode('overwrite').parquet('../data/datamart/average_like_comment_time_difference')

    def get_personal_user_statistics():
        public_videos = videos.filter(videos['private'] == 0)
        public_video_events = events.join(public_videos, on=col('video_id') == col('id'))

        likes = public_video_events.filter(public_video_events['event'] == 'like')
        comments = public_video_events.filter(public_video_events['event'] == 'commented')

        likes_count = likes.groupBy('user_id').count().withColumnRenamed('count', 'likes_count')
        comments_count = comments.groupBy('user_id').count().withColumnRenamed('count', 'comments_count')

        user_stats = users.alias('u').join(likes_count.alias('lc'), on=col('u.id') == col('lc.user_id'),
                                           how='left_outer') \
            .join(comments_count.alias('cc'), on=col('u.id') == col('cc.user_id'), how='left_outer')
        user_stats = user_stats.fillna(0, subset=['likes_count', 'comments_count'])

        user_stats.select('fname', 'lname', 'likes_count', 'comments_count').write.mode('overwrite').parquet(
            '../data/datamart/user_stats')

    def update_tags(tags_list, events_list):
        current_tags = set()

        for tags, event_type in zip(tags_list, events_list):
            if event_type == 'created' or event_type == 'add_tags':
                current_tags.update(tags)
            elif event_type == 'remove_tags':
                current_tags.difference_update(tags)

        return list(current_tags)

    update_tags_udf = udf(update_tags, ArrayType(StringType()))

    def get_likes_by_category():
        likes = events.filter(events['event'] == 'like').select('user_id', 'video_id', 'timestamp').distinct()
        tags_events = events.filter(col('event').isin(['add_tags', 'remove_tags', 'created'])) \
            .withColumn('tags', split('tags', ','))

        tags_with_likes = tags_events.alias('te').join(
            likes.alias('l'),
            (col('te.video_id') == col('l.video_id')) & (col('te.timestamp') < col('l.timestamp'))
        )

        window_spec = Window.partitionBy('te.video_id', 'l.timestamp').orderBy('te.timestamp')

        tags_with_likes = tags_with_likes \
            .withColumn('current_tags', collect_list(col('te.tags')).over(window_spec)) \
            .withColumn('event_types', collect_list(col('te.event')).over(window_spec)) \
            .withColumn('final_tags', update_tags_udf('current_tags', 'event_types'))

        likes_by_category = (
            tags_with_likes
            .withColumn('category', explode('final_tags'))
            .groupBy('category')
            .count()
            .orderBy(desc('count'))
        )

        likes_by_category.write.mode('overwrite').parquet('../data/datamart/likes_by_category')

    get_likes_by_category()
    get_personal_user_statistics()
    get_average_time_between_comment_and_like()
    get_top_liked_videos()


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('Analytical cases') \
        .getOrCreate()

    main(spark)

    spark.stop()
