import configparser
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .master("local[*]")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ[
        'AWS_ACCESS_KEY_ID'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",
                                                      os.environ['AWS_SECRET_ACCESS_KEY'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                                      "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "/song-data"
    print(f"song data folder {song_data}")
    # read song data file
    df = spark.read \
        .option("recursiveFileLookup", "true") \
        .json(song_data)
    df = df.withColumn("year", when(col("year") == 0, None).otherwise(col("year")))
    df.cache()

    # extract columns to create songs table
    songs_table = df.select('song_id', "title", "artist_id", "year", "duration")
    songs_table = songs_table.dropDuplicates(['song_id'])
    songs_table.createOrReplaceTempView("songs")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
        .partitionBy("year", "artist_id") \
        .mode("overwrite") \
        .parquet(output_data + "/songs")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude",
                              "artist_longitude")
    artists_table.dropDuplicates(['artist_id', 'artist_name'])
    artists_table.createOrReplaceTempView("artists")

    # write artists table to parquet files
    artists_table.write \
        .mode("overwrite") \
        .parquet(output_data + "/artists")


# TODO: rename all columns to snake case on start
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "/log-data"

    # read log data file
    df = spark.read \
        .option("recursiveFileLookup", "true") \
        .json(log_data)

    # filter by actions for song plays
    df = df.where(col("page") == "NextSong")

    # dedupe events
    df = df.dropDuplicates(['ts', 'userid', 'sessionid', 'song', 'artist'])
    df.cache()

    # extract columns for users table


    # Adapted from https://sparkbyexamples.com/pyspark/pyspark-window-functions/
    deduped_users_df = df.withColumn("row_number", row_number().over(
        Window.partitionBy("userId").orderBy(desc("ts"))))
    users_table = deduped_users_df.select("userId", "firstName", "lastName", "gender", "level") \
        .where(deduped_users_df.row_number == 1)

    # write users table to parquet files
    users_table \
        .write \
        .parquet(output_data + "/users")

    # create timestamp column from original timestamp column
    start_time_column_name = 'start_time'
    df = df.withColumn(start_time_column_name, from_unixtime(col("ts") / 1000)).drop('ts')

    # extract columns to create time table
    time_table = df.selectExpr(start_time_column_name)
    time_table = time_table.dropDuplicates([start_time_column_name])
    time_table = time_table.selectExpr(start_time_column_name,
                                       f'hour({start_time_column_name}) as hour',
                                       f'day({start_time_column_name}) as day',
                                       f'weekofyear({start_time_column_name}) as week_of_year',
                                       f'month({start_time_column_name}) as month',
                                       f'year({start_time_column_name}) as year',
                                       f'dayofweek({start_time_column_name}) as weekday')

    # write time table to parquet files partitioned by year and month
    time_table.write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(output_data + "/time")

    # read in song data to use for songplays table
    song_df = df.withColumn('userAgent', regexp_replace('userAgent', '"', ''))

    artists_and_songs_df = spark.sql("SELECT * FROM songs s JOIN artists a USING(artist_id) ")

    song_df = song_df.join(artists_and_songs_df, [song_df.song == artists_and_songs_df.title, \
                                                  song_df.artist == artists_and_songs_df.artist_name, \
                                                  song_df.length == artists_and_songs_df.duration],
                           'left')
    song_df = song_df.join(time_table.alias("time_table"), [start_time_column_name], 'inner')

    # generate uuid
    # copied code from https://stackoverflow.com/questions/49785108/spark-streaming-with-python-how-to-add-a-uuid-column/50095755
    song_df = song_df.withColumn("songplay_id", expr("uuid()"))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = song_df.selectExpr('songplay_id', start_time_column_name,
                                         'userId as user_id',
                                         'level', 'song_id', 'artist_id',
                                         'sessionId as session_id', 'location',
                                         'userAgent as user_agent', 'time_table.year', 'month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table \
        .write \
        .partitionBy("year", "month") \
        .mode('overwrite') \
        .json(output_data + '/songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"

    # input_data = "file:///" + os.getcwd() + "/data"
    # output_data = os.getcwd() + "/out/"
    output_data = "s3a://udacity-data-modelling/sparkify"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
