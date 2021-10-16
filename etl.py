import configparser
import os

import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

START_TIME_COLUMN_NAME = 'start_time'

config = configparser.ConfigParser()
# Copied code how to get vars from config without header section from
# https://stackoverflow.com/questions/2819696/parsing-properties-file-in-python/2819788#2819788
with open('dl.cfg', 'r') as file:
    config_string = '[default]\n' + file.read()
config.read_string(config_string)

# Remove ` at the start and end of the string
os.environ['AWS_ACCESS_KEY_ID'] = config.get('default', 'AWS_ACCESS_KEY_ID')[1:-1]
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('default', 'AWS_SECRET_ACCESS_KEY')[1:-1]


def create_spark_session():
    """
    Creates and returns spark session
    """
    global logger

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .getOrCreate()

    # set spark log levels to warn
    log4j = spark.sparkContext._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    log4j.LogManager.getLogger('org.apache.spark').setLevel(log4j.Level.WARN)
    log4j.LogManager.getLogger('org.spark-project').setLevel(log4j.Level.WARN)

    logger = log4j.LogManager.getLogger('ETL')
    logger.setLevel(log4j.Level.INFO)

    # set aws s3 properties
    hadoop_configuration = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_configuration.set("fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])
    hadoop_configuration.set("fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])
    hadoop_configuration.set("fs.s3a.aws.credentials.provider",
                             "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    return spark


def process_song_data(spark, input_data_path, output_data_path):
    """
    Process song data creating various table associated with it
    :param spark: spark session
    :param input_data_path: input data location
    :param output_data_path: output data location
    """
    # get filepath to song data file
    song_data = input_data_path + "/song-data/A/A/C"
    # read song data file
    logger.info(f"Reading song data from {song_data}")
    song_data_df = spark.read \
        .option("recursiveFileLookup", "true") \
        .json(song_data)
    song_data_df = song_data_df.withColumn("year",
                                           f.when(f.col("year") == 0, None).otherwise(f.col(
                                               "year")))
    logger.info("Caching song data")
    song_data_df.cache()

    create_songs_table(output_data_path, song_data_df)

    create_artists_table(output_data_path, song_data_df)


def create_artists_table(output_data_path, song_data_df):
    """
    Create and save artists table
    :param output_data_path: output data location
    :param song_data_df: song data dataframe
    """
    logger.info("Creating artists table")
    artists_table = song_data_df.select("artist_id", "artist_name", "artist_location",
                                        "artist_latitude",
                                        "artist_longitude")
    logger.info("Deduping artists")
    artists_table.dropDuplicates(['artist_id', 'artist_name'])
    artists_table.createOrReplaceTempView("artists")

    # write artists table to parquet files
    path = output_data_path + "/artists"
    logger.info(f"Writing artists table to {path}")
    artists_table.write \
        .mode("overwrite") \
        .parquet(path)


def create_songs_table(output_data_path, song_data_df):
    """
    Crate and save songs table
    :param output_data_path: output data location
    :param song_data_df: song data dataframe
    """
    # extract columns to create songs table
    logger.info("Creating songs table")
    songs_table = song_data_df.select('song_id', "title", "artist_id", "year", "duration")
    songs_table = songs_table.dropDuplicates(['song_id'])
    songs_table.createOrReplaceTempView("songs")

    # write songs table to parquet files partitioned by year and artist
    path = output_data_path + "/songs"
    logger.info(f"Writing songs table to {path}")
    songs_table.write \
        .partitionBy("year", "artist_id") \
        .mode("overwrite") \
        .parquet(path)


def process_log_data(spark, input_data_path, output_data_path):
    """
    Processes log data and creates/writes various table associated with it
    :param spark: spark session
    :param input_data_path: input data location
    :param output_data_path: output data location
    """
    # get filepath to log data file
    log_data = input_data_path + "/log-data/"

    # when reading json spark 2.4.3 cannot infer schema automatically
    log_data_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", LongType(), True),
        StructField("song", StringType(), True),
        StructField("status", LongType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True),
    ])

    logger.info("Reading log data files")
    # read log data file
    df = spark.read \
        .option("recursiveFileLookup", "true") \
        .schema(log_data_schema) \
        .json(log_data)
    # filter by actions for song plays
    df = df.where(f.col("page") == "NextSong")

    logger.info("Deduping log events")

    # dedupe events
    df = df.dropDuplicates(['ts', 'userid', 'sessionid', 'song', 'artist'])
    logger.info("Caching events")
    df.cache()

    create_users_table(df, output_data_path)

    # create timestamp column from original timestamp column
    df = df.withColumn(START_TIME_COLUMN_NAME, f.from_unixtime(f.col("ts") / 1000)).drop('ts')

    time_table = create_time_table(df, output_data_path, START_TIME_COLUMN_NAME)

    create_songplays_table(df, output_data_path, spark, START_TIME_COLUMN_NAME, time_table)


def create_songplays_table(log_df, output_data_path, spark, start_time_column_name, time_table_df):
    """
    Create and save songplays table
    :param log_df: dataframe with logs data
    :param output_data_path: output location
    :param spark: spark session
    :param start_time_column_name: 
    :param time_table_df: dataframe with time data
    """
    logger.info("Creating songplays table")
    # read in song data to use for songplays table
    song_df = log_df.withColumn('userAgent', f.regexp_replace('userAgent', '"', ''))
    artists_and_songs_df = spark.sql("SELECT * FROM songs s JOIN artists a USING(artist_id) ")
    song_df = song_df.join(artists_and_songs_df, [song_df.song == artists_and_songs_df.title, \
                                                  song_df.artist == artists_and_songs_df.artist_name, \
                                                  song_df.length == artists_and_songs_df.duration],
                           'left')
    song_df = song_df.join(time_table_df.alias("time_table"), [start_time_column_name], 'inner')
    # generate uuid
    # copied code from https://stackoverflow.com/questions/49785108/spark-streaming-with-python-how-to-add-a-uuid-column/50095755
    song_df = song_df.withColumn("songplay_id", f.expr("uuid()"))
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = song_df.selectExpr('songplay_id', start_time_column_name,
                                         'userId as user_id',
                                         'level', 'song_id', 'artist_id',
                                         'sessionId as session_id', 'location',
                                         'userAgent as user_agent', 'time_table.year', 'month')

    # write songplays table to parquet files partitioned by year and month
    path = output_data_path + '/songplays'
    logger.info(f"Writing songplays table to {path}")
    songplays_table \
        .write \
        .partitionBy("year", "month") \
        .mode('overwrite') \
        .json(path)


def create_time_table(log_df, output_data_path, start_time_column_name):
    """
    Creates and writes time table
    :param log_df: dataframe with log data
    :param output_data_path: output data location
    :param start_time_column_name:
    :return: dataframe with time data
    """
    logger.info("Creating time table")
    # extract columns to create time table
    time_table = log_df.selectExpr(start_time_column_name)
    time_table = time_table.dropDuplicates([start_time_column_name])
    time_table = time_table.selectExpr(start_time_column_name,
                                       f'hour({start_time_column_name}) as hour',
                                       f'day({start_time_column_name}) as day',
                                       f'weekofyear({start_time_column_name}) as week_of_year',
                                       f'month({start_time_column_name}) as month',
                                       f'year({start_time_column_name}) as year',
                                       f'dayofweek({start_time_column_name}) as weekday')

    # write time table to parquet files partitioned by year and month
    path = output_data_path + "/time"
    logger.info(f"Writing time table to {path}")
    time_table.write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(path)
    return time_table


def create_users_table(log_data_df, output_data_path):
    """
    Create and save users table
    :param log_data_df: log data dataframe
    :param output_data_path: output data location
    """
    logger.info("Creating users table")
    # Adapted from https://sparkbyexamples.com/pyspark/pyspark-window-functions/
    users_df = log_data_df.withColumn("row_number", f.row_number().over(
        Window.partitionBy("userId").orderBy(f.desc("ts"))))
    deduped_users_df = users_df.where(users_df.row_number == 1)
    users_table = deduped_users_df.select("userId", "firstName", "lastName", "gender", "level")

    # write users table to parquet files
    path = output_data_path + "/users"
    logger.info(f"Writing users table to {path}")
    users_table \
        .write \
        .mode("overwrite") \
        .parquet(path)


def main():
    input_data_path = "s3a://udacity-dend"
    output_data_path = "s3a://udacity-data-modelling/sparkify"

    if not output_data_path:
        raise ValueError('output_data_path is not set')

    spark = create_spark_session()

    process_song_data(spark, input_data_path, output_data_path)
    process_log_data(spark, input_data_path, output_data_path)


if __name__ == "__main__":
    main()
