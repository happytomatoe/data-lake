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
    print("Starting spark context")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .getOrCreate()
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
    print(f"Reading song data from {song_data}")
    song_data_df = spark.read \
        .option("recursiveFileLookup", "true") \
        .json(song_data)
    song_data_df = song_data_df.withColumn("year",
                                           f.when(f.col("year") == 0, None).otherwise(f.col(
                                               "year")))
    print("Caching song data")
    song_data_df.cache()

    create_song_table(output_data_path, song_data_df)

    create_artists_table(output_data_path, song_data_df)


def create_artists_table(output_data_path, song_data_df):
    """
    Create and save artists table
    :param output_data_path: output data location
    :param song_data_df: song data dataframe
    """
    print("Processing artists data")
    artists_table = song_data_df.select("artist_id", "artist_name", "artist_location",
                                        "artist_latitude",
                                        "artist_longitude")
    print("Deduping artists")
    artists_table.dropDuplicates(['artist_id', 'artist_name'])
    artists_table.createOrReplaceTempView("artists")

    print("Writing artists data")
    # write artists table to parquet files
    artists_table.write \
        .mode("overwrite") \
        .parquet(output_data_path + "/artists")


def create_song_table(output_data_path, song_data_df):
    """
    Crate and save song table
    :param output_data_path: output data location
    :param song_data_df: song data dataframe
    """
    # extract columns to create songs table
    print("Processing song data")
    songs_table = song_data_df.select('song_id', "title", "artist_id", "year", "duration")
    songs_table = songs_table.dropDuplicates(['song_id'])
    songs_table.createOrReplaceTempView("songs")
    # write songs table to parquet files partitioned by year and artist
    print("Writing song table to s3")
    songs_table.write \
        .partitionBy("year", "artist_id") \
        .mode("overwrite") \
        .parquet(output_data_path + "/songs")


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

    print("Reading log data file")
    # read log data file
    df = spark.read \
        .option("recursiveFileLookup", "true") \
        .schema(log_data_schema) \
        .json(log_data)
    df.printSchema()
    # filter by actions for song plays
    df = df.where(f.col("page") == "NextSong")

    print("Deduping events")

    # dedupe events
    df = df.dropDuplicates(['ts', 'userid', 'sessionid', 'song', 'artist'])
    print("Caching events")
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
    print("Creating songplays table")
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
    print("Writing songplays table to s3")
    # write songplays table to parquet files partitioned by year and month
    songplays_table \
        .write \
        .partitionBy("year", "month") \
        .mode('overwrite') \
        .json(output_data_path + '/songplays')


def create_time_table(log_df, output_data_path, start_time_column_name):
    """
    Creates and writes time table
    :param log_df: dataframe with log data
    :param output_data_path: output data location
    :param start_time_column_name:
    :return: dataframe with time data
    """
    print("Creating time table")
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
    print("Writing time table to s3")
    # write time table to parquet files partitioned by year and month
    time_table.write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(output_data_path + "/time")
    return time_table


def create_users_table(log_data_df, output_data_path):
    """
    Create and save users table
    :param log_data_df: log data dataframe
    :param output_data_path: output data location
    """
    print("Creating users table")
    # Adapted from https://sparkbyexamples.com/pyspark/pyspark-window-functions/
    users_df = log_data_df.withColumn("row_number", f.row_number().over(
        Window.partitionBy("userId").orderBy(f.desc("ts"))))
    deduped_users_df = users_df.where(users_df.row_number == 1)
    users_table = deduped_users_df.select("userId", "firstName", "lastName", "gender", "level")

    print("Writing users data to s3")
    # write users table to parquet files
    users_table \
        .write \
        .mode("overwrite") \
        .parquet(output_data_path + "/users")


def main():
    spark = create_spark_session()
    input_data_path = "s3a://udacity-dend"
    output_data_path = ""

    if not output_data_path:
        raise ValueError('output_data_path is not set')

    process_song_data(spark, input_data_path, output_data_path)
    process_log_data(spark, input_data_path, output_data_path)


if __name__ == "__main__":
    main()
