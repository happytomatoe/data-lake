# Data lake

# Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data
warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as
a directory with JSON metadata on the songs in their app.


### Fact Table
1) Songplays - records in event data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
2) Users - users in the app
- user_id, first_name, last_name, gender, level.

3) Songs - songs in music database
- song_id, title, artist_id, year, duration

4) Artists - artists in music database
- artist_id, name, location, lattitude, longitude

5) Time - timestamps of records in songplays broken down into specific units
- fields: start_time, hour, day, week, month, year, weekday
- sortkey based on start_time to speed up joins

# How to run a project


1) Go to `$SPARK_HOME/jars`

```shell
cd $SPARK_HOME/jars
```

2) Download hadoop aws jar. You can choose hadoop aws version on
   mvn [central repo](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws)

```shell
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar
```

Note: hadoop-aws version should be compatible with hadoop-common.jar in the $SPARK_HOME/jars folder

3) Download aws sdk version that coresponds to hadoop aws version. Then you selected hadoop aws version on the bottom of
   the page there is "Compile Dependencies". Click on aws sdk version, it will take to the aws sdk download page. 
Next command downloads aws sdk jar for hadoop aws 2.7.3

```shell
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar
```

4) Set `aws access key` and `aws secret key` in `dl.cfg`
5) Change `output_data_path` variable in `etl.py` main function
6) Run

```shell
spark-submit etl.py
```