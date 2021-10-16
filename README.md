#Data lake
#Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

#TODO
To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.



# How to run a project

1) Go to $SPARK_HOME/jars
2) Download hadoop aws jar. You can choose hadoop aws version on mvn [central repo](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws)
```shell
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.pom
```
3) Download aws sdk version that coresponds to hadoop aws version. 
Then you selected hadoop aws version on the bottom of the page there is "Compile Dependencies". 
Click on aws sdk version, it will take to the aws sdk download page. For 2.7.3
```shell
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar
```
4)  