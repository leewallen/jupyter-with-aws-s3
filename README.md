# Accessing Data From S3

This example shows how to configure a JupyterLab docker image to access data from AWS S3.

## Build a Docker Image with AWS Related JARs

First, we need to build a docker image that includes the missing jars files needed for accessing S3. You can also add the jars using a volume mount, and then include code in your notebook to update the `PYSPARK_SUBMIT_ARGS` to include the jars from their location within the docker image. I felt like baking the jars into the docker image was a little easier that having to run a code cell to update the `PYSPARK_SUBMIT_ARGS`.

This example is using Spark 3.0.1 with Hadoop 3.2, and the files that we're adding are:

* aws-java-sdk-bundle-1.11.950.jar
* hadoop-aws-3.2.0.jar
* jets3t-0.9.4.jar

Here is an example Dockerfile to use:

```
FROM jupyter/pyspark-notebook:8ea7abc5b7bc

USER root

ENV PYSPARK_SUBMIT_ARGS '--packages com.amazonaws:aws-java-sdk:1.11.950,org.apache.hadoop:hadoop-aws:3.2.0,net.java.dev.jets3t:jets3t:0.9.4 pyspark-shell'


# Download missing jars

# Get AWS SDK JAR
RUN (cd /usr/local/spark/jars && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.950/aws-java-sdk-bundle-1.11.950.jar)

# Get Hadoop-AWS Jar
RUN (cd /usr/local/spark/jars && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar)

# Get jets3t JAR
RUN (cd /usr/local/spark/jars && curl -O https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar)

USER $NB_UID
```


## Run the Docker Container and Pass in AWS Credentials

This example is assuming that you have appropriate credentials saved in $HOME/.aws/credentials, and have jq installed.

Fetch temporary credentials from AWS and run the docker container with the credentials and session token passed in as environment variables:

```bash
creds_json=$(aws --profile default --region us-west-2 sts get-session-token)

docker run -d --name jupyter --rm -p 8888:8888 \
  -e AWS_ACCESS_KEY_ID=$(echo "$creds_json" | jq -r .Credentials.AccessKeyId) \
  -e AWS_SECRET_ACCESS_KEY=$(echo "$creds_json" | jq -r .Credentials.SecretAccessKey) \
  -e AWS_SESSION_TOKEN=$(echo "$creds_json" | jq -r .Credentials.SessionToken) \
  jupyter-docker:yourtag jupyter lab --LabApp.token ''
```


## Configure Spark


```python
from pyspark.sql import SparkSession
```


```python
spark = SparkSession.builder \
    .master("local") \
    .appName("Covid19TimeSeries") \
    .config("fs.s3a.path.style.access", True) \
    .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
    .config("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com") \
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("com.amazonaws.services.s3.enableV4", True) \
    .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
    .getOrCreate()
```

## Read Data From S3

At this point you should be able to read data in from S3.


```python
s3path = "s3a://dev-leewallen-spark/covid-19-time-series/parquet/covid-19.parquet"
parquetDF = spark.read.parquet(s3path)
```


```python
from pyspark.sql.functions import col 
```


```python
parquetDF.show(5)
```

    +---------+--------------+----------+------+--------------+---------+
    |Confirmed|Country/Region|      Date|Deaths|Province/State|Recovered|
    +---------+--------------+----------+------+--------------+---------+
    |        0|   Afghanistan|2020-01-22|     0|          null|        0|
    |        0|   Afghanistan|2020-01-23|     0|          null|        0|
    |        0|   Afghanistan|2020-01-24|     0|          null|        0|
    |        0|   Afghanistan|2020-01-25|     0|          null|        0|
    |        0|   Afghanistan|2020-01-26|     0|          null|        0|
    +---------+--------------+----------+------+--------------+---------+
    only showing top 5 rows
    



```python
parquetDF.filter((col('`Country/Region`') == "US") & (col('Confirmed') > 0)).show(5)
```

    +---------+--------------+----------+------+--------------+---------+
    |Confirmed|Country/Region|      Date|Deaths|Province/State|Recovered|
    +---------+--------------+----------+------+--------------+---------+
    |        1|            US|2020-01-22|     0|          null|        0|
    |        1|            US|2020-01-23|     0|          null|        0|
    |        2|            US|2020-01-24|     0|          null|        0|
    |        2|            US|2020-01-25|     0|          null|        0|
    |        5|            US|2020-01-26|     0|          null|        0|
    +---------+--------------+----------+------+--------------+---------+
    only showing top 5 rows
    

