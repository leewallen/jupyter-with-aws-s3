FROM jupyter/pyspark-notebook:8ea7abc5b7bc
# FROM jupyter/pyspark-notebook:8ea7abc5b7bc


USER root
ENV PYSPARK_SUBMIT_ARGS '--packages com.amazonaws:aws-java-sdk:1.11.950,org.apache.hadoop:hadoop-aws:3.2.0,net.java.dev.jets3t:jets3t:0.9.4 pyspark-shell'

RUN mkdir /var/lib/apt/lists/partial
# pre-requisites
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3-boto3 \
    vim \
    curl

# Download missing jars
# Get AWS SDK JAR
RUN (cd /usr/local/spark/jars && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.950/aws-java-sdk-bundle-1.11.950.jar)
# Get Hadoop-AWS Jar
RUN (cd /usr/local/spark/jars && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar)
# Get jets3t JAR
RUN (cd /usr/local/spark/jars && curl -O https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar)

USER $NB_UID

RUN pip3 install boto3
RUN pip3 install botocore
RUN pip3 install awscli
RUN pip3 install chart_studio
RUN pip3 install pandas
RUN pip3 install requests


# ENTRYPOINT ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--LabApp.token=''"]
