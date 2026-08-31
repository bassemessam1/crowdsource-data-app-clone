FROM apache/spark:3.5.1-python3
USER root

# Install python dependencies
RUN pip install --no-cache-dir \
        h3==3.7.7 \
        google-cloud-bigquery==3.13.0 \
        google-cloud-storage==2.16.0 \
        avro-python3==1.10.2

RUN curl -fL \ 
        "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.1/spark-avro_2.12-3.5.1.jar" \ 
        -o /opt/spark/jars/spark-avro_2.12-3.5.1.jar 

        # Download GCS Hadoop connector # This provides com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem 
RUN curl -fL \ 
        "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.22/gcs-connector-hadoop3-2.2.22-shaded.jar" \ 
        -o /opt/spark/jars/gcs-connector-hadoop3-2.2.22-shaded.jar

RUN curl -fL \ 
    "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.34.0/spark-bigquery-with-dependencies_2.12-0.34.0.jar" \ 
    -o /opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.34.0.jar

COPY jobs/ /opt/spark/jobs/

USER spark
