FROM apache/airflow:3.0.1

USER root

# Install OpenJDK 17 (needed for Spark)
RUN apt-get update && apt-get install -y openjdk-17-jdk curl && apt-get clean

# Set JAVA_HOME and PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Spark 3.5.1 (compatible with your master/worker)
ENV HADOOP_VERSION=3
RUN curl -fsSL https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-3.5.5-bin-hadoop${HADOOP_VERSION} /opt/spark

# Set Spark env
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow

# Install Airflow with Spark provider and Great Expectations
ARG AIRFLOW_VERSION=3.0.1
RUN pip install apache-airflow[apache-spark]==${AIRFLOW_VERSION} great_expectations
