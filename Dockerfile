# File used to create the docker image:



FROM eclipse-temurin:11-jdk

ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl python3 python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python

# Download Spark from archive.apache.org 
RUN curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# Install Delta Lake JARs for Spark 3.5.1
RUN curl -L "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar" \
    -o ${SPARK_HOME}/jars/delta-spark_2.12-3.2.0.jar

RUN curl -L "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar" \
    -o ${SPARK_HOME}/jars/delta-storage-3.2.0.jar

# Install pyspark
RUN pip3 install --break-system-packages --no-cache-dir pyspark==${SPARK_VERSION}

WORKDIR /app
COPY ./src /app/src
COPY ./tests /app/tests
COPY requirements.txt pyproject.toml /app/

# install application dependencies and make the package available
RUN pip3 install --break-system-packages --no-cache-dir -r requirements.txt && \
    pip3 install --break-system-packages --no-cache-dir -e /app

# by default start an interactive pyspark shell; clients can override
# this with a different command when running the container
CMD ["pyspark", "--master", "local[*]"]
