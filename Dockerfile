# File used to create the docker image:

# docker buildx build --load -t sparkengineer_swissre_pablosanchez .
# docker run -it -v ${PWD}/src:/app/src sparkengineer_swissre_pablosanchez

FROM eclipse-temurin:11-jdk

ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl python3 python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Download Spark from archive.apache.org 
RUN curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# Install pyspark
RUN pip3 install --break-system-packages --no-cache-dir pyspark==${SPARK_VERSION}

WORKDIR /app
COPY ./src /app/src

CMD ["pyspark", "--master", "local[*]"]
