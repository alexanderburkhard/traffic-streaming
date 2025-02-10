FROM openjdk:11-jre-slim

# Install Python and pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Install PySpark and the Spark-Kafka integration package
RUN pip3 install pyspark[kafka]

# Set the working directory in the container
WORKDIR /app


# Copy the current directory contents into the container at /app
COPY . /app

# Copy the log4j.properties file to the Spark configuration directory
COPY log4j.properties /opt/spark/conf/

# Run the Spark application
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "--master", "local[*]", "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties", "/app/spark-streaming-app.py"]
