FROM flink:1.15.4

# install python3: it has updated Python to 3.9 in Debian 11 and so install Python 3.7 from source
# it currently only supports Python 3.6, 3.7 and 3.8 in PyFlink officially.

RUN apt-get update -y && \
apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev && \
wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz && \
tar -xvf Python-3.7.9.tgz && \
cd Python-3.7.9 && \
./configure --without-tests --enable-shared && \
make -j6 && \
make install && \
ldconfig /usr/local/lib && \
cd .. && rm -f Python-3.7.9.tgz && rm -rf Python-3.7.9 && \
ln -s /usr/local/bin/python3 /usr/local/bin/python && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

# install PyFlink and dependencies
RUN pip3 install apache-flink==1.15.4

# Install PostgreSQL client library, environment variable loader, and Kafka producer
RUN pip3 install psycopg2-binary==2.9.9 python-dotenv==0.21.1 kafka-python==2.0.2

# Add Flink jobs directory to Python path so modules like db_utils are importable
ENV PYTHONPATH="/opt/flink/jobs"

# Install required system dependencies for connectors
RUN apt-get update -y && \
    apt-get install -y openjdk-11-jdk wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download Flink Kafka connector JAR
RUN mkdir -p /opt/flink/lib && \
    wget -q https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.15.4/flink-sql-connector-kafka-1.15.4.jar \
    -O /opt/flink/lib/flink-sql-connector-kafka-1.15.4.jar

# Create directories for Flink state backend
RUN mkdir -p /var/flink/checkpoints /var/flink/savepoints /var/flink/rocksdb && \
    chmod -R 777 /var/flink
