FROM flink:2.0.0-scala_2.12-java17

RUN mkdir -p /opt/flink/jobs
RUN wget -P /opt/flink/usrlib https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.1.0/kafka-clients-4.1.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/2.0.0/flink-metrics-prometheus-2.0.0.jar

USER root

RUN apt-get update -y && \
    apt-get install python3 python3-pip -y
RUN ln -s /usr/bin/python3 /usr/bin/python
COPY ./requirements.txt .
RUN pip install -r ./requirements.txt --no-cache-dir

ENV PYTHONPATH=/opt/flink/jobs

USER flink