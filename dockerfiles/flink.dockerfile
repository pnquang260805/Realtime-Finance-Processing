FROM flink:2.1.0-scala_2.12-java21

RUN mkdir -p /opt/flink/usrlib
RUN wget -P /opt/flink/usrlib https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/4.0.0-2.0/flink-connector-kafka-4.0.0-2.0.jar

USER root

RUN apt-get update && apt-get install -y python3 python3-pip
RUN ln -s /usr/bin/python3 /usr/bin/python
COPY ./requirements.txt .
RUN pip install -r ./requirements.txt


USER flink