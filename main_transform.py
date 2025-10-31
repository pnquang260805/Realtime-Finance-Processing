import os
import sys


from services.transform_finnhub import FinnhubTransform

if __name__ == "__main__":
    broker = "kafka:29092"
    jars_base = "/opt/flink/usrlib"
    jars_name = ["flink-sql-connector-kafka-4.0.1-2.0", "kafka-clients-4.1.0"]
    jars_path = [os.path.join(jars_base, f"{jar}.jar") for jar in jars_name]

    input_topic = "raw-trade-topic"
    output_topic = "processed-trade-topic"
    src_table = "kafka_src"
    out_table = "trade_cleansed"

    transformer = FinnhubTransform(jars_path, broker)

    transformer.transform(input_topic, output_topic,
                          src_table_name=src_table, out_table_name=out_table)
