import os
import sys

from pyflink.table.udf import udf
from pyflink.table import DataTypes

from services.transform_finnhub import FinnhubTransform
from user_defined_functions.symbol_price_udf import SymbolPriceUdf
from user_defined_functions.symbol_volume import SymbolVolume

if __name__ == "__main__":
    broker = "kafka:29092"
    jars_base = "/opt/flink/usrlib"
    jars_name = ["flink-sql-connector-kafka-4.0.1-2.0", "kafka-clients-4.1.0", "flink-metrics-prometheus-2.0.0"]
    jars_path = [os.path.join(jars_base, f"{jar}.jar") for jar in jars_name]

    input_topic = "raw-trade-topic"
    output_topic = "processed-trade-topic"
    src_table = "kafka_src"
    flatten_table = "kafka_output"

    # Setup pipeline
    transformer = FinnhubTransform(jars_path, broker)
    t_env = transformer.t_env
    t_env.create_temporary_function("push_symbol_price", udf(SymbolPriceUdf(), result_type=DataTypes.STRING()))
    t_env.create_temporary_function("push_symbol_volume", udf(SymbolVolume(), result_type=DataTypes.INT()))

    # Registrate kafka tables
    transformer.register_kafka_tables(input_topic, output_topic, src_table, flatten_table)

    # Main transform
    flattened_table = transformer.transform(
            input_topic, output_topic,
            src_table_name=src_table,
            out_table_name=flatten_table
        ).wait()