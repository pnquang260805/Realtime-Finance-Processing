import os
import sys

from pyflink.table.udf import udf
from pyflink.table import DataTypes

from services.transform_finnhub import FinnhubTransform
from services.validate_and_deduplicate import Validate, Deduplication
from user_defined_functions.symbol_price_udf import SymbolPriceUdf
from user_defined_functions.symbol_volume import SymbolVolume

if __name__ == "__main__":
    broker = "kafka:29092"
    jars_base = "/opt/flink/usrlib"
    jars_name = ["flink-sql-connector-kafka-4.0.1-2.0", "kafka-clients-4.1.0"]
    jars_path = [os.path.join(jars_base, f"{jar}.jar") for jar in jars_name]

    input_topic = "raw-trade-topic"
    output_topic = "processed-trade-topic"
    src_table = "kafka_src"
    flatten_table = "kafka_output"

    transformer = FinnhubTransform(jars_path, broker)
    t_env = transformer.t_env
    t_env.create_temporary_function("push_symbol_price", udf(SymbolPriceUdf(), result_type=DataTypes.STRING()))
    t_env.create_temporary_function("push_symbol_volume", udf(SymbolVolume(), result_type=DataTypes.INT()))
    flattened_table = transformer.transform(
            input_topic, output_topic,
            src_table_name=src_table,
            out_table_name=flatten_table
        )
    t_env.create_temporary_view("flattened_data", flattened_table)
    # xử lí validate
    validator = Validate(t_env)
    validate_view = validator.get_validate_view("flattened_data")

    validator.validate_status(validate_view)

    # lọc ra các bản ghi hợp lệ
    filter_query = """
        SELECT
            symbol,
            price,
            volume,
            high,
            low,
            previous_close,
            price_change,
            change_percentage,
            trade_type,
            ts
        FROM '{validate_view}'
        WHERE validation_status = 'VALID'
    """
    valid_data = t_env.sql_query(filter_query)
    t_env.create_temporary_view("valid_data", valid_data)

    # xử lí deplicate
    deduplicator = Deduplication(t_env)
    deduplicate_view = deduplicator.get_deduplicate_view("valid_data", window_size_sec=60)
    # in ra status của phần xử lí trùng lặp
    deduplicator.dedup_status("valid_data", deduplicate_view)

    output_query = f"""
        SELECT 
            push_symbol_price(CAST(symbol AS STRING), price),
            price,
            push_symbol_volume(CAST(symbol AS STRING),volume),
            high,
            low,
            previous_close,
            price_change,
            change_percentage,
            trade_type,
            ts
        FROM `{deduplicate_view}`
    """
    
    final_data = t_env.sql_query(output_query)
    final_data.execute_insert(output_topic).wait()