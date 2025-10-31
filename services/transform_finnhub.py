from dataclasses import dataclass

from pyflink.table import (TableDescriptor, Schema,
                           DataTypes, FormatDescriptor)
from pyflink.table.expressions import *
from pyflink.table.udf import udf
from pyflink.table.types import FloatType

from services.flink_service import FlinkService
from utils.redis_lookup import RedisLookup


@udf(result_type=FloatType())
def redis_lookup(symbol: str, field: str) -> float:
    return float(RedisLookup().get_value(symbol, field) or 0.0)


@dataclass
class FinnhubTransform(FlinkService):
    kafka_broker: str

    def __post_init__(self):
        super().__post_init__()
        self.lookup = RedisLookup()

    def __kafka_source_schema(self) -> Schema:
        return (Schema.new_builder()
                .column("data", DataTypes.ARRAY(DataTypes.ROW([
                    DataTypes.FIELD(
                        "p", DataTypes.FLOAT()),
                    DataTypes.FIELD(
                        "s", DataTypes.STRING()),
                    DataTypes.FIELD(
                        "t", DataTypes.FLOAT()),
                    DataTypes.FIELD(
                        "v", DataTypes.FLOAT()),
                ])))
                .column("type", DataTypes.STRING())
                .build())

    def __output_table_schema(self) -> Schema:
        return (
            Schema.new_builder()
            .column("symbol", DataTypes.STRING())
            .column("price", DataTypes.FLOAT())
            .column("volume", DataTypes.FLOAT())
            .column("high", DataTypes.FLOAT())
            .column("low", DataTypes.FLOAT())
            .column("previous_close", DataTypes.FLOAT())
            .column("price_change", DataTypes.FLOAT())
            .column("change_percentage", DataTypes.FLOAT())
            .column("trade_type", DataTypes.STRING())
            .column("ts", DataTypes.FLOAT())
            .build()
        )

    def __register_kafka_tables(self, input_topic: str, output_topic: str, source_table_name="src_temp", output_table_name="out_temp") -> None:
        self.t_env.create_temporary_table(source_table_name,
                                          TableDescriptor.for_connector(
                                              connector="kafka")
                                          .schema(self.__kafka_source_schema())
                                          .option("topic", input_topic)
                                          .option('properties.bootstrap.servers', self.kafka_broker)
                                          .option('properties.group.id', 'transaction_group')
                                          .option('scan.startup.mode', 'latest-offset')
                                          #   .option('scan.startup.mode', 'earliest-offset')
                                          .format(FormatDescriptor.for_format('json')  # Gửi string thì flink tự parse
                                                  .option('fail-on-missing-field', 'false')
                                                  .option('ignore-parse-errors', 'true')
                                                  .build())

                                          .build())
        self.t_env.create_temporary_table(
            output_table_name,
            TableDescriptor.for_connector("kafka")
            .schema(self.__output_table_schema())
            .option("topic", output_topic)
            .option('properties.bootstrap.servers', self.kafka_broker)
            .format(FormatDescriptor.for_format('json')
                    .build())
            .build()
        )

    def __build_transformed_table(self, src_table_name: str):
        raw = self.t_env.from_path(src_table_name)

        extracted_query = f"""
            SELECT 
                s AS symbol,
                p AS price,
                v AS volume,
                `type` AS trade_type,
                t AS ts
            FROM `{src_table_name}`
            CROSS JOIN UNNEST(data) AS t(p, s, t, v)
        """
        extracted_table = self.t_env.sql_query(extracted_query)

        temp_enrich_table = (extracted_table
                             .add_or_replace_columns(redis_lookup(col("symbol"), lit("high")).alias("high"))
                             .add_or_replace_columns(redis_lookup(col("symbol"), lit("low")).alias("low"))
                             .add_or_replace_columns(redis_lookup(col("symbol"), lit("pc")).alias("previous_close")))

        self.t_env.create_temporary_view(
            "enriched_temp_table", temp_enrich_table)
        enriched_query = """
            SELECT 
                *, 
                (price - previous_close) AS price_change, 
                ((price - previous_close) / previous_close) * 100 AS change_percentage
            FROM enriched_temp_table
        """
        enriched_table = self.t_env.sql_query(enriched_query)
        final_table = enriched_table.select(
            col("symbol"),
            col("price"),
            col("volume"),
            col("high"),
            col("low"),
            col("previous_close"),
            col("price_change"),
            col("change_percentage"),
            col("trade_type"),
            col("ts")
        )
        return final_table

    def transform(self, input_topic: str, output_topic: str,
                  src_table_name: str,
                  out_table_name: str):
        self.__register_kafka_tables(
            input_topic, output_topic, src_table_name, out_table_name)
        final_table = self.__build_transformed_table(src_table_name)
        final_table.execute_insert(out_table_name).wait()
