from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes, FormatDescriptor
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Configuration

flink_kafka_connector_path = "file:///opt/flink/jobs/jars/flink-sql-connector-kafka-4.0.1-2.0.jar"
kafka_clients_path = "file:///opt/flink/jobs/jars/kafka-clients-4.1.0.jar"

config = Configuration()
config.set_string("python.execution-mode", "process")
# 1. Create stream env

settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(settings)

t_env.get_config().add_configuration(config)
t_env.execute_sql(f"ADD JAR '{flink_kafka_connector_path}'")
t_env.execute_sql(f"ADD JAR '{kafka_clients_path}'")

# 2. Create Kafka source
broker = "kafka:29092"
intopic = "raw-trade-topic"
output_topic = "processed-trade-topic"

kafka_src_name = "kafka_source"
kafka_dest_name = "kafka_processed"
t_env.create_temporary_table(
    kafka_src_name,
    TableDescriptor.for_connector(connector="kafka")
    .schema(Schema.new_builder()
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

    .option("topic", intopic)
    .option('properties.bootstrap.servers', 'kafka:29092')
    .option('properties.group.id', 'transaction_group')
    .option('scan.startup.mode', 'latest-offset')
    .format(FormatDescriptor.for_format('json')  # Gửi string thì flink tự parse
            .option('fail-on-missing-field', 'false')
            .option('ignore-parse-errors', 'true')
            .build())

    .build()
)

t_env.create_temporary_table(
    kafka_dest_name,
    TableDescriptor.for_connector("kafka")
    .schema(Schema.new_builder()
            .column("symbol", DataTypes.STRING())
            .column("price", DataTypes.FLOAT())
            .column("volume", DataTypes.FLOAT())
            .column("trade_type", DataTypes.STRING())
            .column("ts", DataTypes.FLOAT())
            .build())
    .option("topic", output_topic)
    .option('properties.bootstrap.servers', 'kafka:29092')
    .format(FormatDescriptor.for_format('json')
            .build())
    .build()
)

raw_data = t_env.from_path("kafka_source")
extracted_table_name = "stock_table"
# UNNEST(data) bung mảng thành các phần tử --> AS t(d) d là cột đại diện, t là tên bảng tạm mặc định --> CROSS JOIN: join vs các cột khác --> SELECT
# t(p, s, t, v) đặt alias cho từng phần trong ROW TYPE
extracted_table_query = f"""
    SELECT 
        s AS symbol,
        p AS price,
        v AS volume,
        `type` AS trade_type,
        t AS ts
    FROM `{kafka_src_name}`
    CROSS JOIN UNNEST(data) AS t(p, s, t, v)
"""
transformed_data = t_env.sql_query(
    extracted_table_query)  # Cái này trả về 1 TableResult
transformed_data.execute_insert(kafka_dest_name).wait()
