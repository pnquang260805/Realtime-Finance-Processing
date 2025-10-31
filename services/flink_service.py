from dataclasses import dataclass

from typing import List

from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes, FormatDescriptor
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Configuration
from pyflink.table import expressions as exprs


@dataclass
class FlinkService:
    jars_path: List[str]

    def __post_init__(self):
        config = Configuration()
        config.set_string("python.execution-mode", "process")
        settings = EnvironmentSettings.in_streaming_mode()

        self.t_env = TableEnvironment.create(settings)
        self.t_env.get_config().add_configuration(config)
        for jar_path in self.jars_path:
            self.t_env.execute_sql(f"ADD JAR '{jar_path}'")

    def get_table_env(self):
        return self.t_env
