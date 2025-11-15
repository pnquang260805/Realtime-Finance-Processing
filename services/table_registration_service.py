from pyflink.table import TableDescriptor, FormatDescriptor
from dataclasses import dataclass

from services.flink_service import FlinkService
from schema.kafka_schema import KafkaSchema

@dataclass()
class TableRegistrationService(FlinkService):
    kafka_broker: str

    def __post_init__(self):
        super().__post_init__()
        self.kafka_schema = KafkaSchema()
    def register_kafka_tables(self, input_topic: str, output_topic: str, source_table_name="src_temp", output_table_name="out_temp") -> None:
        self.t_env.create_temporary_table(source_table_name,
                                          TableDescriptor.for_connector(
                                              connector="kafka")
                                          .schema(self.kafka_schema.kafka_source_schema())
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
            .schema(self.kafka_schema.output_table_schema())
            .option("topic", output_topic)
            .option('properties.bootstrap.servers', self.kafka_broker)
            .format(FormatDescriptor.for_format('json')
                    .build())
            .build()
        )