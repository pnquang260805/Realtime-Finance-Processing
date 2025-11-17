from pyflink.table import Schema, DataTypes

class KafkaSchema:
    def kafka_source_schema(self) -> Schema:
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

    def output_table_schema(self) -> Schema:
        return (
            Schema.new_builder()
            .column("symbol", DataTypes.STRING().not_null())
            .column("price", DataTypes.FLOAT())
            .column("volume", DataTypes.FLOAT())
            .column("high", DataTypes.FLOAT())
            .column("low", DataTypes.FLOAT())
            .column("previous_close", DataTypes.FLOAT())
            .column("price_change", DataTypes.FLOAT())
            .column("change_percentage", DataTypes.FLOAT())
            .column("trade_type", DataTypes.STRING())
            .column("ts", DataTypes.FLOAT())
            .primary_key("symbol")
            .build()
        )