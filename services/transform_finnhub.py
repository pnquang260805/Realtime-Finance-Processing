from dataclasses import dataclass

from pyflink.table import (TableResult, Table)
from pyflink.table.expressions import *
from pyflink.table.udf import udf
from pyflink.table.types import FloatType

from services.flink_service import FlinkService
from utils.redis_lookup import RedisLookup
from services.table_registration_service import TableRegistrationService


@udf(result_type=FloatType())
def redis_lookup(symbol: str, field: str) -> float:
    return float(RedisLookup().get_value(symbol, field) or 0.0)


@dataclass
class FinnhubTransform(TableRegistrationService):
    def __post_init__(self):
        super().__post_init__()
        self.lookup = RedisLookup()

    def flatten_data(self, src_table_name: str) -> Table:
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
        return self.t_env.sql_query(extracted_query)

    def handle_null(self, input_table : Table) -> Table:
        """
        Xử lý null.
        symbol, price, volume mà null thì không có giá trị --> bỏ luôn
        trade_type có thể điền lại null

        :param input_table: Bảng của công đoạn trước
        :return: Deduplicated and dropped null table
        """
        temp_view_name = "having_null_view"
        self.t_env.create_temporary_view(temp_view_name, input_table)

        unknow_value = "UNKNOW"
        handle_null_query = f"""
            SELECT 
                symbol, 
                price, 
                volume, 
                COALESCE(trade_type, '{unknow_value}') AS trade_type,
                ts
            FROM `{temp_view_name}`
            WHERE symbol IS NOT NULL AND price IS NOT NULL AND volume IS NOT NULL
        """
        handled_null_table = self.t_env.sql_query(handle_null_query)
        return handled_null_table

    def deduplicate_data(self, input_table: Table) -> Table:
        """
        Tương đương với:
            SELECT DISTINCT * FROM ...

        :param input_table:
        :return: Bảng đã bỏ trùng lặp
        """
        before_deduplicate_name = "before_deduplicate"
        self.t_env.create_temporary_view(before_deduplicate_name, input_table)
        deduplicate_query = f"""
            SELECT symbol, price, volume, trade_type, ts
            FROM (
                SELECT *, 
                    ROW_NUMBER() OVER (PARTITION BY symbol, price, volume, trade_type ORDER BY ts) AS rn
                FROM `{before_deduplicate_name}`
            ) AS temp
            WHERE rn = 1
        """
        return self.t_env.sql_query(deduplicate_query)


    def enrich_data(self, input_table : Table) -> Table:
        temp_enrich_table = (input_table
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

    def transform(self, input_topic: str, output_topic: str, src_table_name: str, out_table_name: str) -> TableResult:
        """
        Là pipeline tổng hợp dữ liệu chính
        Luồng hoạt động:
            Raw data (kafka) --> Flat data --> Deduplicate (func: deduplicate_data) --> Handle NULL (func: handle_null) --> Enrich data (func: enrich_data) --> Druid (kafka)

        :param input_topic: Topic đầu vào
        :param output_topic: Topic đầu ra (topic ghi dữ liệu)
        :param src_table_name:
        :param out_table_name:
        :return: TableResult
        """
        flatten_table = self.flatten_data(src_table_name)
        deduplicate_table = self.deduplicate_data(flatten_table)
        null_handled_table = self.handle_null(deduplicate_table)
        enriched_table = self.enrich_data(null_handled_table)
        return enriched_table.execute_insert(out_table_name)
