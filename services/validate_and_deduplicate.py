from pyflink.table import TableEnvironment, TableDescriptor, Schema, DataTypes, FormatDescriptor
from pyflink.table.udf import udf
from datetime import datetime   

@udf(result_type=DataTypes.BOOLEAN())
def is_valid_symbol(symbol: str) -> bool:
    if symbol == "" or symbol is None:
        return False   
    return symbol.isalpha() and symbol.isupper()

@udf(result_type=DataTypes.BOOLEAN())
def is_valid_price(price: float) -> bool:
    if price <= 0 or price is None:
        return False
    return 0.01 <= price <= 1000000

@udf(result_type=DataTypes.BOOLEAN())
def is_valid_volume(volume: float) -> bool:
    if volume <= 0 or volume is None:
        return False
    return volume >= 1

@udf(result_type=DataTypes.BOOLEAN())
def is_valid_timestamp(ts: float) -> bool:
    if ts is None or ts <= 0:
        return False
    current_ts = int(datetime.now().timestamp() * 1000)  # Convert to milliseconds
    return abs(current_ts - ts) <= 300000

@udf(result_type=DataTypes.BOOLEAN()) 
def is_validate_schema(data: dict) -> bool:
    required_field = {
        "symbol", "price", "volume", "high", "low",
        "previous_change", "price_change", "change_percentage",
        "trade_type", "ts"
    }
    return required_field.issubset(data.keys())

class Validate():
    def __init__(self, t_env: TableEnvironment):
        self.t_env = t_env

    def get_validate_view(self,table_name: str) -> str:
        # Tạo 1 view tên là validate để có thể kiểm tra xem các bản ghi hợp lệ
        validate_table = f"""
            SELECT *,
            CASE
                WHEN NOT is_valid_symbol(symbol) THEN 'INVALID_SYMBOL'
                WHEN NOT is_valid_price(price) THEN 'INVALID_PRICE'
                WHEN NOT is_valid_volume(volume) THEN 'INVALID_VOLUME'
                WHEN NOT is_valid_timestamp(ts) THEN 'INVALID_TIMESTAMP'
                WHEN NOT is_validate_schema(STRUCT(*)) THEN 'INVALID_SCHEMA'
                ELSE 'VALID'
            END as validation_status
            FROM {table_name}
        """
        validated = self.t_env.sql_query(validate_table)
        self.t_env.create_temporary_view("validated_data", validated)
    
    def validate_status(self, table_name: str) -> None:
        status_query = f"""
            SELECT 
                COUNT(*) as total_records,
                SUM(CASE WHEN validation_status = 'VALID' THEN 1 ELSE 0 END) as valid_count,
                SUM(CASE WHEN validation_status != 'VALID' THEN 1 ELSE 0 END) as invalid_count,
                CAST(
                    SUM(CASE WHEN validation_status = 'VALID' THEN 1 ELSE 0 END) * 100.0 
                    / COUNT(*) 
                    AS DECIMAL(5,2)
                ) as valid_percentage
            FROM {table_name}
        """
        print("=" * 60)
        print("Thống kê dữ liệu được validate")
        print("=" * 60)
        self.t_env.sql_query(status_query).execute().print()

class Deduplication():
    def __init__(self, t_env: TableEnvironment):
        self.t_env = t_env
    """
    Tạo watermark để dùng cho Tumbling Window
    Watermark được sử dụng để xác định thời điểm một cửa sổ được kích hoạt
    Input: table_name (tên bảng)
    Output: Tên bảng/view sau khi setup watermark    
    # Đổi từ mili giây sang giây để tính event time
    """
    def setup_watermark(self, table_name: str, ts_column: str = "ts") -> str:
        watermark_view = f"""
            SELECT *,
                CAST({ts_column} / 1000 AS BIGINT) as ts_sec,
                TO_TIMESTAMP(FROM_UNIXTIME(CAST({ts_column} / 1000 AS BIGINT))) AS event_time
            FROM {table_name}
            WHERE validation_status = 'VALID'
        """
        watermarked = self.t_env.sql_query(watermark_view)
        self.t_env.create_temporary_view("temp_watermarked", watermarked)
        return "temp_watermarked"
    
    def get_deduplicate_view(self, table_name: str, window_size_sec: int = 60) -> str:
        """
        setup_tumbling_window
        Kết hợp Watermark và Tumbling Window)
        Loại bỏ duplicate trong mỗi Tumbling Window
        Input: 
        table_name (tên bảng đã validate)
        window_size_sec (kích thước window, mặc định 60 giây)
        Output: Bảng dedup theo window
        """
        watermark_view = self.setup_watermark(table_name)

        dedup_view = f"""
            SELECT *, 
            WINDOW_START, WINDOW_END,
            ROW_NUMBER() OVER(
                PARTITION BY
                WINDOW_START,
                WINDOW_END,
                symbol, price, volume
                ORDER BY ts ASC
                ) AS row_num
            FROM {watermark_view}
            GROUP BY 
                WINDOW_START,
                WINDOW_END,
                symbol,
                price,
                volume,
                high,
                low,
                previous_close,
                price_change,
                change_percentage,
                trade_type,
                ts,
                validation_status,
                TUMBLE(event_time, INTERVAL '{window_size_sec}' SECOND)
        """
        window_dedup_view = self.t_env.sql_query(dedup_view)
        self.t_env.create_temporary_view("temp_window_dedup", window_dedup_view)

        final_query = """
            SELECT *,
                WINDOW_START,
                WINDOW_END
            FROM temp_window_dedup
            WHERE row_num = 1
        """
        final = self.t_env.sql_query(final_query)
        self.t_env.create_temporary_view("deduplicated_data", final)
    
    def dedup_status(self, before_view: str, after_view: str) -> None:
        """
        In ra thống kê deduplication
        
        Input: 
            before_view (view trước dedup)
            after_view (view sau dedup)
        """
        
        stats_query = f"""
            SELECT 
                (SELECT COUNT(*) FROM `{before_view}`) as before_count,
                (SELECT COUNT(*) FROM `{after_view}`) as after_count,
                ((SELECT COUNT(*) FROM `{before_view}`) - 
                 (SELECT COUNT(*) FROM `{after_view}`)) as duplicates_removed,
                CAST(
                    ((SELECT COUNT(*) FROM `{before_view}`) - 
                     (SELECT COUNT(*) FROM `{after_view}`)) * 100.0 /
                    (SELECT COUNT(*) FROM `{before_view}`)
                    AS DECIMAL(5,2)
                ) as duplicate_percentage
        """
        print("=" * 60)
        print("Thống kê dữ liệu được loại bỏ trùng")
        print("=" * 60)
        self.t_env.sql_query(stats_query).execute().print()