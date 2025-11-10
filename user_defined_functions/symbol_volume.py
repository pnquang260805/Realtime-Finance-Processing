from prometheus_client import Gauge
from pyflink.table import FunctionContext
from pyflink.table.udf import udf, ScalarFunction

from utils.pushgateway import Pushgateway

class SymbolVolume(ScalarFunction):
    def __init__(self):
        self.gauge = None
        self.pushgateway = None

    def open(self, function_context: FunctionContext):
        self.pushgateway = Pushgateway()
        registry = self.pushgateway.get_registry()
        self.gauge = Gauge("symbol_volume", "current symbol price", ["symbol"], registry)

    def eval(self, symbol : str, volume : int):
        self.gauge.labels(symbol=symbol).set(volume)
        self.pushgateway.push_metric()
        return volume
