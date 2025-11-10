from prometheus_client import push_to_gateway, CollectorRegistry

class Pushgateway:
    def __init__(self, prometheus_host : str = "pushgateway", prometheus_port : int = 9091):
        self.registry = CollectorRegistry()
        self.gateway = f"{prometheus_host}:{prometheus_port}"

    def get_registry(self):
        return self.registry

    def push_metric(self, job: str = "default_job"):
        push_to_gateway(self.gateway, job=job, registry=self.registry)