import json
import traceback
from typing import Dict

try:
    from kafka import KafkaProducer
except ImportError:
    # 如果导入失败，提示用户安装 kafka-python 包
    print("错误：未找到 'kafka' 模块。")
    print("请运行以下命令安装所需的依赖：")
    print("pip install kafka-python==2.0.2")
    import sys
    sys.exit(1)


class KafkaService:

    def __init__(self, config):
        bootstrap_servers = config["bootstrap_servers"].split(",")
        print(f"kafka config: {config}")
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: json.dumps(k).encode(),
            value_serializer=lambda v: json.dumps(v).encode(),
            api_version=tuple(config.get("api_version")) if config.get("api_version") else (2, 7, 0),
            acks=config.get("acks", "all"),
            compression_type=config.get("compression_type", "gzip"),
            retries=config.get("retries", 10),
            batch_size=config.get("batch_size", 163840),
            linger_ms=config.get("linger_ms", 1),
            max_block_ms=config.get("max_block_ms", 2000),
            buffer_memory=config.get("buffer_memory", 335544320),
            request_timeout_ms=config.get("request_timeout_ms", 600000),
            security_protocol=config.get("security_protocol", "SASL_SSL"),
            sasl_mechanism=config.get("sasl_mechanism", "SCRAM-SHA-512"),
            sasl_plain_username=config["username"],
            sasl_plain_password=config["password"],
            ssl_cafile=config.get("ssl_cafile")
        )
        self.topic = config["topic"]

    def send(
        self,
        message: Dict,
        source: str,
        key: str = None,
        topic: str = None
    ):
        if topic is None:
            topic = self.topic
        message["source"] = source
        for _ in range(3):
            try:
                future = self.producer.send(
                    topic,
                    value=message,
                    key=key
                )
                future.get(timeout=1)
                return True
            except Exception as e:
                print(traceback.format_exc())
        return False


class KafkaServiceFactory:

    @staticmethod
    def create_kafka_service(
        config_path: str, cluster_name: str, env: str, ssl_cafile: str = None
    ):
        with open(config_path, 'r') as f:
            config = json.load(f)
        kafka_config = config[cluster_name][env]
        if ssl_cafile:
            kafka_config["ssl_cafile"] = ssl_cafile
        return KafkaService(kafka_config)