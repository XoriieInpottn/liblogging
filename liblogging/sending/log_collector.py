#!/usr/bin/env python3

__author__ = "yubin"


import argparse
import os
import sys
import json
from datetime import datetime
import time

from .kafka_service import KafkaServiceFactory
from ..util import split_trace_id, thread_pool_manager


def get_date_from_time(
    time, time_format: str = "%Y-%m-%d %H:%M:%S.%f", date_format: str = "%Y-%m-%d"
) -> str:
    date_string = datetime.strptime(time, time_format).strftime(date_format)
    return date_string


def process_message(line):
    message = json.loads(line)
    trace_id = message.get("trace_id", "")
    if trace_id:
        _, chat_dict = split_trace_id(trace_id)
        message.update({
            "uid": chat_dict.get("uid", ""),
            "session_id": chat_dict.get("session_id", ""),
            "turn": chat_dict.get("turn", 0)
        })
    create_time = message.get("create_time")
    message.update({"create_date": get_date_from_time(create_time)})
    return trace_id, message


class LogCollector:

    def __init__(self, args):
        start_time = time.time()
        self.kafka_service = KafkaServiceFactory.create_kafka_service(
                config_path=args.config_path,
                cluster_name=args.cluster_name,
                env=args.env,
                ssl_cafile=args.ssl_cafile
            )
        print(f"Init kafka service time: {time.time() - start_time}")

    def collect(
        self, send_kafka: bool = False, chat_env: str = "dev", use_default_process: bool = True
    ):
        start_time = time.time()
        while True:
            try:
                line = sys.stdin.readline().strip()
                if not line:
                    continue
                print(line)
                print(round(time.time() - start_time, 3))
                start_time = time.time()
                try:
                    if use_default_process:
                        trace_id, message = process_message(line)
                    else:
                        message = json.loads(line)
                        trace_id = message.get("trace_id", "")

                    if send_kafka and trace_id:
                        print(f"Sending kafka message. env:{chat_env}")
                        thread_pool_manager.submit(
                            self.kafka_service.send,
                            message={
                                "log_history": json.dumps(message),
                                "current_env": chat_env
                            },
                            key=trace_id,
                            source=message.get("message_source")
                        )
                except ValueError as e:
                    pass

            except EOFError:
                print("End of input reached. Exiting...", file=sys.stderr)
                break
            except Exception as e:
                print(f"Unexpected error: {e}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--send-kafka", action='store_true', default=False)
    parser.add_argument("--config-path", type=str, required=True, help="Path to Kafka config json file")
    parser.add_argument("--cluster-name", type=str, default="cluster_2", help="Kafka cluster name")
    parser.add_argument(
        "--env", type=str, default=os.environ.get("CHAT_ENV", "dev"), help="Environment (e.g., dev, test, online)"
    )
    parser.add_argument("--ssl-cafile", type=str, required=True, help="ssl_cafile path")
    parser.add_argument(
        "--use-default-process",
        type=bool,
        default=True,
        help="whether user default process. you can also use another function to process message by redirecting")

    args = parser.parse_args()

    log_collector = LogCollector(args)
    log_collector.collect(
        send_kafka=args.send_kafka, chat_env=args.env, use_default_process=args.use_default_process
    )