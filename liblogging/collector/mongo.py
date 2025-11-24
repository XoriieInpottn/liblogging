#!/usr/bin/env python3

__author__ = "xi"

import json
import sys
from datetime import datetime
from queue import Empty
from queue import Queue
from threading import Thread
from time import time
from typing import Any

from libdata import LazyMongoClient
from libdata.url import URL
from libentry import ArgumentParser
from pydantic import BaseModel
from pydantic import Field

from ..util import split_trace_id


class CollectorConfig(BaseModel):
    mongo_url: str | URL
    max_queue_size: int = Field(default=100_000, ge=1, le=100_000_000)
    batch_size: int = Field(default=100, ge=1, le=10000)
    wait_time: float = 0.2
    max_wait_time: float = 0.5

    def model_post_init(self, context: Any, /) -> None:
        self.mongo_url = URL.ensure_url(self.mongo_url)
        db, coll = self.mongo_url.get_database_and_table()
        if db is None or coll is None:
            raise ValueError("Both database and collection name should be given in the URL.")


def sending_loop(queue: Queue[dict], config: CollectorConfig):
    client = LazyMongoClient(config.mongo_url)

    batch_size = config.batch_size
    wait_time = config.wait_time
    max_wait_time = config.max_wait_time

    while True:
        buffer = [queue.get()]

        t = time()
        for _ in range(batch_size):
            try:
                buffer.append(queue.get(timeout=wait_time))
            except Empty:
                break
            if time() - t > max_wait_time:
                break

        if buffer[-1] is not None:
            client.insert_many(buffer)
        else:
            client.insert_many(buffer[:-1])
            break


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
    create_date = datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d")
    message.update({"create_date": create_date})
    return trace_id, message


def main():
    parser = ArgumentParser()
    parser.add_schema("config", CollectorConfig)
    config: CollectorConfig = parser.parse_args().config

    queue = Queue(config.max_queue_size)

    sending_thread = Thread(
        target=sending_loop,
        kwargs=dict(queue=queue, config=config)
    )
    sending_thread.start()

    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                break
            line = line.strip()
            try:
                trace_id, message = process_message(line)
                queue.put(message)
                print(message)
            except ValueError:
                print(line)
        except EOFError:
            print("End of input reached. Exiting...", file=sys.stderr)
            break
        except Exception as e:
            print(f"Unexpected error: {e}", file=sys.stderr)
            continue

    queue.put(None)
    sending_thread.join()
    print("Mongo collector exited.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
