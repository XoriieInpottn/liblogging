#!/usr/bin/env python3

__author__ = "xi"
__all__ = [
    "get_log_context",
    "log_request",
    "Logger",
    "logger",
]

import functools
import inspect
import json
import logging
import os
import sys
import threading
from contextvars import ContextVar
from datetime import datetime
from logging import Formatter, Handler, NOTSET
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Mapping, Optional

try:
    from tqdm import tqdm


    class TqdmHandler(Handler):

        def __init__(self, stream, level=NOTSET):
            super().__init__(level)
            self.stream = stream

        def emit(self, record):
            # noinspection PyBroadException
            try:
                msg = self.format(record)
                tqdm.write(msg, file=self.stream)
                self.flush()
            except RecursionError:
                raise
            except Exception:
                self.handleError(record)


    StreamHandler = TqdmHandler
except ImportError:
    tqdm = None
    from logging import StreamHandler


class ThreadCoroutineLocal(threading.local):

    def __init__(self):
        super().__init__()
        self._co_local = ContextVar[Optional[Dict]]("_co_local", default=None)

    @property
    def co_local(self):
        co_local = self._co_local.get()
        if co_local is None:
            co_local = {}
            self._co_local.set(co_local)
        return co_local


thread_local = ThreadCoroutineLocal()


def get_log_context():
    return thread_local.co_local


def log_request(fields=("trace_id", "request_id")):
    def decorator(fn):
        if not inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            def _wrapper(*args, **kwargs):
                log_context = thread_local.co_local
                log_context.clear()
                for field, value in _find_log_items(fields, args, kwargs):
                    log_context[field] = value
                return fn(*args, **kwargs)
        else:
            @functools.wraps(fn)
            async def _wrapper(*args, **kwargs):
                log_context = thread_local.co_local
                log_context.clear()
                for field, value in _find_log_items(fields, args, kwargs):
                    log_context[field] = value
                return await fn(*args, **kwargs)

        return _wrapper

    return decorator


def _find_log_items(fields: List[str], args: tuple, kwargs: dict):
    args = [*args, *kwargs.values()]
    for field in fields:
        if field in kwargs:
            yield field, kwargs[field]
        else:
            for arg in args:
                try:
                    yield field, getattr(arg, field)
                    break
                except AttributeError:
                    pass


class JSONFormatter(Formatter):

    def format(self, record):
        context: dict = thread_local.co_local

        create_time = datetime.strptime(self.formatTime(record), "%Y-%m-%d %H:%M:%S,%f")
        message = record.getMessage()
        extra_message = {}
        if isinstance(message, Mapping) and "message" in message:
            extra_message = {**message}
            message = extra_message["message"]
            del extra_message["message"]

        log_data = {
            # "uid": uid,
            # "session_id": session_id,
            # "turn": turn,
            "time": create_time.strftime("%Y-%m-%d %H:%M:%S"),
            "level": record.levelname,
            "trace_id": context.get("trace_id", None),
            "code": f"{record.filename}:{record.lineno}:{record.funcName}",
            # "message_source": getattr(record, "message_source", "planning_service"),  # 控制不同源
            # "message_type": getattr(record, "message_type", "common"),  # 控制不同log类型
            # "data": getattr(record, "data", {}),
            "message": message,
            **extra_message
        }

        output_log = json.dumps(log_data, ensure_ascii=False)
        return output_log


class Logger(logging.Logger):

    def __init__(
            self,
            name: str,
            level: int = logging.INFO,
            to_console: bool = True,
            log_file: str = None,
            max_size: int = 1024 * 1024 * 10,
            backup_count: int = 3,
            formatter: Formatter = JSONFormatter(),
    ):
        super().__init__(name, level)
        self.propagate = False
        self.warning_once = functools.lru_cache(self.warning)

        if log_file:
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)
            file_handler = RotatingFileHandler(log_file, maxBytes=max_size, backupCount=backup_count)
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            self.addHandler(file_handler)

        if to_console:
            console_handler = StreamHandler(stream=sys.stdout)
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            self.addHandler(console_handler)

    def turn_start(self):
        turn_id = thread_local.co_local.get("turn_id", "?")
        self._log(logging.INFO, f"TurnStart[{turn_id}]", ())

    def turn_end(self):
        turn_id = thread_local.co_local.get("turn_id", "?")
        self._log(logging.INFO, f"TurnEnd[{turn_id}]", ())


logger = Logger("libentry.logger")
