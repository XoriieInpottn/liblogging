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
import time
from contextvars import ContextVar
from logging import Formatter, Handler, NOTSET
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Mapping, Optional, Sequence, Union

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


def log_request(*fields, **fields_with_default):
    def decorator(fn):
        if not inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            def _wrapper(*args, **kwargs):
                log_context = thread_local.co_local
                log_context.clear()
                for field, value in _find_log_items(fields, fields_with_default, args, kwargs):
                    log_context[field] = value
                return fn(*args, **kwargs)
        else:
            @functools.wraps(fn)
            async def _wrapper(*args, **kwargs):
                log_context = thread_local.co_local
                log_context.clear()
                for field, value in _find_log_items(fields, fields_with_default, args, kwargs):
                    log_context[field] = value
                return await fn(*args, **kwargs)

        return _wrapper

    return decorator


def _find_log_items(fields: List[str], fields_with_default: dict, args: tuple, kwargs: dict):
    args = [*args, *kwargs.values()]
    all_fields = set(fields).union(fields_with_default.keys())
    for field in all_fields:
        if field in kwargs:
            yield field, kwargs[field]
        else:
            for arg in args:
                try:
                    yield field, getattr(arg, field)
                    break
                except AttributeError:
                    pass
            else:
                if field in fields_with_default:
                    yield field, fields_with_default[field]


class ContextJSONFormatter(Formatter):

    def format(self, record):
        context: dict = thread_local.co_local

        message = record.msg
        extra_message = {}
        if isinstance(message, Mapping) and "message" in message:
            extra_message = {**message}
            message = extra_message["message"]
            del extra_message["message"]
        message_type = extra_message.pop("message_type") if "message_type" in extra_message else "common"
        log_data = {
            "create_time": self.formatTime(record),
            "level": record.levelname,
            # 通过上下文变量控制trace_id
            "trace_id": context.get("trace_id", None),
            "line_info": f"{record.filename}:{record.lineno}:{record.funcName}",
            # 日志消息
            "message": message,
            # 通过上下文变量控制不同源, 比如对话，额外算法服务: 历史标题，欢迎语，Planning等
            "message_source": context.get("message_source", "chat_log"),
            # 控制不同log类型，便于筛选日志数据, 比如tool, llm, turn等
            "message_type": message_type,
            **extra_message
        }
        try:
            output_log = json.dumps(log_data, ensure_ascii=False)
        except TypeError:
            log_data["message"] = str(log_data["message"])
            output_log = json.dumps(log_data, ensure_ascii=False)
        return output_log

    def formatTime(self, record):
        """to match mysql 'datetime(3)' format"""
        ct = self.converter(record.created)
        s = time.strftime(self.default_time_format, ct)
        default_msec_format = '%s.%03d'
        s = default_msec_format % (s, record.msecs)
        return s


class Logger(logging.Logger):

    def __init__(
            self,
            name: str,
            level: int = logging.INFO,
            to_console: bool = True,
            log_file: str = None,
            max_size: int = 1024 * 1024 * 10,
            backup_count: int = 3,
            formatter: Formatter = ContextJSONFormatter()
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

    def track_start(
        self, message: Union[str, Mapping], message_type: str = "on_track_start", stacklevel: int = 2, **kwargs
    ):
        self._log(
            logging.INFO, {"message": message, "message_type": message_type, **kwargs}, (), stacklevel=stacklevel
        )

    def track_end(
        self, message: Union[str, Mapping], message_type: str = "on_track_end", stacklevel: int = 2, **kwargs
    ):
        self._log(
            logging.INFO, {"message": message, "message_type": message_type, **kwargs}, (), stacklevel=stacklevel
        )

    def service_start(self):
        self._log(logging.INFO, {"message": "service_start", "message_type": "on_service_start"}, (), stacklevel=2)

    def service_end(self):
        self._log(logging.INFO, {"message": "service_end", "message_type": "on_service_end"}, (), stacklevel=2)

    def turn_start(self, request: Mapping, **kwargs):
        self.track_start(message=request, message_type="on_turn_start", stacklevel=3, **kwargs)

    def turn_end(self, response: Mapping, **kwargs):
        self.track_end(message=response, message_type="on_turn_end", stacklevel=3, **kwargs)

    def tool_start(self, tool_name: str, inputs: Mapping, **kwargs):
        self.track_start(
            message={"func_name": tool_name, "inputs": inputs},
            message_type="on_tool_start",
            stacklevel=3,
            **kwargs
        )

    def tool_end(self, tool_name: str, output: Mapping, execute_time: float, **kwargs):
        self.track_end(
            message={"func_name": tool_name, "output": output, "duration": round(execute_time, 3)},
            message_type="on_tool_end",
            stacklevel=3,
            **kwargs
        )

    def llm_start(
        self,
        llm_chain_name: str,
        messages: Sequence[Mapping],
        template_info: Mapping,
        model_kwargs: Mapping = None,
        **kwargs
    ):
        log_info_dict = {
            "func_name": llm_chain_name,
            "messages": messages,
            "template_info": template_info,
            "model_kwargs": model_kwargs
        }
        self.track_start(
            message=log_info_dict,
            message_type="on_llm_start",
            stacklevel=3,
            **kwargs
        )

    def llm_end(
        self,
        llm_name: str,
        content: str,
        execute_time: float,
        completion_tokens: int = None,
        prompt_tokens: int = None,
        role: str = "assistant",
        **kwargs
    ):
        log_info_dict = {
            "func_name": llm_name,
            "response": {"role": role, "content": content},
            "generated_tokens": completion_tokens,
            "prompt_tokens": prompt_tokens,
            "duration": round(execute_time, 3)
        }
        self.track_end(message=log_info_dict, message_type="on_llm_end", stacklevel=3, **kwargs)

    def agent(self, message: Union[str, Mapping], **kwargs):
        self._log(logging.INFO, {"message": message, "message_type": "agent", **kwargs}, ())


logger = Logger("libentry.logger")
