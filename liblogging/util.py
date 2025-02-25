#!/usr/bin/env python3

__author__ = "yubin"
__all__ = [
    "get_trace_id",
    "split_trace_id",
]

import uuid
from typing import Dict, Union, Tuple
from datetime import datetime


def get_trace_id(
    request: Union[object, Dict], add_timestamp: bool = True, combine_symbol: str = "+"
) -> str:
    """
    获取trace_id

    参数:
    request (Union[Dict, object]): 请求对象，可以是包含 'uid', 'session_id', 'turn' 属性的对象或包含这些键的字典
    add_timestamp (bool): 是否添加时间戳
    combine_symbol (str): 各个部分的连接符

    返回:
    str: 生成的trace_id
    """
    try:
        if hasattr(request, "uid") and hasattr(request, "session_id") and hasattr(request, "turn"):
            combine = [request.uid, request.session_id, str(request.turn)]
        elif isinstance(request, Dict):
            combine = [request.get("uid"), request.get("session_id"), str(request.get("turn"))]
        else:
            raise TypeError("Unsupported request type")
        if add_timestamp:
            timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            combine.append(timestamp)
        result = combine_symbol.join(combine)
    except (AttributeError, TypeError) as e:
        print(f"Error occurred: {e}")
        result = str(uuid.uuid4())
    return result


def split_trace_id(trace_id: str, combine_symbol: str = "+") -> Tuple:
    """
    拆分trace_id

    参数:
    trace_id (str): 需要拆分的trace_id
    combine_symbol (str): 各个部分的连接符

    返回:
    tuple: 包含原始trace_id和一个字典，字典包含 'uid', 'session_id', 'turn' 键，如果无法正确拆分则返回空字典
    """
    parts = trace_id.split(combine_symbol)
    if len(parts) == 3 or len(parts) == 4:  # 包含时间戳时长度为4
        try:
            uid, session_id, turn = parts[:3]
            return trace_id, {"uid": uid, "session_id": session_id, "turn": turn}
        except ValueError:
            pass
    return trace_id, {}
