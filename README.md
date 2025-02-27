# liblogging

Utilities for logging and sending logs.
```shell
pip install liblogging
```

## 🌟Feature
### 统一日志格式记录
统一了当前agent的日志记录格式，也可自己基于默认格式进行拓展。
当前记录的信息和对应的key如下：
```python
{
    "create_time": "时间戳，默认和mysql列datatime(3)保持一致",
    "level": "like INFO, ERROR, WARNING",
    # 通过上下文变量保存trace_id
    "trace_id": "trace_id for 追溯不同服务的调用链路",
    "line_info": "{record.filename}:{record.lineno}:{record.funcName}",
    "message": message,
    # 通过上下文变量区分不同源, 方便接收不同服务源信息, 比如Chat, Welcome, Planning等
    "message_source": context.get("message_source", "chat_log"),
    # 控制不同log类型，便于筛选日志数据, 比如tool, llm, turn等
    "message_type": message_type,
    # 可以根据自己需求加入其他的字段
    **extra_message
}
```
上述日志信息均以json字符串的形式记录下来，方便存储及后续处理。

### 配置上下文变量，无须重复传参显示记录
通过装饰器形式, 指定需要配置的全局上下文变量, 仅需在整个程序/服务入口配置一次即可。

需要注意的是配置的全局上下文变量，根据加入装饰器下的函数入参名称匹配进行更新，推荐函数参数定义使用`BaseModel`。

```python
主程序/服务: service1.py
from pydantic import BaseModel

from liblogging import log_request,logger


class Request(BaseModel):
    name: str
    trace_id: str

#在主程序入口配置了trace_id这一全局上下文变量，会通过函数入参对该字段进行赋值，后续在该服务下的其他程序logger.info时会读取这一变量并记录下来。
#同时也支持默认参数配置，比如message_source设置了默认值，后续使用logger会记录message_source为"demo"。
@log_request("trace_id", message_source="demo")
def your_service_entry(request: Request):
    logger.info("Processing request")
```

```python
该服务下的其他程序: function1.py，可直接logger.info(). trace_id, message_source均会记录下来。
from liblogging import logger

def test(name):
    logger.info(f"Testing {name}")
```

### 重定向并发送到消息队列
以默认集成的kafka为例，可将上述统一日志格式记录的形式发送至kafka。

kafka 配置文件格式：
```json
{
    "{cluster_name}": {
        "{env_name}": {
            "bootstrap_servers": "server1, server2, server3",
            "username": "username",
            "password": "******",
            "topic": "your topic",
            "...": "..."
        }
    }
}
```

使用形式:
```shell
python service 2>&1 | tee {log_file_path} | liblogging_collector --config-path {your_kafka_path}  --ssl-cafile {your_ssl_cafile_path} --send-kafka
```
tee {log_file_path} 可以将你的程序记录（输出+错误）重定向到文件中（可选）。

[log_collector.py](liblogging/sending/log_collector.py)为`liblogging_collector`的源代码地址。

`env_name`不指定的话，默认读取`os.environ.get("CHAT_ENV", "dev")`.

## 📋Example
增加额外记录字段信息，以及搭配[libentry](https://github.com/XoriieInpottn/libentry)使用的样例见 [example](example)。


## 💡Tips

1. If using Kafka to send messages, please use `pip install liblogging[collector]`.
2. 如果需要数据持久化，推荐日志消息都写在message列中，维护一列节省内存空间。需要后续进行查询的，以字典形式记录，比如logger.info({"key": "value"}), 便于后续查找。