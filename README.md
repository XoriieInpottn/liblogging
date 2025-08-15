# liblogging
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/XoriieInpottn/liblogging)

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
python -u service 2>&1 | tee {log_file_path} | liblogging_collector --config-path {your_kafka_path}  --ssl-cafile {your_ssl_cafile_path} --send-kafka
```
tee {log_file_path} 可以将你的程序记录（输出+错误）重定向到文件中（可选）。

[log_collector.py](liblogging/sending/log_collector.py)为`liblogging_collector`的源代码地址。

`env_name`不指定的话，默认读取`os.environ.get("CHAT_ENV", "dev")`.

## 📋Example
增加额外记录字段信息，以及搭配[libentry](https://github.com/XoriieInpottn/libentry)使用的样例见 [example](example)。


## 💡Tips

1. If using Kafka to send messages, please use `pip install liblogging[collector]`.
2. 如果需要数据持久化，推荐日志消息都写在message列中，维护一列节省内存空间。需要后续进行查询的，以字典形式记录，比如logger.info({"key": "value"}), 便于后续查找。

3. 当前默认的trace_id，推荐使用[liblogging/util.py](liblogging/util.py)中的`get_trace_id`函数，该函数会根据请求对象的`uid`, `session_id`, `turn`等字段生成trace_id，默认的[liblogging/sending/log_collector.py](liblogging/sending/log_collector.py)也会根据trace_id拆解`uid`, `session_id`, `turn`，根据`create_time`拆解`create_date`，方便后续进行追溯以及数据存储。以下是构建trace_id的在整个服务入口的示例：
```python
from liblogging.util import get_trace_id

class Request(BaseModel):
    uid: str = Field(..., description="用户id")
    session_id: str = Field(..., description="会话id")
    turn: int = Field(..., description="轮次")
    trace_id: str = Field(..., description="trace_id")

@log_request("trace_id", "message_source")
def set_logger_global_vars(trace_id: str, message_source: str):
    print(f"setting global vars: trace_id={trace_id}, message_source={message_source}")

def run():
    trace_id = get_trace_id(request)
    # 设置全局上下文变量, 这里注意需要以trace_id=xxx, message_source=xxx形式显式传入
    set_logger_global_vars(trace_id=trace_id, message_source="demo")
    request.trace_id = trace_id
    # 可以直接给其他服务传入request对象，后续的logger.info会自动记录trace_id，其他服务需要在服务入口使用@log_request装饰器配置trace_id, message_source等全局上下文变量。可见example/service.py
    your_service_entry(request)

if __name__ == "__main__":
    run()
```

4. liblogging提供了一些常用的，比如logger.tool_start, logger.tool_end, logger.track_start, logger.track_end等。

    (1) 默认message列就是当使用`logger.info(<message>)`时，message的值。

    (2) 从本质上可以理解为当info的内容是字典时，字典的key就是表中的列名，value就是列的值，而使用liblogging默认这一套 `trace_id`, ``create_time`, `create_date`, `uid`, `session_id`, `turn`等字段会自动添加.

    (3) 如果需要统计相应的字段推荐这种方式，方便后续进行查询。
```python
logger.info({
    "message": {"key1": "value1", "key2": "value2"},  # message列的内容，liblogging将json序列化后存储
    "message_type": "<your message_type>"  # message_type列方便后续进行查询
})
```


由log_collector.py默认的数据表结构如下（如果有额外的字段，创建表时和log时的key保持一致即可）：
```sql
CREATE TABLE `agent_log`.`your_table_name(需要和message_source一致)` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `uid` varchar(64) NOT NULL DEFAULT '',
  `session_id` varchar(128) NOT NULL DEFAULT '',
  `turn` smallint NOT NULL DEFAULT '0',
  `trace_id` varchar(255) NOT NULL DEFAULT '',
  `create_date` date NOT NULL,
  `create_time` datetime(3) NOT NULL,
  `insert_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '插入时间',
  `line_info` varchar(255) NOT NULL DEFAULT '' COMMENT '对应代码行信息',
  `message_source` varchar(64) NOT NULL DEFAULT '' COMMENT '消息来源：plan, memory, intent, guess question等，对应表名',
  `message_type` varchar(32) NOT NULL DEFAULT '' COMMENT '消息类型, 可以筛选该key获取相关指标信息',
  `message` text,
  `level` varchar(32) NOT NULL DEFAULT '' COMMENT 'info， warning, error等',
  PRIMARY KEY (`id`,`create_date`),
  KEY `session_id_index` (`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
PARTITION BY RANGE (TO_DAYS(create_date)) (
    PARTITION p202507 VALUES LESS THAN (TO_DAYS('2025-08-01')),
    PARTITION p202508 VALUES LESS THAN (TO_DAYS('2025-09-01')),
    PARTITION p202509 VALUES LESS THAN (TO_DAYS('2025-10-01')),
    PARTITION p202510 VALUES LESS THAN (TO_DAYS('2025-11-01')),
    PARTITION p202511 VALUES LESS THAN (TO_DAYS('2025-12-01')),
    PARTITION p202512 VALUES LESS THAN (TO_DAYS('2026-01-01')),
    PARTITION p202601 VALUES LESS THAN (TO_DAYS('2026-02-01')),
    PARTITION p202602 VALUES LESS THAN (TO_DAYS('2026-03-01')),
    PARTITION p202603 VALUES LESS THAN (TO_DAYS('2026-04-01')),
    PARTITION p202604 VALUES LESS THAN (TO_DAYS('2026-05-01')),
    PARTITION p202605 VALUES LESS THAN (TO_DAYS('2026-06-01')),
    PARTITION p202606 VALUES LESS THAN (TO_DAYS('2026-07-01')),
    PARTITION p202607 VALUES LESS THAN (TO_DAYS('2026-08-01')),
    PARTITION p202608 VALUES LESS THAN (TO_DAYS('2026-09-01')),
    PARTITION p202609 VALUES LESS THAN (TO_DAYS('2026-10-01')),
    PARTITION p202610 VALUES LESS THAN (TO_DAYS('2026-11-01')),
    PARTITION p202611 VALUES LESS THAN (TO_DAYS('2026-12-01')),
    PARTITION p202612 VALUES LESS THAN (TO_DAYS('2027-01-01')),
    PARTITION pMaxRange VALUES LESS THAN MAXVALUE
);
```