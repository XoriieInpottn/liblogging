import time

from pydantic import BaseModel
from liblogging import logger


class Request(BaseModel):
    trace_id: str
    name: str


class Response(BaseModel):
    result: str


class DemoTool:
    def __init__(self):
        pass

    def show(self, request: Request) -> Response:
        start_time = time.time()
        # 内置了常用的logger函数. logger.tool_start, logger.tool_end等.
        logger.tool_start(tool_name="show", inputs=request.model_dump())
        logger.info("this is a demo tool")
        # 使用logger.info 增加其他日志字段，请用字典形式传入，字典中须包含message，然后添加其他字段。
        # 如下所示的"data"字段, 此外"message_type"字段也通过这种方式自定义，可用于区分不同的日志类型。默认为"common""
        logger.info({
            "message": "this is a demo tool",
            "data": {"name": "添加额外的日志字段"},
            "message_type": "self_definition"
        })
        logger.tool_end(tool_name="show", output={"result": "hello world"}, execute_time=time.time() - start_time)

        # 如果需要自己增加xxx start, xxx end, 可使用内置的logger.track_start 和 logger.track_end
        # 增加其他日志字段, 按照关键字参数传入
        logger.track_start(message="my track start", message_type="my_start", data={"name": "我自己定义的start"})
        logger.track_end(
            message="my track end", message_type="my_end", data={"name": "end", "duration": time.time() - start_time}
        )
        return Response(result="hello world")