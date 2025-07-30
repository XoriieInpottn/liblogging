
from libentry import api
from libentry.service.flask import run_service
from liblogging import log_request

from .demo_tool import DemoTool, Request, Response


class GuessQuestionService:
    def __init__(self):
        self.tool = DemoTool()

    @api.post()
    @log_request("trace_id", message_source="platform_algo")
    def show(self, request: Request) -> Response:
        output = self.tool.show(request)
        return output


def main():
    run_service(
        service_type=GuessQuestionService,
        host="0.0.0.0",
        port="12580",
        num_workers=1,
        num_threads=20
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


# python -u -m example.service
# python example/client.py