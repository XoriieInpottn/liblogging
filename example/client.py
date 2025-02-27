from libentry.api import APIClient


def request():
    client = APIClient(base_url="http://localhost:12580")
    result = client.post("show", {"trace_id": "test-session", "name": "test-man"})
    print(result)


request()