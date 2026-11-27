import json
import signal
import time

from .events import utc_now


def log(message, **fields):
    print(
        json.dumps({"time": utc_now(), "message": message, **fields}, ensure_ascii=False),
        flush=True,
    )


class Shutdown:
    def __init__(self, grace=30):
        self.requested = False
        self.deadline = None
        self.grace = grace

    def install(self):
        signal.signal(signal.SIGTERM, self.request)
        signal.signal(signal.SIGINT, self.request)

    def request(self, *_):
        if not self.requested:
            self.requested = True
            self.deadline = time.monotonic() + self.grace
            log("shutdown_requested", grace_seconds=self.grace)

    def remaining(self):
        return max(0, self.deadline - time.monotonic()) if self.deadline is not None else 3600
