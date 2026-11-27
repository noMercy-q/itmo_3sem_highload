import os
import time

from .kafka_io import Publisher
from .runtime import Shutdown, log
from .settings import Settings, integer
from .store import Store


def run():
    settings = Settings.load()
    producer_id = os.environ["PRODUCER_ID"]
    mode = os.getenv("KEY_MODE", "user_id")

    if mode not in ("user_id", "none"):
        raise ValueError("KEY_MODE must be user_id or none")

    rate = float(os.getenv("EVENTS_PER_SECOND", "12"))

    if rate <= 0:
        raise ValueError("EVENTS_PER_SECOND must be positive")

    users = integer("USERS_PER_PRODUCER", 7, 1)
    limit = integer("EVENT_LIMIT", 0)
    shutdown = Shutdown()
    shutdown.install()
    store = Store(settings.sink_path)
    publisher = Publisher(settings.bootstrap, producer_id, shutdown)
    sent = 0

    try:
        while not shutdown.requested and (limit == 0 or sent < limit):
            started = time.monotonic()
            event = store.next_event(producer_id, users)
            message = publisher.send(
                "events", event.encode(), event.user_id if mode == "user_id" else None
            )
            store.delivered(event.event_id)
            sent += 1
            log(
                "produced",
                producer=producer_id,
                user_id=event.user_id,
                seq=event.seq,
                event_id=event.event_id,
                partition=message.partition(),
                offset=message.offset(),
            )
            until = started + 1 / rate

            while not shutdown.requested and time.monotonic() < until:
                time.sleep(min(0.05, max(0, until - time.monotonic())))
    finally:
        try:
            publisher.close()
        finally:
            store.close()

    log("producer_stopped", producer=producer_id, acknowledged=sent)
