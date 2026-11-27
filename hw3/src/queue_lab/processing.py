import time

from .runtime import log


class PoisonEvent(Exception):
    pass


def process(event, delay_ms):
    time.sleep(delay_ms / 1000)

    if event.is_poison:
        raise PoisonEvent("payload.poison is true")


def handle(
    event,
    store,
    publisher,
    consumer_id,
    topic,
    partition,
    offset,
    delay_ms,
):
    for attempt in range(1, 4):
        try:
            process(event, delay_ms)
        except PoisonEvent as error:
            log(
                "processing_failed",
                consumer=consumer_id,
                event_id=event.event_id,
                attempt=attempt,
                error=str(error),
                partition=partition,
                offset=offset,
            )

            if attempt < 3:
                continue

            headers = [
                ("error", str(error).encode()),
                ("original_topic", topic.encode()),
                ("original_partition", str(partition).encode()),
                ("original_offset", str(offset).encode()),
                ("attempts", b"3"),
            ]
            publisher.send("events.dlt", event.encode(), event.user_id, headers)
            log(
                "dead_lettered",
                consumer=consumer_id,
                event_id=event.event_id,
                partition=partition,
                offset=offset,
            )
            return "dlt"

        inserted = store.save(event, consumer_id, topic, partition, offset)
        log(
            "sink_written" if inserted else "deduplicated",
            consumer=consumer_id,
            event_id=event.event_id,
            user_id=event.user_id,
            seq=event.seq,
            partition=partition,
            offset=offset,
        )
        return "sink" if inserted else "duplicate"

    raise AssertionError("processing attempts did not return a result")
