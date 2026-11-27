import os
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from .events import Event
from .kafka_io import Publisher
from .processing import handle
from .runtime import Shutdown, log
from .settings import Settings, integer
from .store import Store


def run():
    settings = Settings.load()
    consumer_id = os.environ["CONSUMER_ID"]
    delay_ms = integer("PROCESSING_DELAY_MS", 40)
    after_sink_ms = integer("AFTER_SINK_DELAY_MS", 0)
    shutdown = Shutdown()
    shutdown.install()
    store = Store(settings.sink_path)
    dlt = Publisher(settings.bootstrap, consumer_id + "-dlt", shutdown)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap,
            "client.id": consumer_id,
            "group.id": "events-processor",
            "group.protocol": "classic",
            "partition.assignment.strategy": "range",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 6000,
            "heartbeat.interval.ms": 2000,
            "max.poll.interval.ms": 300000,
            "queued.max.messages.kbytes": 1024,
            "fetch.max.bytes": 1048576,
            "allow.auto.create.topics": False,
        }
    )
    pending = {}

    def commit(partitions=None):
        allowed = (
            {(p.topic, p.partition) for p in partitions} if partitions is not None else set(pending)
        )
        offsets = [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in pending.items()
            if (topic, partition) in allowed
        ]

        if not offsets:
            return

        try:
            result = consumer.commit(offsets=offsets, asynchronous=False)

            for position in result:
                if position.error:
                    log("commit_error", consumer=consumer_id, error=str(position.error))
                else:
                    key = (position.topic, position.partition)

                    if pending.get(key) == position.offset:
                        pending.pop(key, None)
        except KafkaException as error:
            log("commit_error", consumer=consumer_id, error=str(error))

    def assigned(client, partitions):
        try:
            positions = client.committed(partitions, timeout=5)
            offsets = {str(p.partition): p.offset for p in positions}
        except KafkaException as error:
            offsets = {"error": str(error)}

        log(
            "assigned",
            consumer=consumer_id,
            partitions=[p.partition for p in partitions],
            committed=offsets,
        )

    def revoked(_client, partitions):
        commit(partitions)

        for position in partitions:
            pending.pop((position.topic, position.partition), None)

        log("revoked", consumer=consumer_id, partitions=[p.partition for p in partitions])

    def lost(_client, partitions):
        for position in partitions:
            pending.pop((position.topic, position.partition), None)

        log("lost", consumer=consumer_id, partitions=[p.partition for p in partitions])

    consumer.subscribe(["events"], on_assign=assigned, on_revoke=revoked, on_lost=lost)

    try:
        while not shutdown.requested:
            message = consumer.poll(0.5)

            if message is None:
                continue

            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    log("consume_error", consumer=consumer_id, error=str(message.error()))
                    time.sleep(0.2)

                continue

            event = Event.decode(message.value())

            while True:
                try:
                    destination = handle(
                        event,
                        store,
                        dlt,
                        consumer_id,
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        delay_ms,
                    )
                    break
                except Exception as error:
                    log(
                        "handling_error",
                        consumer=consumer_id,
                        error=str(error),
                        partition=message.partition(),
                        offset=message.offset(),
                    )

                    if shutdown.requested:
                        raise

                    time.sleep(0.5)

            if destination != "dlt" and after_sink_ms:
                time.sleep(after_sink_ms / 1000)

            pending[(message.topic(), message.partition())] = message.offset() + 1
            commit()
    finally:
        try:
            commit()
            consumer.close()
            dlt.close()
        finally:
            store.close()

    log("consumer_stopped", consumer=consumer_id)
