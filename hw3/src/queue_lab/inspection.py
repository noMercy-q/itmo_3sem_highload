import time
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from .events import Event


def inspector(bootstrap, group=None):
    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group or "verify-" + str(uuid4()),
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "enable.partition.eof": True,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
            "socket.timeout.ms": 5000,
        }
    )


def read_dead_letters(bootstrap, timeout=15):
    client = inspector(bootstrap)

    try:
        metadata = client.list_topics("events.dlt", timeout=min(timeout, 8))
        topic = metadata.topics["events.dlt"]

        if topic.error:
            raise KafkaException(topic.error)

        bounds = {}
        positions = []

        for partition in sorted(topic.partitions):
            low, high = client.get_watermark_offsets(
                TopicPartition("events.dlt", partition), timeout=5
            )

            if high > low:
                bounds[partition] = high
                positions.append(TopicPartition("events.dlt", partition, low))

        if not positions:
            return []

        client.assign(positions)
        finished = set()
        records = []
        deadline = time.monotonic() + timeout

        while len(finished) < len(bounds) and time.monotonic() < deadline:
            message = client.poll(0.2)

            if message is None:
                continue

            partition = message.partition()

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    if message.offset() >= bounds.get(partition, 0):
                        finished.add(partition)

                    continue

                raise KafkaException(message.error())

            if message.offset() < bounds[partition]:
                records.append(
                    {
                        "event": Event.decode(message.value()),
                        "headers": {key: value.decode() for key, value in message.headers() or []},
                        "partition": partition,
                        "offset": message.offset(),
                    }
                )

            if message.offset() + 1 >= bounds[partition]:
                finished.add(partition)

        if len(finished) != len(bounds):
            raise TimeoutError("DLT snapshot was not completely read; verification is inconclusive")

        return records
    finally:
        client.close()


def cluster_status(bootstrap):
    admin = AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": 5000})
    metadata = admin.list_topics(timeout=6)
    topics = {}

    for name in ("events", "events.dlt"):
        topic = metadata.topics.get(name)

        if topic is None or topic.error:
            topics[name] = {"error": str(topic.error) if topic else "missing"}
        else:
            topics[name] = [
                {"partition": p.id, "leader": p.leader, "replicas": p.replicas, "isr": p.isrs}
                for p in sorted(topic.partitions.values(), key=lambda p: p.id)
            ]

    client = inspector(bootstrap, "events-processor")

    try:
        offsets = client.committed([TopicPartition("events", p) for p in range(3)], timeout=5)
        committed = {str(p.partition): p.offset for p in offsets}
    except KafkaException as error:
        committed = {"error": str(error)}
    finally:
        client.close()

    return {"brokers": sorted(metadata.brokers), "topics": topics, "committed": committed}
