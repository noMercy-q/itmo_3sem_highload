import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType

from .runtime import log
from .settings import Settings
from .store import Store


def run():
    settings = Settings.load()
    store = Store(settings.sink_path, initialize=True)
    store.close()
    admin = AdminClient({"bootstrap.servers": settings.bootstrap, "socket.timeout.ms": 5000})
    deadline = time.monotonic() + 150
    topics = [
        NewTopic(
            name,
            num_partitions=3,
            replication_factor=3,
            config={"min.insync.replicas": "2", "retention.ms": "3600000"},
        )
        for name in ("events", "events.dlt")
    ]

    while True:
        try:
            metadata = admin.list_topics(timeout=8)

            if len(metadata.brokers) != 3:
                raise RuntimeError("waiting for all three Kafka brokers")

            for name, future in admin.create_topics(topics, request_timeout=15).items():
                try:
                    future.result()
                except KafkaException as error:
                    if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                        raise

                log("topic_ready", topic=name)

            metadata = admin.list_topics(timeout=8)

            for name in ("events", "events.dlt"):
                partitions = metadata.topics[name].partitions.values()

                if len(metadata.topics[name].partitions) != 3 or any(
                    len(p.replicas) != 3 for p in partitions
                ):
                    raise ValueError(
                        "existing topic has incompatible partitions or replication factor"
                    )

                resource = ConfigResource(ResourceType.TOPIC, name)
                config = admin.describe_configs([resource])[resource].result(timeout=10)

                if config["min.insync.replicas"].value != "2":
                    raise ValueError("existing topic must have min.insync.replicas=2")

            return
        except ValueError:
            raise
        except Exception as error:
            if time.monotonic() >= deadline:
                raise TimeoutError("Kafka topic initialization did not finish") from error

            log("initialization_retry", error=str(error))
            time.sleep(2)
