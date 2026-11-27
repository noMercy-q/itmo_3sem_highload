import time

from confluent_kafka import KafkaException, Producer

from .runtime import log


class Publisher:
    def __init__(self, bootstrap, client_id, shutdown):
        self.shutdown = shutdown
        self.client = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,
                "max.in.flight.requests.per.connection": 5,
                "delivery.timeout.ms": 10000,
                "request.timeout.ms": 3000,
                "retry.backoff.ms": 200,
                "partitioner": "murmur2_random",
                "sticky.partitioning.linger.ms": 0,
                "queue.buffering.max.kbytes": 1024,
                "allow.auto.create.topics": False,
            }
        )

    def send(self, topic, value, key, headers=None):
        while self.shutdown.remaining() > 0:
            outcome = []

            def delivered(error, message):
                outcome.append((error, message))

            try:
                self.client.produce(
                    topic, value=value, key=key, headers=headers, on_delivery=delivered
                )
            except (BufferError, KafkaException) as error:
                log("produce_error", topic=topic, error=str(error))
                self.client.poll(min(0.2, self.shutdown.remaining()))
                continue

            while not outcome and self.shutdown.remaining() > 0:
                self.client.poll(min(0.2, self.shutdown.remaining()))

            if not outcome:
                break

            error, message = outcome[0]

            if error is None:
                return message

            log("delivery_error", topic=topic, error=str(error))
            time.sleep(min(0.2, self.shutdown.remaining()))

        raise TimeoutError("shutdown deadline expired before Kafka acknowledged the event")

    def close(self):
        remaining = self.client.flush(min(30, self.shutdown.remaining()))

        if remaining:
            log("flush_incomplete", pending=remaining)
            raise TimeoutError(f"{remaining} messages were not flushed")
