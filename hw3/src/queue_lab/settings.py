import os
from dataclasses import dataclass


def integer(name, default, minimum=0):
    value = int(os.getenv(name, str(default)))

    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")

    return value


@dataclass(frozen=True)
class Settings:
    bootstrap: str
    sink_path: str
    github_login: str

    @classmethod
    def load(cls):
        return cls(
            bootstrap=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"
            ),
            sink_path=os.getenv("SINK_PATH", "/data/sink.sqlite3"),
            github_login=os.getenv("GITHUB_LOGIN", "noMercy-q"),
        )
