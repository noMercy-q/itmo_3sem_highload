import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4


EVENT_TYPES = ("page_view", "add_to_cart", "purchase")


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass(frozen=True)
class Event:
    event_id: str
    user_id: str
    type: str
    seq: int
    produced_at: str
    payload: Any

    @classmethod
    def generate(cls, producer_id, user_number, seq, turn):
        return cls(
            event_id=str(uuid4()),
            user_id=f"{producer_id}-user-{user_number}",
            type=EVENT_TYPES[turn % len(EVENT_TYPES)],
            seq=seq,
            produced_at=utc_now(),
            payload={"product_id": turn % 100, "poison": (turn + 1) % 50 == 0},
        )

    @classmethod
    def decode(cls, value):
        data = json.loads(value)
        event = cls(**data)
        UUID(event.event_id)

        if not isinstance(event.user_id, str) or not event.user_id:
            raise ValueError("user_id must be a nonempty string")

        if event.type not in EVENT_TYPES:
            raise ValueError("unknown event type")

        if type(event.seq) is not int or event.seq < 1:
            raise ValueError("seq must be a positive integer")

        when = datetime.fromisoformat(event.produced_at)

        if when.tzinfo is None:
            raise ValueError("produced_at must include a timezone")

        return event

    @property
    def is_poison(self):
        return isinstance(self.payload, dict) and self.payload.get("poison") is True

    def encode(self):
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True).encode()
