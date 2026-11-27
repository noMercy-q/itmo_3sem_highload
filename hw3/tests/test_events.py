import unittest
from dataclasses import replace

from queue_lab.events import Event


class EventTests(unittest.TestCase):
    def test_json_roundtrip(self):
        event = Event.generate("p1", 0, 1, 0)
        self.assertEqual(event, Event.decode(event.encode()))

    def test_payload_can_be_any_json(self):
        event = replace(Event.generate("p1", 0, 1, 0), payload=[1, "text", None])
        self.assertEqual(event, Event.decode(event.encode()))

    def test_invalid_uuid_sequence_type_and_timestamp_are_rejected(self):
        base = Event.generate("p1", 0, 1, 0)

        for changed in (
            replace(base, event_id="bad"),
            replace(base, seq=0),
            replace(base, seq=True),
            replace(base, type="unknown"),
            replace(base, produced_at="2026-11-15T12:00:00"),
        ):
            with self.subTest(changed=changed), self.assertRaises((ValueError, TypeError)):
                Event.decode(changed.encode())

    def test_two_percent_of_generated_events_are_poison(self):
        events = [Event.generate("p1", 0, n + 1, n) for n in range(500)]
        self.assertEqual(10, sum(event.is_poison for event in events))
