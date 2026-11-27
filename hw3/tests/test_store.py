import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

from queue_lab.events import Event
from queue_lab.store import Store


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = str(Path(self.directory.name) / "sink.db")
        self.store = Store(self.path, initialize=True)

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def test_outbox_recovers_the_same_uuid_and_sequence(self):
        first = self.store.next_event("p1", 2)
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(first, self.store.next_event("p1", 2))
        self.store.delivered(first.event_id)
        second = self.store.next_event("p1", 2)
        self.assertNotEqual(first.event_id, second.event_id)
        self.assertEqual("p1-user-1", second.user_id)

    def test_sequences_are_contiguous_and_users_belong_to_one_producer(self):
        sequences = {}

        for producer in ("p1", "p2", "p3"):
            for _ in range(20):
                event = self.store.next_event(producer, 3)
                sequences.setdefault(event.user_id, []).append(event.seq)
                self.store.delivered(event.event_id)

        self.assertEqual(9, len(sequences))

        for values in sequences.values():
            self.assertEqual(list(range(1, len(values) + 1)), values)

    def test_user_count_cannot_change_midstream(self):
        first = self.store.next_event("p1", 2)
        self.store.delivered(first.event_id)

        with self.assertRaises(ValueError):
            self.store.next_event("p1", 3)

    def test_duplicate_has_one_sink_row_and_a_separate_counter(self):
        event = Event.generate("p1", 0, 1, 0)
        self.assertTrue(self.store.save(event, "c1", "events", 0, 0))
        self.assertFalse(self.store.save(event, "c2", "events", 0, 0))
        self.assertEqual(1, self.store.counts()["sink"])
        self.assertEqual(1, self.store.counts()["deduplicated"])
        self.assertEqual("c1", self.store.sink_rows()[0]["consumer_id"])

    def test_uuid_collision_does_not_silently_discard_another_body(self):
        event = Event.generate("p1", 0, 1, 0)
        self.store.save(event, "c1", "events", 0, 0)

        with self.assertRaises(ValueError):
            self.store.save(replace(event, seq=2), "c2", "events", 0, 1)

        self.assertEqual(0, self.store.counts()["deduplicated"])

    def test_concurrent_redelivery_is_atomic(self):
        event = Event.generate("p1", 0, 1, 0)

        def write(_):
            store = Store(self.path)

            try:
                return store.save(event, "c1", "events", 0, 0)
            finally:
                store.close()

        with ThreadPoolExecutor(max_workers=4) as pool:
            inserted = list(pool.map(write, range(12)))

        self.assertEqual(1, sum(inserted))
        self.assertEqual(11, self.store.counts()["deduplicated"])
