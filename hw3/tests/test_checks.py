import unittest

from queue_lab.checks import check
from queue_lab.events import Event


def expected(event):
    return {"event_id": event.event_id, "body": event.encode().decode()}


def sink(event, write_id):
    return {
        **expected(event),
        "user_id": event.user_id,
        "seq": event.seq,
        "write_id": write_id,
        "partition_id": 0,
        "consumer_id": "c1",
    }


def dead_letter(event):
    return {
        "event": event,
        "headers": {
            "error": "poison",
            "attempts": "3",
            "original_topic": "events",
            "original_partition": "0",
            "original_offset": "0",
        },
    }


class CheckTests(unittest.TestCase):
    def setUp(self):
        self.counts = {"deduplicated": 7, "pending_outbox": 0}

    def test_poison_in_dlt_covers_the_sequence_gap(self):
        one = Event.generate("p1", 0, 1, 0)
        two = Event.generate("p1", 0, 2, 49)
        report = check(
            [expected(one), expected(two)], [sink(one, 1)], [dead_letter(two)], self.counts
        )
        self.assertTrue(report["complete"])
        self.assertEqual(0, report["gap_count"])
        self.assertEqual(7, report["deduplicated"])

    def test_missing_tail_is_detected_using_acknowledged_manifest(self):
        one = Event.generate("p1", 0, 1, 0)
        two = Event.generate("p1", 0, 2, 1)
        report = check([expected(one), expected(two)], [sink(one, 1)], [], self.counts)
        self.assertFalse(report["complete"])
        self.assertEqual(1, report["missing_count"])
        self.assertEqual([2], report["users"][0]["missing"])

    def test_missing_initial_sequence_is_detected(self):
        event = Event.generate("p1", 0, 3, 2)
        report = check([expected(event)], [sink(event, 1)], [], self.counts)
        self.assertFalse(report["complete"])
        self.assertEqual([1, 2], report["users"][0]["missing"])

    def test_late_smaller_sequence_is_counted_once_per_event(self):
        events = [Event.generate("p1", 0, seq, seq) for seq in (1, 2, 3)]
        rows = [sink(events[2], 1), sink(events[1], 2), sink(events[0], 3)]
        report = check(list(map(expected, events)), rows, [], self.counts)
        self.assertTrue(report["complete"])
        self.assertEqual(2, report["order_violations"])

    def test_dlt_redeliveries_do_not_inflate_processed_count(self):
        event = Event.generate("p1", 0, 1, 49)
        report = check([expected(event)], [], [dead_letter(event), dead_letter(event)], self.counts)
        self.assertTrue(report["complete"])
        self.assertEqual(1, report["dlt"])
        self.assertEqual(1, report["dlt_redeliveries"])

    def test_invalid_dlt_headers_cannot_pass_verification(self):
        event = Event.generate("p1", 0, 1, 49)
        record = dead_letter(event)
        record["headers"]["attempts"] = "2"
        report = check([expected(event)], [], [record], self.counts)
        self.assertFalse(report["complete"])

    def test_non_poison_in_dlt_cannot_pass_verification(self):
        event = Event.generate("p1", 0, 1, 0)
        report = check([expected(event)], [], [dead_letter(event)], self.counts)
        self.assertFalse(report["complete"])
