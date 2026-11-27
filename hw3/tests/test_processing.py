import unittest
from unittest.mock import Mock, patch

from queue_lab.events import Event
from queue_lab.processing import handle


class ProcessingTests(unittest.TestCase):
    @patch("queue_lab.processing.log")
    def test_poison_has_three_attempts_before_dlt(self, _log):
        event = Event.generate("p1", 0, 1, 49)
        store, publisher = Mock(), Mock()

        with patch("queue_lab.processing.time.sleep") as sleep:
            destination = handle(event, store, publisher, "c1", "events", 2, 42, 10)

        self.assertEqual("dlt", destination)
        self.assertEqual(3, sleep.call_count)
        store.save.assert_not_called()
        publisher.send.assert_called_once()
        headers = dict(publisher.send.call_args.args[3])
        self.assertEqual(b"events", headers["original_topic"])
        self.assertEqual(b"2", headers["original_partition"])
        self.assertEqual(b"42", headers["original_offset"])
        self.assertEqual(b"3", headers["attempts"])

    @patch("queue_lab.processing.log")
    def test_storage_failure_propagates_instead_of_becoming_a_dead_letter(self, _log):
        store, publisher = Mock(), Mock()
        store.save.side_effect = OSError("disk unavailable")

        with self.assertRaises(OSError):
            handle(Event.generate("p1", 0, 1, 0), store, publisher, "c1", "events", 0, 0, 0)

        publisher.send.assert_not_called()

    @patch("queue_lab.processing.log")
    def test_duplicate_is_reported_without_rewriting_the_sink(self, _log):
        store, publisher = Mock(), Mock()
        store.save.return_value = False
        result = handle(Event.generate("p1", 0, 1, 0), store, publisher, "c2", "events", 1, 8, 0)
        self.assertEqual("duplicate", result)
        publisher.send.assert_not_called()
