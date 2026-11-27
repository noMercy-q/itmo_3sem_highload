from collections import defaultdict

from .events import Event


def check(expected, sink, dead_letters, counts):
    intended = {row["event_id"]: Event.decode(row["body"]) for row in expected}
    sink = [row for row in sink if row["event_id"] in intended]
    dlt = [row for row in dead_letters if row["event"].event_id in intended]
    observed = defaultdict(set)
    errors = []
    sink_ids = set()
    dlt_ids = set()
    maxima = defaultdict(int)
    violations = []

    for row in sorted(sink, key=lambda value: value["write_id"]):
        event = intended[row["event_id"]]

        if event.is_poison or event.encode().decode() != row["body"]:
            errors.append("unexpected sink body: " + event.event_id)

        if row["user_id"] != event.user_id or row["seq"] != event.seq:
            errors.append("sink columns disagree with event: " + event.event_id)

        if event.event_id in sink_ids:
            errors.append("duplicate sink event_id: " + event.event_id)

        sink_ids.add(event.event_id)
        observed[event.user_id].add(event.seq)

        if event.seq < maxima[event.user_id]:
            violations.append(
                {
                    "user_id": event.user_id,
                    "seq": event.seq,
                    "previous_max": maxima[event.user_id],
                    "write_id": row["write_id"],
                    "partition": row["partition_id"],
                    "consumer": row["consumer_id"],
                }
            )

        maxima[event.user_id] = max(maxima[event.user_id], event.seq)

    for record in dlt:
        event = record["event"]
        original = intended[event.event_id]
        headers = record["headers"]

        if not event.is_poison or event.encode() != original.encode():
            errors.append("unexpected DLT body: " + event.event_id)

        try:
            valid = (
                bool(headers["error"])
                and headers["original_topic"] == "events"
                and 0 <= int(headers["original_partition"]) < 3
                and int(headers["original_offset"]) >= 0
                and headers["attempts"] == "3"
            )
        except (KeyError, ValueError):
            valid = False

        if not valid:
            errors.append("invalid DLT headers: " + event.event_id)

        dlt_ids.add(event.event_id)
        observed[event.user_id].add(event.seq)

    if sink_ids & dlt_ids:
        errors.append("events present in both sink and DLT")

    high = defaultdict(int)

    for event in intended.values():
        high[event.user_id] = max(high[event.user_id], event.seq)

    users = []

    for user, last in sorted(high.items()):
        missing = sorted(set(range(1, last + 1)) - observed[user])
        users.append({"user_id": user, "max_seq": last, "missing": missing})

    missing_ids = sorted(set(intended) - sink_ids - dlt_ids)
    gaps = sum(len(user["missing"]) for user in users)
    return {
        "complete": not missing_ids and not gaps and not errors,
        "expected": len(intended),
        "sink": len(sink_ids),
        "dlt": len(dlt_ids),
        "poison_expected": sum(event.is_poison for event in intended.values()),
        "dlt_redeliveries": len(dlt) - len(dlt_ids),
        "deduplicated": counts["deduplicated"],
        "pending_outbox": counts["pending_outbox"],
        "missing_count": len(missing_ids),
        "missing_ids": missing_ids[:10],
        "gap_count": gaps,
        "order_violations": len(violations),
        "order_examples": violations[:5],
        "errors": errors[:10],
        "users": users,
    }
