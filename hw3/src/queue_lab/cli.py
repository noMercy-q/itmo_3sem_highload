import argparse
import json
import time
import unittest

from .checks import check
from .events import utc_now
from .settings import Settings
from .store import Store


def heading(settings):
    print(settings.github_login + " " + utc_now(), flush=True)


def print_report(report):
    print(
        f"acknowledged={report['expected']} sink={report['sink']} dlt={report['dlt']} "
        f"poison={report['poison_expected']} pending_outbox={report['pending_outbox']}"
    )
    print(f"deduplicated={report['deduplicated']} dlt_redeliveries={report['dlt_redeliveries']}")
    print(
        f"missing={report['missing_count']} seq_gaps={report['gap_count']} "
        f"order_violations={report['order_violations']} invalid={len(report['errors'])}"
    )

    for user in report["users"]:
        print(f"{user['user_id']}: seq=1..{user['max_seq']} missing={user['missing']}")

    for example in report["order_examples"][:3]:
        print("late: " + json.dumps(example, ensure_ascii=False))

    for error in report["errors"]:
        print("error: " + error)

    print("RESULT: " + ("PASS" if report["complete"] else "INCOMPLETE"), flush=True)


def verify(args):
    from .inspection import read_dead_letters

    settings = Settings.load()
    heading(settings)
    store = Store(settings.sink_path)

    try:
        expected = store.generated()
        deadline = time.monotonic() + args.wait

        while True:
            dlt = read_dead_letters(settings.bootstrap)
            report = check(expected, store.sink_rows(), dlt, store.counts())

            if report["complete"] or time.monotonic() >= deadline:
                break

            time.sleep(0.5)

        if args.json:
            print(json.dumps(report, indent=2, ensure_ascii=False), flush=True)
        else:
            print_report(report)

        return 0 if report["complete"] else 1
    finally:
        store.close()


def status(_args):
    from .inspection import cluster_status

    settings = Settings.load()
    heading(settings)
    store = Store(settings.sink_path)
    report = {"counts": store.counts()}

    try:
        report["cluster"] = cluster_status(settings.bootstrap)
    except Exception as error:
        report["cluster_error"] = str(error)

    print(json.dumps(report, indent=2), flush=True)
    store.close()
    return 0


def state(args):
    settings = Settings.load()
    heading(settings)
    store = Store(settings.sink_path)
    where, parameters = (" WHERE consumer_id=?", (args.consumer,)) if args.consumer else ("", ())
    row = store.db.execute(
        "SELECT * FROM sink" + where + " ORDER BY write_id DESC LIMIT 1", parameters
    ).fetchone()
    recent = store.db.execute("SELECT * FROM dedup_attempts ORDER BY id DESC LIMIT 5").fetchall()
    print(
        json.dumps(
            {
                "counts": store.counts(),
                "latest": dict(row) if row else None,
                "dedup_attempts": [dict(item) for item in recent],
            },
            indent=2,
        ),
        flush=True,
    )
    store.close()
    return 0


def main():
    parser = argparse.ArgumentParser(description="Kafka resilience lab")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init")
    commands.add_parser("producer")
    commands.add_parser("consumer")
    commands.add_parser("status")
    commands.add_parser("test")
    state_parser = commands.add_parser("state")
    state_parser.add_argument("--consumer")
    check_parser = commands.add_parser("verify")
    check_parser.add_argument("--wait", type=float, default=0)
    check_parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if args.command == "init":
        from .initialize import run
    elif args.command == "producer":
        from .producer import run
    elif args.command == "consumer":
        from .consumer import run
    elif args.command == "verify":
        return verify(args)
    elif args.command == "status":
        return status(args)
    elif args.command == "state":
        return state(args)
    else:
        suite = unittest.defaultTestLoader.discover("tests")
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1

    run()
    return 0
