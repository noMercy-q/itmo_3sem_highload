#!/usr/bin/env python3

import argparse
from contextlib import redirect_stdout
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
sys.path.insert(0, str(ROOT / "src"))


def compose(*arguments, environment=None, check=True):
    env = dict(os.environ, COMPOSE_PROJECT_NAME="hw3")

    if environment:
        env.update({name: str(value) for name, value in environment.items()})

    result = subprocess.run(
        ["docker", "compose", *arguments], cwd=ROOT, env=env, capture_output=True, text=True
    )

    if check and result.returncode:
        raise RuntimeError(result.stdout + result.stderr)

    return result


def tool(*arguments, environment=None):
    result = compose("run", "--rm", "--no-deps", "tools", *arguments, environment=environment)
    lines = result.stdout.splitlines()
    return lines[0], json.loads("\n".join(lines[1:]))


def save_json(name, data):
    (REPORTS / (name + ".json")).write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")


def snapshot(name, environment):
    header, data = tool("status", environment=environment)
    (REPORTS / (name + ".txt")).write_text(header + "\n" + json.dumps(data, indent=2) + "\n")
    save_json(name, data)
    return data


def wait_for(callback, seconds=90):
    deadline = time.monotonic() + seconds

    while time.monotonic() < deadline:
        value = callback()

        if value:
            return value

        time.sleep(0.5)

    raise TimeoutError("experiment condition was not reached")


def fresh(environment):
    print("Resetting only project hw3", flush=True)
    compose("down", "--volumes", "--timeout", "35", environment=environment)
    compose("up", "-d", environment=environment)
    wait_for(lambda: tool("state", environment=environment)[1]["counts"]["acknowledged"] >= 90)


def stop_producers(environment):
    compose(
        "stop", "--timeout", "35", "producer-1", "producer-2", "producer-3", environment=environment
    )
    counts = tool("state", environment=environment)[1]["counts"]

    if counts["pending_outbox"]:
        raise RuntimeError("a producer stopped with an unacknowledged outbox event")


def verify(name, environment):
    from queue_lab.cli import print_report

    header, report = tool("verify", "--wait", "120", "--json", environment=environment)
    output = io.StringIO()

    with redirect_stdout(output):
        print(header)
        print_report(report)

    text = output.getvalue()
    (REPORTS / (name + ".txt")).write_text(text)
    save_json(name, {"captured": header, **report})
    print(text, flush=True)
    return report


def logs(name, environment):
    result = compose("logs", "--no-color", "--tail", "500", environment=environment, check=False)
    (REPORTS / (name + ".log")).write_text(result.stdout + result.stderr)


def kill_experiment():
    environment = {"EVENT_LIMIT": 0, "EVENTS_PER_SECOND": 12, "KEY_MODE": "user_id"}
    fresh(environment)
    snapshot("kill-before", environment)
    initial = tool("state", "--consumer", "consumer-1", environment=environment)[1]["latest"]
    initial_id = initial["write_id"] if initial else 0
    slow = {**environment, "AFTER_SINK_DELAY_MS": 4000}
    compose("up", "-d", "--no-deps", "--force-recreate", "consumer-1", environment=slow)

    def vulnerable_write():
        state = tool("state", "--consumer", "consumer-1", environment=environment)[1]
        row = state["latest"]

        if row and row["write_id"] > initial_id:
            age = (
                datetime.now(timezone.utc) - datetime.fromisoformat(row["written_at"])
            ).total_seconds()

            if age < 2:
                return row

        return None

    row = wait_for(vulnerable_write)
    compose("kill", "--signal", "SIGKILL", "consumer-1", environment=environment)
    save_json("kill-target", row)
    time.sleep(9)
    snapshot("kill-after-rebalance", environment)
    logs("kill-rebalance", environment)
    compose("up", "-d", "--no-deps", "consumer-1", environment=environment)
    time.sleep(5)
    stop_producers(environment)
    report = verify("kill-verify", environment)

    if report["deduplicated"] < 1 or report["order_violations"]:
        raise AssertionError("kill experiment did not demonstrate safe deduplication")

    snapshot("kill-restored", environment)
    logs("kill", environment)


def broker_experiment():
    environment = {"EVENT_LIMIT": 0, "EVENTS_PER_SECOND": 12, "KEY_MODE": "user_id"}
    fresh(environment)
    before = snapshot("brokers-before", environment)
    compose("stop", "--timeout", "20", "kafka-1", environment=environment)
    time.sleep(8)
    one = snapshot("brokers-one-down", environment)
    time.sleep(4)
    one_later = snapshot("brokers-one-down-later", environment)

    if one_later["counts"]["acknowledged"] <= one["counts"]["acknowledged"]:
        raise AssertionError("publishing did not continue with two live brokers")

    compose("stop", "--timeout", "20", "kafka-2", environment=environment)
    time.sleep(5)
    two = snapshot("brokers-two-down", environment)
    time.sleep(12)
    two_later = snapshot("brokers-two-down-later", environment)
    logs("brokers-outage", environment)

    if two_later["counts"]["acknowledged"] != two["counts"]["acknowledged"]:
        raise AssertionError("acks unexpectedly continued without min ISR / controller quorum")

    compose("start", "kafka-2", environment=environment)
    wait_for(
        lambda: tool("state", environment=environment)[1]["counts"]["acknowledged"]
        > two_later["counts"]["acknowledged"],
        seconds=120,
    )
    compose("start", "kafka-1", environment=environment)

    def all_replicas():
        data = tool("status", environment=environment)[1]
        partitions = data.get("cluster", {}).get("topics", {}).get("events", [])
        return (
            data
            if isinstance(partitions, list)
            and len(partitions) == 3
            and all(len(partition["isr"]) == 3 for partition in partitions)
            else None
        )

    restored = wait_for(all_replicas, seconds=120)
    save_json("brokers-restored", restored)
    stop_producers(environment)
    report = verify("brokers-verify", environment)

    if report["order_violations"]:
        raise AssertionError("keyed events changed order after broker recovery")

    logs("brokers", environment)
    save_json(
        "brokers-progress",
        {
            "before": before["counts"]["acknowledged"],
            "one_down_start": one["counts"]["acknowledged"],
            "one_down_end": one_later["counts"]["acknowledged"],
            "two_down_start": two["counts"]["acknowledged"],
            "two_down_end": two_later["counts"]["acknowledged"],
            "recovered": restored["counts"]["acknowledged"],
        },
    )


def ordering_experiment():
    reports = {}

    for mode in ("user_id", "none"):
        environment = {
            "EVENT_LIMIT": 150,
            "EVENTS_PER_SECOND": 15,
            "KEY_MODE": mode,
            "PROCESSING_DELAY_MS": 40,
            "CONSUMER_3_DELAY_MS": 200,
        }
        fresh(environment)
        wait_for(lambda: tool("state", environment=environment)[1]["counts"]["acknowledged"] >= 450)
        stop_producers(environment)
        reports[mode] = verify("ordering-" + mode, environment)
        logs("ordering-" + mode, environment)

    if reports["user_id"]["order_violations"] or not reports["none"]["order_violations"]:
        raise AssertionError("the comparison did not demonstrate per-partition ordering")


def main():
    parser = argparse.ArgumentParser(
        description="Reproduce failure experiments in this homework's Compose project only."
    )
    parser.add_argument("case", choices=("kill", "brokers", "ordering", "all"))
    parser.add_argument(
        "--reset", action="store_true", help="allow clearing this lab's volumes before each case"
    )
    args = parser.parse_args()

    if not args.reset:
        parser.error("use --reset: these experiments recreate volumes of project hw3")

    REPORTS.mkdir(exist_ok=True)
    cases = {"kill": kill_experiment, "brokers": broker_experiment, "ordering": ordering_experiment}

    try:
        for name, run in cases.items():
            if args.case in (name, "all"):
                print("Experiment: " + name, flush=True)
                run()
    finally:
        compose("stop", "--timeout", "35", check=False)


if __name__ == "__main__":
    main()
