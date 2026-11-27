import sqlite3
from pathlib import Path

from .events import Event, utc_now


SCHEMA = """
CREATE TABLE IF NOT EXISTS producer_state (
    producer_id TEXT PRIMARY KEY, cursor INTEGER NOT NULL, users INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS user_sequences (
    user_id TEXT PRIMARY KEY, producer_id TEXT NOT NULL, seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS generated (
    event_id TEXT PRIMARY KEY, producer_id TEXT NOT NULL, user_id TEXT NOT NULL,
    seq INTEGER NOT NULL, body TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0,
    UNIQUE(user_id, seq)
);
CREATE TABLE IF NOT EXISTS sink (
    write_id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
    user_id TEXT NOT NULL, seq INTEGER NOT NULL, consumer_id TEXT NOT NULL,
    topic TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_id INTEGER NOT NULL,
    written_at TEXT NOT NULL, body TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
INSERT OR IGNORE INTO counters VALUES ('deduplicated', 0);
CREATE TABLE IF NOT EXISTS dedup_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL,
    consumer_id TEXT NOT NULL, partition_id INTEGER NOT NULL,
    offset_id INTEGER NOT NULL, recorded_at TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path, initialize=False):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=15, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA busy_timeout=15000")
        self.db.execute("PRAGMA synchronous=FULL")

        if initialize:
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.executescript(SCHEMA)

    def close(self):
        self.db.close()

    def next_event(self, producer_id, users):
        self.db.execute("BEGIN IMMEDIATE")

        try:
            pending = self.db.execute(
                "SELECT body FROM generated WHERE producer_id=? AND delivered=0 ORDER BY rowid LIMIT 1",
                (producer_id,),
            ).fetchone()

            if pending:
                event = Event.decode(pending["body"])
            else:
                state = self.db.execute(
                    "SELECT cursor, users FROM producer_state WHERE producer_id=?", (producer_id,)
                ).fetchone()

                if state and state["users"] != users:
                    raise ValueError("cannot change users per producer without resetting lab data")

                turn = state["cursor"] if state else 0
                user_number = turn % users
                user_id = f"{producer_id}-user-{user_number}"
                previous = self.db.execute(
                    "SELECT seq FROM user_sequences WHERE user_id=?", (user_id,)
                ).fetchone()
                seq = previous["seq"] + 1 if previous else 1
                event = Event.generate(producer_id, user_number, seq, turn)
                self.db.execute(
                    "INSERT INTO producer_state VALUES (?, ?, ?) "
                    "ON CONFLICT(producer_id) DO UPDATE SET cursor=excluded.cursor",
                    (producer_id, turn + 1, users),
                )
                self.db.execute(
                    "INSERT INTO user_sequences VALUES (?, ?, ?) "
                    "ON CONFLICT(user_id) DO UPDATE SET seq=excluded.seq",
                    (user_id, producer_id, seq),
                )
                self.db.execute(
                    "INSERT INTO generated(event_id,producer_id,user_id,seq,body) VALUES (?,?,?,?,?)",
                    (event.event_id, producer_id, user_id, seq, event.encode().decode()),
                )

            self.db.execute("COMMIT")
            return event
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def delivered(self, event_id):
        self.db.execute("UPDATE generated SET delivered=1 WHERE event_id=?", (event_id,))

    def save(self, event, consumer_id, topic, partition, offset):
        self.db.execute("BEGIN IMMEDIATE")

        try:
            body = event.encode().decode()
            result = self.db.execute(
                "INSERT INTO sink(event_id,user_id,seq,consumer_id,topic,partition_id,offset_id,written_at,body) "
                "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO NOTHING",
                (
                    event.event_id,
                    event.user_id,
                    event.seq,
                    consumer_id,
                    topic,
                    partition,
                    offset,
                    utc_now(),
                    body,
                ),
            )
            inserted = result.rowcount == 1

            if not inserted:
                previous = self.db.execute(
                    "SELECT body FROM sink WHERE event_id=?", (event.event_id,)
                ).fetchone()

                if previous["body"] != body:
                    raise ValueError("event_id collision with a different event body")

                self.db.execute("UPDATE counters SET value=value+1 WHERE name='deduplicated'")
                self.db.execute(
                    "INSERT INTO dedup_attempts(event_id,consumer_id,partition_id,offset_id,recorded_at) "
                    "VALUES (?,?,?,?,?)",
                    (event.event_id, consumer_id, partition, offset, utc_now()),
                )

            self.db.execute("COMMIT")
            return inserted
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def generated(self, delivered_only=True):
        where = " WHERE delivered=1" if delivered_only else ""
        return [dict(row) for row in self.db.execute("SELECT * FROM generated" + where)]

    def sink_rows(self):
        return [dict(row) for row in self.db.execute("SELECT * FROM sink ORDER BY write_id")]

    def counts(self):
        return {
            "generated": self.db.execute("SELECT COUNT(*) FROM generated").fetchone()[0],
            "acknowledged": self.db.execute(
                "SELECT COUNT(*) FROM generated WHERE delivered=1"
            ).fetchone()[0],
            "pending_outbox": self.db.execute(
                "SELECT COUNT(*) FROM generated WHERE delivered=0"
            ).fetchone()[0],
            "sink": self.db.execute("SELECT COUNT(*) FROM sink").fetchone()[0],
            "deduplicated": self.db.execute(
                "SELECT value FROM counters WHERE name='deduplicated'"
            ).fetchone()[0],
        }
