import sqlite3


def upsert(conn, row):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS t (
            k TEXT PRIMARY KEY,
            pane INTEGER,
            value INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO t(k, pane, value)
        VALUES (?, ?, ?)
        ON CONFLICT(k) DO UPDATE SET
            pane = excluded.pane,
            value = excluded.value
        WHERE excluded.pane >= t.pane
        """,
        (row["k"], row["pane"], row["value"]),
    )
    conn.commit()


def test_upsert_is_idempotent_and_latest_wins():
    conn = sqlite3.connect(":memory:")
    upsert(conn, {"k": "m|w", "pane": 1, "value": 10})
    upsert(conn, {"k": "m|w", "pane": 1, "value": 10})
    upsert(conn, {"k": "m|w", "pane": 2, "value": 30})
    rows = conn.execute("SELECT k, pane, value FROM t").fetchall()
    assert rows == [("m|w", 2, 30)]
