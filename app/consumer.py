from __future__ import annotations

import os
import sqlite3
import time


def main() -> None:
    db_path = os.getenv("OUTPUT_DB", "/data/metrics.db")
    deadline = time.time() + 30
    while not os.path.exists(db_path) and time.time() < deadline:
        time.sleep(1)
    if not os.path.exists(db_path):
        raise SystemExit(f"No existe {db_path}")

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT merchant_id, window_start, window_end, confirmed_amount,
               confirmed_count, high_risk_count, pane_timing, pane_index,
               idempotency_key
        FROM payment_metrics
        ORDER BY window_start, merchant_id
        """
    ).fetchall()

    print(
        "merchant_id | window_start | amount | count | high_risk | "
        "timing | pane | key"
    )
    for row in rows:
        print(
            f"{row[0]} | {row[1]} | {row[3]} | {row[4]} | "
            f"{row[5]} | {row[6]} | {row[7]} | {row[8]}"
        )
    print(f"ROWS={len(rows)}")


if __name__ == "__main__":
    main()
