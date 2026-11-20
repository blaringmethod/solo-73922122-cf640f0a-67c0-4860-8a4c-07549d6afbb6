#!/usr/bin/env python3
"""Integration self-tests for the COMMIT/ROLLBACK [AND CHAIN] fix.

Runs against a real PostgreSQL (DATABASE_URL env var, default localhost).
Exercises the RoundManager engine directly.  Run:

    PYTHONPATH=<pylibs> python3 tests/test_chain.py
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2  # noqa: E402

from app.core import RoundManager, RoundStateError  # noqa: E402

DSN = os.environ.get("DATABASE_URL", "postgresql://postgres@127.0.0.1:5432/postgres")
FAILS: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


def wait_terminal(mgr, rnd, seq, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rec = rnd.find_step(seq)
        if rec and rec["status"] in ("done", "error", "canceled"):
            return rec
        time.sleep(0.05)
    raise AssertionError(f"step {seq} never reached a terminal state")


def wait_state(rnd, state, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if rnd.state == state:
            return rnd
        time.sleep(0.05)
    raise AssertionError(f"round never reached {state}; state={rnd.state} detail={rnd.detail}")


def admin_query(sql, args=()):
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            return cur.fetchall() if cur.description else None
    finally:
        conn.close()


def new_round(mgr) -> "object":
    rnd = mgr.create_round()
    rnd.begin_tx("A")
    rnd.begin_tx("B")
    return rnd


def main() -> int:
    mgr = RoundManager(DSN, step_timeout=10.0)
    mgr.start()
    try:
        # ---- 1. COMMIT AND CHAIN: original tx committed; queued steps canceled ----
        print("1) COMMIT AND CHAIN commits the original tx, chained tx is rolled back")
        rnd = new_round(mgr)
        rnd.submit_step("A", "sql", "INSERT INTO batches VALUES (3, true)")
        rnd.submit_step("A", "sql", "COMMIT AND CHAIN")
        rnd.submit_step("A", "sql", "INSERT INTO batches VALUES (4, false)")  # queued after
        recs = [wait_terminal(mgr, rnd, s) for s in (1, 2, 3)]
        check("step 1 done", recs[0]["status"] == "done", str(recs[0]))
        check("step 2 done with committed outcome + chained",
              recs[1]["status"] == "done"
              and recs[1]["result"]["outcome"] == "committed"
              and recs[1]["result"]["chained"] is True
              and recs[1]["result"]["via"] == "sql",
              str(recs[1]))
        check("step 3 canceled, never executed in the chained transaction",
              recs[2]["status"] == "canceled", str(recs[2]))
        # committed row must be visible from the other path; the queued insert must not
        rnd.submit_step("B", "sql", "SELECT id FROM batches ORDER BY id")
        rb = wait_terminal(mgr, rnd, 4)
        check("B sees committed row 3 but not canceled row 4",
              rb["result"]["rows"] == [[1], [2], [3]], str(rb["result"]))
        check("path A marked committed", rnd.txs["A"]["state"] == "committed",
              rnd.txs["A"]["state"])
        check("path A detail explains chain handling",
              "AND CHAIN" in (rnd.txs["A"]["detail"] or ""), rnd.txs["A"]["detail"])
        # the worker must be winding down / gone and no backend remains
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and rnd.txs["A"]["worker"].is_alive():
            time.sleep(0.05)
        check("path A worker thread exited", not rnd.txs["A"]["worker"].is_alive())
        check("path A connection released flag", rnd.txs["A"]["released"] is True)
        leftover = admin_query(
            "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
            "AND pid <> pg_backend_pid() AND query LIKE %s",
            (f'%{rnd.schema}%',))
        check("no backend holding the round schema remains", leftover[0][0] == 0, str(leftover))
        # no new steps accepted on the finished path
        try:
            rnd.submit_step("A", "sql", "SELECT 1")
            check("further steps on path A rejected", False)
        except RoundStateError:
            check("further steps on path A rejected", True)
        # path B is untouched and keeps working
        rnd.submit_step("B", "sql", "SELECT 1 AS ok")
        rb2 = wait_terminal(mgr, rnd, 5)
        check("path B still operational", rb2["status"] == "done" and rb2["result"]["rows"] == [[True]])
        rnd.submit_step("B", "rollback")
        wait_terminal(mgr, rnd, 6)
        rnd.stop("completed")
        wait_state(rnd, "finished")
        check("round finishes only after the connection was released",
              rnd.state == "finished" and rnd.txs["A"]["released"]
              and rnd.txs["B"]["released"])
        check("schema dropped after finish",
              admin_query("SELECT count(*) FROM pg_namespace WHERE nspname=%s",
                          (rnd.schema,))[0][0] == 0)

        # ---- 2. ROLLBACK AND CHAIN: original tx rolled back ----
        print("2) ROLLBACK AND CHAIN rolls the original tx back; writes vanish")
        rnd = new_round(mgr)
        rnd.submit_step("A", "sql", "INSERT INTO batches VALUES (5, true)")
        rnd.submit_step("A", "sql", "ROLLBACK AND CHAIN")
        rnd.submit_step("A", "sql", "INSERT INTO batches VALUES (6, false)")
        recs = [wait_terminal(mgr, rnd, s) for s in (1, 2, 3)]
        check("rollback-chain step recorded as rolled_back",
              recs[1]["status"] == "done"
              and recs[1]["result"]["outcome"] == "rolled_back"
              and recs[1]["result"]["chained"] is True)
        check("queued step after ROLLBACK AND CHAIN canceled", recs[2]["status"] == "canceled")
        rnd.submit_step("B", "sql", "SELECT count(*) AS n FROM batches")
        rb = wait_terminal(mgr, rnd, 4)
        check("baseline unchanged (2 rows), row 5/6 absent", rb["result"]["rows"] == [[2]],
              str(rb["result"]))
        check("path A marked rolled_back", rnd.txs["A"]["state"] == "rolled_back")
        rnd.submit_step("B", "rollback")
        wait_terminal(mgr, rnd, 5)
        rnd.stop("completed")
        wait_state(rnd, "finished")

        # ---- 3. plain COMMIT / ROLLBACK as SQL: same correct bookkeeping ----
        print("3) plain COMMIT / ROLLBACK issued through /sql end the tx correctly")
        rnd = new_round(mgr)
        rnd.submit_step("A", "sql", "INSERT INTO batches VALUES (7, true)")
        rnd.submit_step("A", "sql", "COMMIT")
        rnd.submit_step("A", "sql", "SELECT 1")
        recs = [wait_terminal(mgr, rnd, s) for s in (1, 2, 3)]
        check("plain COMMIT via SQL recorded as committed",
              recs[1]["status"] == "done"
              and recs[1]["result"]["outcome"] == "committed"
              and recs[1]["result"]["chained"] is False)
        check("steps after plain COMMIT canceled", recs[2]["status"] == "canceled")
        check("path A committed and released",
              rnd.txs["A"]["state"] == "committed" and rnd.txs["A"]["released"])
        rnd.submit_step("B", "sql", "ROLLBACK")
        rb = wait_terminal(mgr, rnd, 4)
        check("plain ROLLBACK via SQL recorded as rolled_back",
              rb["status"] == "done" and rb["result"]["outcome"] == "rolled_back"
              and rb["result"]["chained"] is False)
        check("path B rolled_back", rnd.txs["B"]["state"] == "rolled_back")
        rnd.stop("completed")
        wait_state(rnd, "finished")

        # ---- 4. ROLLBACK TO SAVEPOINT keeps the original transaction alive ----
        print("4) ROLLBACK TO SAVEPOINT does not end the transaction; work continues")
        rnd = new_round(mgr)
        rnd.submit_step("A", "sql", "SAVEPOINT s1")
        rnd.submit_step("A", "sql", "INSERT INTO batches VALUES (8, true)")
        rnd.submit_step("A", "sql", "ROLLBACK TO SAVEPOINT s1")
        rnd.submit_step("A", "sql", "INSERT INTO batches VALUES (9, false)")
        rnd.submit_step("A", "sql", "SELECT id FROM batches ORDER BY id")
        recs = [wait_terminal(mgr, rnd, s) for s in (1, 2, 3, 4, 5)]
        check("all savepoint steps are normal done steps",
              all(r["status"] == "done" for r in recs), str([r["status"] for r in recs]))
        check("row 8 undone, row 9 present in-tx",
              recs[4]["result"]["rows"] == [[1], [2], [9]], str(recs[4]["result"]))
        check("path A still open after savepoint rollback", rnd.txs["A"]["state"] == "open")
        check("path A worker still alive", rnd.txs["A"]["worker"].is_alive())
        # B must not see the uncommitted row 9
        rnd.submit_step("B", "sql", "SELECT count(*) AS n FROM batches")
        rb = wait_terminal(mgr, rnd, 6)
        check("B cannot see A's uncommitted row 9", rb["result"]["rows"] == [[2]])
        # B keeps working independently the whole time
        rnd.submit_step("B", "sql", "SELECT 1 AS ok")
        rb2 = wait_terminal(mgr, rnd, 7)
        check("path B unaffected throughout", rb2["result"]["rows"] == [[True]])
        rnd.submit_step("A", "rollback")
        wait_terminal(mgr, rnd, 8)
        rnd.submit_step("B", "rollback")
        wait_terminal(mgr, rnd, 9)
        rnd.stop("completed")
        wait_state(rnd, "finished")

        # ---- 5. finish while a chain statement is in flight: waits for release ----
        print("5) stopping a round after COMMIT AND CHAIN waits for connection release")
        rnd = new_round(mgr)
        rnd.submit_step("A", "sql", "INSERT INTO batches VALUES (10, true)")
        wait_terminal(mgr, rnd, 1)
        rnd.submit_step("A", "sql", "COMMIT AND CHAIN")
        rnd.submit_step("A", "sql", "SELECT 1")
        wait_terminal(mgr, rnd, 2)
        rnd.submit_step("B", "rollback")
        wait_terminal(mgr, rnd, 4)
        rnd.stop("stopped")
        fin = wait_state(rnd, "finished")
        check("finished reason=stopped", fin.reason == "stopped")
        check("both paths released before finished",
              all(fin.txs[t]["released"] for t in ("A", "B")))
        check("schema cleaned up",
              admin_query("SELECT count(*) FROM pg_namespace WHERE nspname=%s",
                          (rnd.schema,))[0][0] == 0)

        # ---- 6. failed statements keep the path open; recovery still possible ----
        print("6) a failing SQL statement errors the step but keeps the path open")
        rnd = new_round(mgr)
        rnd.submit_step("B", "sql", "SELECT no_such_column FROM batches")
        err = wait_terminal(mgr, rnd, 1)
        check("ordinary SQL error recorded with pgcode 42703",
              err["status"] == "error" and err["pgcode"] == "42703", str(err))
        check("path B still open after an ordinary error", rnd.txs["B"]["state"] == "open")
        rnd.submit_step("B", "rollback")
        rb = wait_terminal(mgr, rnd, 2)
        check("rollback endpoint recovers the aborted tx", rb["status"] == "done")
        check("path B marked rolled_back (endpoint semantics keep the worker until stop)",
              rnd.txs["B"]["state"] == "rolled_back")
        rnd.submit_step("A", "rollback")
        wait_terminal(mgr, rnd, 3)
        rnd.stop("completed")
        wait_state(rnd, "finished")
        check("path B connection released when the round finishes",
              rnd.txs["B"]["released"] is True)

        # ---- 7. regression: original endpoints and the half-batch flow still work ----
        print("7) regression: commit/rollback endpoints still behave as before")
        rnd = new_round(mgr)
        rnd.submit_step("A", "sql", "INSERT INTO batches VALUES (12, true)")
        rnd.submit_step("A", "commit")
        # Wait for A's commit to be fully durable before B reads; otherwise B's
        # first statement may race the commit and take a pre-commit snapshot
        # (ordinary READ COMMITTED behavior, not a lab bug).
        commit_rec = wait_terminal(mgr, rnd, 2)
        check("endpoint commit recorded as committed",
              commit_rec["status"] == "done"
              and commit_rec["result"] == {"outcome": "committed"})
        rnd.submit_step("B", "sql", "SELECT id FROM batches WHERE ready = true ORDER BY id")
        read_rec = wait_terminal(mgr, rnd, 3)
        check("reader sees the committed row via the other connection",
              read_rec["result"]["rows"] == [[1], [12]], str(read_rec["result"]))
        check("endpoint commit keeps path state committed",
              rnd.txs["A"]["state"] == "committed")
        rnd.submit_step("B", "rollback")
        wait_terminal(mgr, rnd, 4)
        rnd.stop("completed")
        wait_state(rnd, "finished")

    finally:
        mgr.shutdown()

    print()
    if FAILS:
        print(f"{len(FAILS)} FAILURE(S): {FAILS}")
        return 1
    print("ALL INTEGRATION TESTS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
