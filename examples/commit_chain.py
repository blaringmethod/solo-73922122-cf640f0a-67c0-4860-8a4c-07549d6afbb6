#!/usr/bin/env python3
"""COMMIT / ROLLBACK ... AND CHAIN and savepoint semantics.

Before the fix, a ``COMMIT AND CHAIN`` (or ``ROLLBACK AND CHAIN``) statement
sent through the ordinary SQL step endpoint was treated like any other
statement: the lab kept attributing later reads and writes to the ORIGINAL
transaction, even though PostgreSQL had already finished it and silently
opened a brand-new transaction on the same connection.

This script demonstrates the corrected behavior:

1. COMMIT AND CHAIN
   - the step record shows the original transaction was committed (the
     database result is kept verbatim, ``chained: true``);
   - steps still queued behind it are marked ``canceled`` and never run
     against the server-opened follow-up transaction;
   - the follow-up transaction is rolled back, the connection is released
     and the path state is ``committed``;
   - the other path (B) keeps running untouched.

2. ROLLBACK AND CHAIN
   - the original transaction is marked rolled_back and its writes vanish;
   - queued steps are canceled as well.

3. ROLLBACK TO SAVEPOINT
   - does NOT end the transaction: the same path keeps going on the
     original transaction and the other path proceeds normally.

Run:  python3 examples/commit_chain.py
"""

import json
import urllib.error
import urllib.request

from lab_client import (BASE, begin, new_round, req, show_round, sql,
                        wait_round, wait_step)


def queue_sql(rid, tx, statement):
    """Queue a SQL step; if the path has already closed, report the 409."""
    data = json.dumps({"sql": statement}).encode()
    request = urllib.request.Request(
        BASE + f"/rounds/{rid}/tx/{tx}/sql", data=data, method="POST",
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request) as resp:
            return json.loads(resp.read())["seq"], None
    except urllib.error.HTTPError as exc:
        return None, f"HTTP {exc.code}: {exc.read().decode().strip()}"


print("=== 1. COMMIT AND CHAIN: original tx committed, queued steps canceled ===")
rid = new_round()
begin(rid, "A")
begin(rid, "B")

# Queue all three without waiting.  The short pg_sleep keeps the worker busy
# so the third step is still queued when COMMIT AND CHAIN ends the original
# transaction; it then gets canceled instead of running inside the
# server-opened follow-up transaction.
sql(rid, "A", "INSERT INTO batches VALUES (3, true)")
sql(rid, "A", "SELECT pg_sleep(2)")
s2 = sql(rid, "A", "COMMIT AND CHAIN")
s3, rejected = queue_sql(rid, "A", "INSERT INTO batches VALUES (4, false)")
chain = wait_step(rid, s2)
print("chain step result:", chain["result"])
if s3 is not None:
    queued = wait_step(rid, s3)
    print("queued step status:", queued["status"], "-", queued["error"])
else:
    print("path A had already closed when the step arrived:", rejected)

# B is an independent transaction path and keeps working.
b = wait_step(rid, sql(rid, "B", "SELECT id FROM batches ORDER BY id"))
print("B sees committed row 3, never the canceled row 4:", b["result"]["rows"])
wait_step(rid, req("POST", f"/rounds/{rid}/tx/B/rollback")["seq"])

req("POST", f"/rounds/{rid}/finish")
wait_round(rid)
show_round(rid)

print()
print("=== 2. ROLLBACK AND CHAIN: original tx rolled back, writes vanish ===")
rid = new_round()
begin(rid, "A")
begin(rid, "B")

sql(rid, "A", "INSERT INTO batches VALUES (5, true)")
sql(rid, "A", "SELECT pg_sleep(2)")
s2 = sql(rid, "A", "ROLLBACK AND CHAIN")
s3, rejected = queue_sql(rid, "A", "INSERT INTO batches VALUES (6, false)")
rchain = wait_step(rid, s2)
print("chain step result:", rchain["result"])
if s3 is not None:
    queued = wait_step(rid, s3)
    print("queued step status:", queued["status"], "-", queued["error"])
else:
    print("path A had already closed when the step arrived:", rejected)
cnt = wait_step(rid, sql(rid, "B", "SELECT count(*) AS n FROM batches"))
print("B still sees only the baseline (2 rows):", cnt["result"]["rows"])
wait_step(rid, req("POST", f"/rounds/{rid}/tx/B/rollback")["seq"])

req("POST", f"/rounds/{rid}/finish")
wait_round(rid)
show_round(rid)

print()
print("=== 3. ROLLBACK TO SAVEPOINT: the original transaction stays open ===")
rid = new_round()
begin(rid, "A")
begin(rid, "B")

for stmt in (
    "SAVEPOINT s1",
    "INSERT INTO batches VALUES (7, true)",
    "ROLLBACK TO SAVEPOINT s1",       # undoes row 7, transaction still open
    "INSERT INTO batches VALUES (8, false)",
    "SELECT id FROM batches ORDER BY id",
):
    rec = wait_step(rid, sql(rid, "A", stmt))
    if rec.get("result", {}).get("rows"):
        print("in-transaction view (row 7 gone, row 8 present):", rec["result"]["rows"])
# B cannot see A's uncommitted row 8 and keeps running independently.
bcnt = wait_step(rid, sql(rid, "B", "SELECT count(*) AS n FROM batches"))
print("B sees only the baseline while A is still open:", bcnt["result"]["rows"])
wait_step(rid, req("POST", f"/rounds/{rid}/tx/A/rollback")["seq"])
wait_step(rid, req("POST", f"/rounds/{rid}/tx/B/rollback")["seq"])

req("POST", f"/rounds/{rid}/finish")
wait_round(rid)
show_round(rid)
