# Runbook: backfilling `position` onto an existing MongoDB event store

## Who this is for

Anyone upgrading an existing Occurrent MongoDB event store deployment to a version where stream position is on.
Stream position is optional but on by default, and this only matters for a deployment that already has events in
its collection. A store with no events, or a brand-new deployment, needs no backfill.

## Why this is needed

`position` is a global, monotonically increasing integer stamped on every event. It replaces the wall-clock/`$natural`
based catch-up reconciliation with a range-based one that is immune to clock skew, and it lets stream and DCB
consumers reconcile on one ordering axis. Events written before position existed have no `position` field. Until
they are backfilled, they are invisible to position-ordered reads and to position-based catch-up, and any store that
writes position will log a loud warning naming this runbook on startup if it finds such events in its collection.

The backfill tool that performs steps 3-4 below lives at `eventstore/migration/position-backfill` (artifact
`org.occurrent:eventstore-mongodb-position-backfill`). It reuses the store's own position document mapper and
counter contract, so the schema it writes is exactly what a live store writes.

## The safe upgrade sequence

Do these steps in order. Skipping ahead (particularly deploying before the index exists, or relying on position
before the backfill has completed) can produce a temporary blind spot where some events are invisible to
position-ordered reads, though it does not lose or corrupt any event data.

### 1. Create the position index

Before deploying the new version, create an index on the `position` field of the event collection. On MongoDB
Atlas (or any replica set), build this as a rolling index so it does not block writes:

```javascript
db.events.createIndex({ position: 1 }, { background: true })
```

Do this first so the new version's queries and the backfill's own scans have the index available from the start.

### 2. Seed the position counter

Seed the counter document above the current historical event count, using an accurate count (not an estimated one,
which can undercount and let a live write collide with a position the backfill has not assigned yet) plus a slack
margin. The backfill tool's `seedCounter()` step does this:

```java
MongoDatabase database = mongoClient.getDatabase("my-database");
PositionBackfill backfill = new PositionBackfill(database, "events", PositionBackfillOptions.defaults());
long seededTo = backfill.seedCounter();
```

This step is idempotent: if the counter already sits at or above the target, it is left untouched. Run it once
before deploying the new version so live writes after deploy immediately get positions above every position the
backfill will later assign to historical events.

### 3. Deploy the new version

Deploy the version that writes position. From this point:

- New stream events get a `position` on write.
- Catch-up subscriptions stay on the legacy time/`$natural` path until the backfill (step 4) completes and full
  coverage is verified (step 5), because position-based catch-up would otherwise miss any un-positioned historical
  event.
- The startup guard logs a WARN (or fails hard, if `requireBackfilledPosition` is enabled) pointing at this runbook
  for as long as un-positioned events remain in the collection.

### 4. Run the backfill

Run the throttled, resumable, idempotent backfill against the same collection:

```java
PositionBackfillResult result = backfill.run();
```

Properties of this step:

- **Ordering.** Events are processed in `_id` order, not by the time field. `_id` is the collection's primary key
  and is always indexed, so no extra index is needed, and the result carries no clock-skew assumption. History is
  immutable, so any stable order is sufficient for replay. `_id` order does not need to match original insertion
  order exactly.
- **Throttled.** Configure `PositionBackfillOptions.batchSize()` and `throttleMillis()` to control write-capacity
  impact. Larger batches finish faster but hold write locks longer per iteration. A `throttleMillis` sleep between
  batches leaves headroom for production traffic.
- **Resumable.** A checkpoint document records the last processed `_id`. If the process is killed or crashes
  mid-run, starting it again resumes from that checkpoint instead of rescanning from the beginning.
- **Idempotent.** Only events missing `position` are touched (`exists(position: false)`), so re-running after a
  partial or a complete run is always safe and a completed run reports zero events positioned on a repeat call.

For very large collections, run this as a long-lived job (a Kubernetes Job, a one-off ECS task, or similar) rather
than a local script, since it may run for hours depending on collection size and the chosen throttle.

### 5. Verify full coverage

Confirm no un-positioned events remain:

```javascript
db.events.countDocuments({ position: { $exists: false } })
```

This should return `0`. The store's own startup guard performs the same check on every restart, so restarting an
instance after the backfill completes is also a verification step: no WARN means no un-positioned events remain.

### 6. Position-based catch-up is now safe

Once step 5 confirms zero un-positioned events, position-based catch-up can be relied on for this collection. No
further action is needed here: the store already switches reconciliation strategy based on whether it writes
position, and once the historical gap is closed, position-ordered reads see the full history.

## Resume-token transition for live subscriptions

A subscription that was running against this store before the upgrade holds a persisted resume token in the legacy
time-based form (`TimeBasedSubscriptionPosition`). After the flip to position-based catch-up, the model is now in
position mode, but that stored token is still time-based.

Both token forms are self-describing, so position-mode catch-up detects a legacy time-based token on resume and
performs a one-time handoff rather than trying to interpret it as a position. The recommended and default handling
is to re-resolve the subscription to the current global position (equivalently, the model's default starting point)
instead of trusting the stale time-based value. This means a subscription resuming for the first time after the
flip effectively restarts its catch-up window from the point of the flip, not from where the time-based token left
off.

Two practical implications:

- If a subscription's projection must not reprocess events it already saw, take a stable snapshot of subscription
  state before deploying the new version, so a re-run from the flip point can be reconciled against that snapshot
  if needed.
- If small amounts of reprocessing are acceptable (idempotent projections, deduplicated side effects), no extra
  action is needed beyond the sequence above.

## Rollback considerations

Backfilling `position` is additive: it only ever sets a field that did not exist, it never removes or rewrites any
other field, and the counter seed step only increases the counter, never decreases it. Rolling back to a version
that does not write or read position leaves the backfilled events intact and harmless. A later re-upgrade does not
need to repeat the backfill for events it already covered, since the tool is idempotent and simply reports zero
newly positioned events for anything already done.
