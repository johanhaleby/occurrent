# Position backfill

Adds the global `position` field to events that were written before `position` existed, so an existing MongoDB
event store can turn stream position on without losing sight of its old events.

Maven coordinates: `org.occurrent:eventstore-mongodb-position-backfill`.

## The problem

Occurrent gives every event a global, always-increasing `position`. Position is what lets catch-up
subscriptions replay history in a fixed order that does not depend on wall-clock time, and it lets stream and DCB
consumers order events on one shared axis.

Events written by an older version have no `position` field. Until you add it, those events are invisible to
position-ordered reads and to position-based catch-up.

You only need this tool if you have an existing store that already contains events. A brand-new or empty
deployment gets `position` on every event from the start and needs no backfill.

## What this tool does

The tool does exactly two things, and nothing else. Both connect straight to MongoDB and both are safe to run more
than once.

- **Seed the counter** (`seedCounter()`). Raises the shared position counter above the current event count plus a
  slack margin, so positions written by the running application land above every position the backfill will assign
  to the old events. The two never collide. It reuses the store's own counter, so a running store and this tool
  share one sequence.
- **Backfill** (`run()`). First seeds the counter (a no-op if you already did it), then sets `position` on every
  event that does not have one, walking the collection in `_id` order in batches. It reuses the store's own
  position mapper, so a backfilled event looks exactly like one a running store writes.

`run()` is:

- **Idempotent.** It only touches events that are missing `position`, so running it twice is safe. A second run
  positions nothing.
- **Resumable.** It records the last processed `_id` in a checkpoint document (`_id: "positionBackfill"` in the
  `<events>_position_backfill_checkpoint` collection). If the process is killed, running it again continues from
  where it stopped. On successful completion the checkpoint document is removed, so a finished backfill leaves no
  state behind.
- **Throttled.** You can make it sleep between batches so it does not compete with production traffic.

Everything the tool does is the two calls above. The steps in the next section around them are things you do, not
things the tool does.

## The upgrade sequence

Enabling position on an existing store is a deliberate, ordered process. Steps marked **[you]** are manual and
mandatory. Steps marked **[tool]** are calls into the Java program described above (see "How to run" below).

One thing to know first: a store will not turn position on by itself for a collection that still has un-backfilled
events. That is a safety guard, so bumping the Occurrent version on a large existing store does not silently build
the position index over the whole collection at startup. It means you turn position on deliberately, in this order.

1. **[you] Create the `position` index** as a rolling or background index, before the store runs with position on,
   so startup does not build it over the whole collection.

   ```javascript
   db.events.createIndex({ position: 1 }, { background: true })
   ```

   On MongoDB Atlas (or any replica set) this builds as a rolling index that does not block writes. Replace
   `events` with your event collection name.

2. **[tool] Seed the counter**, before you deploy, so the positions the running application writes land above the
   ones the backfill gives the old events.

   ```java
   MongoDatabase database = mongoClient.getDatabase("my-database");
   PositionBackfill backfill = new PositionBackfill(database, "events", PositionBackfillOptions.defaults());
   backfill.seedCounter();
   ```

3. **[you] Deploy the new version with stream position enabled explicitly** (`EventStoreConfig.Builder.withStreamPosition()`
   or `occurrent.event-store.stream.position=true`). Explicit is required on an existing store, otherwise the guard
   above keeps position off because the history is not backfilled yet. From here new events get a `position`.
   Catch-up stays on the older time-based path until step 5 confirms full coverage, so nothing is missed meanwhile.

4. **[tool] Run the backfill** to position every remaining old event.

   ```java
   backfill.run();
   ```

5. **[you] Check that every event has a position.**

   ```javascript
   db.events.countDocuments({ position: { $exists: false } })
   ```

   This should return `0`.

6. **Done.** Position-based catch-up is now safe for this collection. The store switches to it on its own.

If you can take a short write freeze, there is a simpler variant: stop writes, run the backfill first, then deploy
with position left on its default. Once every event has a position the guard passes on its own, so you do not need
the explicit setting in step 3. The runbook covers this.

## How to run the module

Steps 2 and 4 above are calls into this module. You can run them from your own code or as a standalone command.

### From Java

Use the constructor to control batching and throttling. Call `seedCounter()` for step 2 and `run()` for step 4:

```java
try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
    MongoDatabase database = mongoClient.getDatabase("my-database");

    PositionBackfillOptions options = PositionBackfillOptions.defaults()
            .withBatchSize(1000)
            .withThrottleMillis(200);

    PositionBackfill backfill = new PositionBackfill(database, "events", options);
    backfill.run();
}
```

### As a standalone command

Build the runnable jar:

```bash
mvn -pl eventstore/migration/position-backfill -am package -DskipTests
```

That produces `eventstore-mongodb-position-backfill-<version>-cli.jar` in the module's `target/` directory, a single
jar with all dependencies bundled. Run it with `<mongoUri> <database> <collection>`:

```bash
java -jar eventstore/migration/position-backfill/target/eventstore-mongodb-position-backfill-<version>-cli.jar \
  "mongodb://localhost:27017" my-database events
```

The command runs `run()` with the default options, which seeds the counter and backfills in one go. If you want to
seed before deploying (step 2) separately, call `seedCounter()` from Java as shown above. For a large collection,
run this as a long-lived job (a Kubernetes Job or a one-off ECS task) rather than a script on your laptop.

## Options

`PositionBackfillOptions.defaults()` is a good starting point. You can adjust:

| Option | Default | What it does |
| --- | --- | --- |
| `batchSize` | 500 | How many events to position per batch. Larger batches finish faster but hold write locks longer each iteration. |
| `throttleMillis` | 0 | How long to sleep between batches. Raise this to leave more room for production traffic. `0` means no pause. |
| `counterSeedSlack` | 10000 | Extra room reserved above the event count when seeding the counter, to absorb events written between the count and the deploy. |

## More detail

For the full operational picture, including how live subscriptions resume after the switch to position-based
catch-up and how rollback behaves, see the runbook at
[`doc/runbooks/position-backfill.md`](../../../doc/runbooks/position-backfill.md).
