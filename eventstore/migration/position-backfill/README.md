# Position backfill

Adds the global `position` field to events that were written before `position` existed, so an existing MongoDB
event store can turn stream position on without losing sight of its old events.

Maven coordinates: `org.occurrent:eventstore-mongodb-position-backfill`.

## The problem

Occurrent stamps a global, always-increasing `position` on every event. Position is what lets catch-up
subscriptions replay history in a fixed order that does not depend on wall-clock time, and it lets stream and DCB
consumers order events on one shared axis.

Events written by an older version have no `position` field. Until you add it, those events are invisible to
position-ordered reads and to position-based catch-up, and the store logs a warning on every startup pointing at
this migration.

You only need this tool if you have an existing store that already contains events. A brand-new or empty
deployment gets `position` on every event from the start and needs no backfill.

## How it works

The tool reuses the store's own counter and its position mapper, so the events it backfills look exactly like the
events a running store writes. It walks the collection in `_id` order (the primary key, always indexed) in batches
and sets `position` on each event that does not have one yet.

Three properties make it safe to run against a live system:

- **Idempotent.** It only touches events that are missing `position`, so running it twice is safe. A second run
  reports zero events positioned.
- **Resumable.** It records the last processed `_id` in a checkpoint document. If the process is killed, running
  it again continues from where it stopped instead of starting over.
- **Throttled.** You can make it sleep between batches so it does not compete with production traffic.

## The safe upgrade sequence

Do these in order. The important part is to have the index in place before you deploy, and to not rely on
position until the backfill has finished. Getting the order wrong can leave old events temporarily invisible to
position reads, but it never loses or corrupts event data.

### 1. Create the `position` index

Create the index before deploying the new version, so the new queries and the backfill's own scans can use it from
the start.

```javascript
db.events.createIndex({ position: 1 }, { background: true })
```

**On MongoDB Atlas (or any replica set):** build it as a rolling index so it does not block writes while it is
being built. Atlas builds indexes in a rolling fashion across the replica set for you. The `{ background: true }`
option above is the equivalent when you run the command yourself.

Replace `events` with your event collection name.

### 2. Seed the counter

Before deploying, move the position counter above the number of events already in the collection, plus a slack
margin. This makes sure that once the new version starts writing, its live positions land above every position the
backfill will later hand to the old events, so the two never collide.

```java
MongoDatabase database = mongoClient.getDatabase("my-database");
PositionBackfill backfill = new PositionBackfill(database, "events", PositionBackfillOptions.defaults());
long seededTo = backfill.seedCounter();
```

This is safe to run more than once. If the counter is already high enough, it is left alone.

### 3. Deploy the version that writes position

From now on, new events get a `position` when they are written. Catch-up subscriptions keep using the older
time-based path until the backfill has finished and you have confirmed full coverage, so nothing is missed in the
meantime. The startup warning keeps showing until no un-positioned events remain.

### 4. Run the backfill

```java
PositionBackfillResult result = backfill.run();
```

`run()` seeds the counter (step 2 again, which is a no-op if you already did it) and then positions every remaining
old event, resuming from the checkpoint if one exists. It blocks until it is done.

For a large collection this can take a while, so run it as a long-lived job (for example a Kubernetes Job or a
one-off ECS task) rather than a script on your laptop. Use the options below to keep it gentle on production.

### 5. Check that every event has a position

```javascript
db.events.countDocuments({ position: { $exists: false } })
```

This should return `0`. Restarting a store instance is also a check: if no un-positioned events remain, the
startup warning is gone.

### 6. Done

Once the count is `0`, position-based catch-up is safe for this collection. The store switches to it on its own,
so there is nothing else to turn on.

## How to run the module

### From Java (recommended)

Call it directly. Use the constructor when you want to control batching and throttling:

```java
try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
    MongoDatabase database = mongoClient.getDatabase("my-database");

    PositionBackfillOptions options = PositionBackfillOptions.defaults()
            .withBatchSize(1000)
            .withThrottleMillis(200);

    PositionBackfill backfill = new PositionBackfill(database, "events", options);
    PositionBackfillResult result = backfill.run();

    System.out.println(result);
}
```

### As a standalone command

The module also ships a `main` method that takes `<mongoUri> <database> <collection>` and runs with default
options:

```
org.occurrent.eventstore.mongodb.migration.positionbackfill.PositionBackfill
```

Run it the way you run any main class, for example from your IDE, or put the module and its runtime dependencies on
the classpath and run:

```bash
java -cp "<module-and-dependencies>" \
  org.occurrent.eventstore.mongodb.migration.positionbackfill.PositionBackfill \
  "mongodb://localhost:27017" my-database events
```

The build does not produce a single bundled jar, so you assemble the classpath yourself, or just call it from Java
as shown above.

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
