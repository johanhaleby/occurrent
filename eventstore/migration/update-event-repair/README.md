# Update event repair

Repairs events that Occurrent's own `updateEvent` damaged in version 0.33.0 or earlier, so they become visible to
DCB reads and to position-ordered reads again.

Maven coordinates: `org.occurrent:occurrent-eventstore-mongodb-update-event-repair`.

Upgrading to 0.34.0? Start with the [upgrade guide](../../../doc/migration/upgrading-to-0.34.0.md) and the
[operational runbook](../../../doc/runbooks/update-event-repair.md).

## The problem

Up to and including 0.33.0, `updateEvent` rebuilt the stored document through the stream-only mapper. That mapper
writes `position` through the general CloudEvent extension writer, which has no `Long` overload, so `position` came
back as a string instead of a number. It also does not know about the indexed `dcbTags` array, so that array was
dropped. Version 0.34.0 fixes both. It does not repair events that were already damaged.

A damaged event is missing from queries, without any error:

- DCB reads, `exists` and `count` skip it.
- Position-ordered stream reads skip it, forwards and backwards. This affects a store with stream position on even
  if it never used DCB.
- Position-based catch-up skips it, so a projection rebuilt from history can be missing events.
- The conflict query behind a conditional append skips it, so an append that should have been refused is accepted.

You only need this tool if you called `updateEvent` while running 0.33.0 or earlier. If you never called it, nothing
in your store is damaged and there is nothing to do.

## How to tell whether you are affected

Cheapest first, and none of the three writes anything.

**Run one query.** This uses the `position` index and costs nothing on a store that was never damaged.

```javascript
db.events.countDocuments({ position: { $type: "string" } })
```

`0` means no event kept a damaged position. Replace `events` with your event collection name.

**Read your startup log.** From 0.34.0, a store that writes position runs that same query when it starts and logs a
warning naming this tool. A store with no damage logs nothing.

The startup check runs only the query above, because it is the one that costs nothing. It does not look for an event
whose tag array is missing but whose position is fine, since that needs a collection scan. So a silent startup rules
out a damaged position, not every kind of damage. The tool checks both.

**Ask the tool.** `report()` writes nothing and returns two counts, how many events the repair would touch and how
many have DCB tags and no position at all. It sizes a repair rather than predicting its outcome. A position another
event already holds, one that is not a number or is not positive, and a tag encoding that cannot be read all look
like ordinary damage from the outside, so a run finds them and the report does not.

```java
MongoDatabase database = mongoClient.getDatabase("my-database");
UpdateEventRepair repair = new UpdateEventRepair(database, "events", UpdateEventRepairOptions.defaults());
UpdateEventRepairReport report = repair.report();
```

## What the tool does

`run()` rebuilds `position` from the string still in the document, and rebuilds the `dcbTags` array from the
`dcbtags` CloudEvent extension, which is a string and so survived the coercion. It reuses the store's own
position mapper and tag decoder, so a repaired event is what a running store would have written.

`run()` is:

- **Idempotent.** It only touches events that still look damaged, so running it twice is safe. A second run repairs
  nothing.
- **Resumable.** It records the last processed `_id` in a checkpoint document (`_id: "updateEventRepair"` in the
  `<events>_update_event_repair_checkpoint` collection). If the process is killed, running it again continues from
  where it stopped, and the events it already repaired stay repaired. On completion the checkpoint document is
  removed.
- **Atomic across what it can restore.** The fields it can restore are written in one update, so a partly applied
  write cannot happen. That is not a promise that both fields always come back. When one is beyond saving and the
  other is not, the recoverable one is restored and the other is reported, so an event can end up with its position
  back and its tag array still missing. Only a rejected write keeps both exactly as they were found.
- **Throttled.** You can make it sleep between batches so it does not compete with production traffic.

Get every instance writing to the collection onto 0.34.0 before you start. The repair walks `_id` order once and
never goes back, so an instance still on 0.33.0 or earlier that calls `updateEvent` on an event the walk has already
passed damages it again, and the run finishes reporting a collection it has left broken.

Run one instance at a time. Two concurrent runs share one checkpoint document, and the first to finish deletes it
while the other is still going, so a later resume would start from the wrong place. If you run this as a Kubernetes
Job, make sure a retry cannot overlap the run it is retrying.

`report()` writes nothing, but it is not cheap. Finding an event whose tag array is missing cannot use an index, so
both `report()` and `run()` read the whole collection. On a large store, run them during a quiet period.

## What the tool cannot do

The repair rebuilds an event from what its document still holds. Where the old write-back destroyed the only copy of
a value, that value is gone, and the tool reports the event rather than inventing one. `UpdateEventRepairResult`
lists the findings by `_id`, and every one is also logged. One event can produce two of them, since the reasons below
are independent, so `unrecoverableEventCount()` counts events rather than findings.

- **A position that was dropped entirely.** An update function that returned an event built from scratch kept
none of the original's extensions, so no position was stored. The tool will not assign a fresh one. A position
  invented in `_id` order would look plausible and be wrong, because a consumer holding a checkpoint from before the
  damage would then disagree with the store. Reported as `POSITION_LOST`. The tag array is still rebuilt.
- **A position another event already holds.** Two events claim one position and the unique index refuses the second.
  Nothing in either document says which one is entitled to it. Reported as `POSITION_ALREADY_TAKEN`, and the event
  is left exactly as it was found.
- **A `position` string that is not a number.** No known path produces this. Reported as `POSITION_NOT_A_NUMBER`.
- **A `dcbtags` that is not a readable string.** Another type, an explicit null, or a value that does not decode to a
  tag set. Nothing Occurrent writes produces any of them, so it points at a document edited outside the library.
  Reported as `UNREADABLE`. The position is still restored, since it does not depend on the tags.
- **A `position` string holding zero or a negative number.** No store assigns one. Positions start above zero and
  every position query reads `position > 0`, so writing such a value back would count as a repair and leave the event
  just as invisible. Only an update function that forged the position produces this. Reported as
  `POSITION_NOT_POSITIVE`. The tag array is still rebuilt.
- **A `position` string above the store's position counter.** The counter is the highest position the store ever
  handed out, and a read clamps its upper bound to that same counter, so a value above it is as invisible as one at
  or below zero, and a later append reaching that number would collide with it. Only an update function that forged
  the position produces this. Reported as `POSITION_ABOVE_COUNTER`. The tag array is still rebuilt. A store with no
  counter document has no ceiling to compare against, so nothing is reported on that ground.

`eventsWithLostPosition()` on the result is separate from all of these. It is asked of the collection when the run
finishes rather than tallied as the run goes, so it still counts an event whose tag array an earlier run rebuilt.
Rebuilding that array is what stops an event looking damaged, so without this number a finished run could report a
clean collection while a position was still gone.

A position the tool does restore is the value the document holds, not one it can check. The old write-back kept
whatever position the update function returned, so a forged one was stored as a string like any other. The cases
above catch a forged value that another event already holds, one that is zero or negative, and one above the store's
counter. What is left is a positive value inside the assigned range that happens to be free, in a gap in the sequence
for instance, and nothing distinguishes it from the event's own. The tool converts it to a number and counts a
repair. If you ran an update function that set `position` itself, the tool
cannot tell you whether the value it restored is the one the event had, and nothing in the store can.

Two kinds of damage are invisible to the tool, both from an update function that returned a replacement event built
from scratch.

- It dropped the `dcbtags` extension, so the document no longer looks like a DCB event and nothing distinguishes it
  from an ordinary stream event.
- It dropped the `position` of a plain stream event, which never had `dcbtags` to begin with, so the document is
  indistinguishable from history written before position existed. A store that writes position warns about it as an
  un-backfilled event, and running the position backfill on it would give it a position it never had. That is why
  every backfill message points at the repair runbook.

Neither is counted or repaired. If the `dcbtags` extension was replaced rather than dropped, the tool rebuilds the
tag array from the replacement tags, since that is all the document has.

## How to run the module

### From Java

```java
try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
    MongoDatabase database = mongoClient.getDatabase("my-database");

    UpdateEventRepairOptions options = UpdateEventRepairOptions.defaults()
            .withBatchSize(1000)
            .withThrottleMillis(200);

    UpdateEventRepair repair = new UpdateEventRepair(database, "events", options);
    UpdateEventRepairResult result = repair.run();
}
```

### As a standalone command

Build the runnable jar:

```bash
mvn -pl eventstore/migration/update-event-repair -am package -DskipTests
```

That produces `occurrent-eventstore-mongodb-update-event-repair-<version>-cli.jar` in the module's `target/` directory, a
single jar with all dependencies bundled. Run it with `<mongoUri> <database> <collection> [report|repair]`. The
command defaults to `report`, which changes nothing.

```bash
java -jar eventstore/migration/update-event-repair/target/occurrent-eventstore-mongodb-update-event-repair-<version>-cli.jar \
  "mongodb://localhost:27017" my-database events report
```

Pass `repair` once you have read the report and want the events fixed. A repair that could not fix every event
exits with status `2`, so a job scheduler does not record it as a clean run when a person still has to look at
something. For a large collection, run it as a long-lived job (a Kubernetes Job or a one-off ECS task) rather than a
script on your laptop.

## Options

| Option | Default | What it does |
| --- | --- | --- |
| `batchSize` | 500 | How many events to read and repair per batch. Larger batches finish faster but hold more in memory each iteration. |
| `throttleMillis` | 0 | How long to sleep between batches. Raise this to leave more room for production traffic. `0` means no pause. |
| `maxReportedUnrecoverable` | 1000 | How many unrepairable findings the result keeps. One event can produce two, so this bounds findings rather than events. The count of events is always complete and every finding is logged, so the cap only bounds the returned list. |

## More detail

The decision behind this tool, including why a store never repairs stored events by itself and when the startup
warning may be removed, is [ADR 136](../../../doc/architecture/decisions/0136-a-store-reports-damage-it-caused-and-never-repairs-it-by-itself.md).
