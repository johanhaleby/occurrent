# Runbook: repairing events damaged by `updateEvent` before 0.34.0

## Who this is for

You run an existing MongoDB event store, you are upgrading to 0.34.0 from 0.33.0 or earlier, and your application
called `EventStoreOperations.updateEvent` at some point while running one of those versions.

If you never called `updateEvent`, nothing in your store is damaged and you can stop reading. The check in step 1
takes a second and settles it either way.

## Why this is needed

Up to and including 0.33.0, `updateEvent` rebuilt the stored document through the stream-only mapper, which writes
`position` through the general CloudEvent extension writer. That writer has no `Long` overload, so `position` came
back as a string instead of a number, and the indexed `dcbTags` array was dropped. Version 0.34.0 fixes the write
path. It does not repair events that were already damaged.

MongoDB compares values within a type, so a string `position` matches neither end of a numeric range. A damaged
event is therefore missing from:

- DCB reads, `exists` and `count`.
- Position-ordered stream reads, forwards and backwards. This affects a store with stream position on even if it
  never used DCB.
- Position-based catch-up, so a projection rebuilt from history can silently be missing events.
- The conflict query behind a conditional append, so an append that should have been refused is accepted instead.

None of it raises an error. The event is just not there.

## The repair sequence

Steps marked **[you]** are manual. Steps marked **[tool]** are calls into the
`occurrent-eventstore-mongodb-update-event-repair` module.

### 1. [you] Find out whether you are affected

```javascript
db.events.countDocuments({ position: { $type: "string" } })
```

Replace `events` with your event collection name. This uses the `position` index and is cheap even on a large
collection.

`0` here does not quite settle it on its own. An event whose position was dropped entirely also counts as damaged
and has no `position` field, so also run:

```javascript
db.events.countDocuments({ dcbtags: { $exists: true }, dcbTags: { $exists: false } })
```

That second query is a collection scan, so run it during a quiet period on a large collection. If both return `0`, you are
not affected and the rest of this runbook does not apply.

From 0.34.0 the store also checks the first of these itself when it starts, and logs a warning naming this runbook
when it finds something. A store with no damage logs nothing.

### 2. [tool] Take a report

The report counts the damage and, separately, the part of it that cannot be repaired. It writes nothing, so it is
safe against a live store.

```java
MongoDatabase database = mongoClient.getDatabase("my-database");
UpdateEventRepair repair = new UpdateEventRepair(database, "events", UpdateEventRepairOptions.defaults());
UpdateEventRepairReport report = repair.report();
```

Or from the command line, where `report` is the default:

```bash
java -jar eventstore-mongodb-update-event-repair-<version>-cli.jar \
  "mongodb://localhost:27017" my-database events report
```

`eventsWithLostPosition` is the number worth pausing on. Those events cannot get their position back, and step 5
covers what to do about them.

### 3. [you] Take a backup

The repair writes to your event collection. Take whatever backup you would take before any other write to it. This
is ordinary caution rather than a specific known risk, and the tool has no undo.

### 4. [tool] Run the repair

```java
UpdateEventRepairOptions options = UpdateEventRepairOptions.defaults()
        .withBatchSize(1000)
        .withThrottleMillis(200);
UpdateEventRepairResult result = new UpdateEventRepair(database, "events", options).run();
```

Or:

```bash
java -jar eventstore-mongodb-update-event-repair-<version>-cli.jar \
  "mongodb://localhost:27017" my-database events repair
```

It can run against a live store. Raise `throttleMillis` to leave more room for production traffic.

If the process is killed part way, run it again. It resumes from a checkpoint document, the events it already
repaired stay repaired, and it only touches events that still look damaged, so a repeated run cannot double-apply
anything.

### 5. [you] Deal with what could not be repaired

`result.unrecoverableEventCount()` is the number of events holding damage the tool will not guess at.
`result.unrecoverableEvents()` names them by `_id`, and every one is also logged, so a truncated list is not a lost
report.

**`POSITION_LOST`.** The event's position was never stored, so there is nothing to restore it from. The tool does
not assign a new one, because a position invented in `_id` order would look right and be wrong, and any consumer
holding a checkpoint from before the damage would then disagree with the store. The event's tag array is repaired,
but the event stays outside position-ordered reads. If you know from your own records what the position was, set it
by hand. Otherwise treat the event as lost from the position axis and decide whether your projections need
rebuilding from a different source.

**`POSITION_ALREADY_TAKEN`.** Two events claim one position and the unique index refuses the second. Nothing in
either document says which one is entitled to it. Look at both events and decide, then set the loser's position by
hand or accept that it stays outside position-ordered reads.

**`POSITION_NOT_A_NUMBER`.** No known Occurrent path produces this, so it points at damage from somewhere else.
Worth investigating before you do anything to it.

### 6. [you] Verify

```javascript
db.events.countDocuments({ position: { $type: "string" } })
db.events.countDocuments({ dcbtags: { $exists: true }, dcbTags: { $exists: false } })
```

Both should be `0`, except for the events step 5 left alone deliberately. Restart the application and confirm the
startup warning is gone.

## The damage this cannot find

One kind of damage is invisible to both the tool and the queries above. If an update function returned a replacement event
built from scratch, without the `dcbtags` extension, the stored document no longer looks like a DCB event at all.
Nothing distinguishes it from an ordinary stream event, so nothing counts it and nothing repairs it. If the
extension was replaced rather than dropped, the tool rebuilds the tag array from the replacement tags, because that
is all the document has left.

If you know you ran an update function that built replacement events from scratch over DCB events, compare against
an external record of what those events should be. The store cannot tell you.

## Rollback considerations

There is nothing to roll back in the tool itself. A repaired event is what a running store would have written, so
0.33.0 reads it exactly as it reads any other event, and downgrading after a repair is safe.

Rolling back to 0.33.0 does bring back the defect. Calling `updateEvent` there damages events again, including ones
you just repaired.
