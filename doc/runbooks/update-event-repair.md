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

That second query is a collection scan, so run it during a quiet period on a large collection. If both return `0`, no damage this tool can find is in the
collection. That is not quite the same as not being affected. An update function that returned a replacement event
built from scratch, without the `dcbtags` extension, left a document that matches neither query and that nothing
can tell apart from an ordinary stream event. If you know you ran one of those over DCB events, read "The damage
this cannot find" below before you stop. Otherwise the rest of this runbook does not apply to you.

From 0.34.0 the store also runs the first of these itself when it starts, and logs a warning naming this runbook
when it finds something. It runs only the first, because that is the one that costs nothing, so a silent startup
rules out a damaged position rather than every kind of damage. Run the second query yourself.

### 2. [tool] Take a report

The report writes nothing, so it is safe against a live store. It returns two counts, how many events the repair
would touch and, separately, how many have DCB tags and no position at all.

It sizes a repair rather than predicting its outcome. A position another event already holds, one that is not a
number or is not positive, and a tag encoding that cannot be read all look like ordinary damage from the outside, so
step 4 finds them and this step does not. A low `eventsWithLostPosition` here is not a promise that step 5 will have
nothing in it.

```java
MongoDatabase database = mongoClient.getDatabase("my-database");
UpdateEventRepair repair = new UpdateEventRepair(database, "events", UpdateEventRepairOptions.defaults());
UpdateEventRepairReport report = repair.report();
```

Or from the command line, where `report` is the default:

```bash
java -jar occurrent-eventstore-mongodb-update-event-repair-<version>-cli.jar \
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
java -jar occurrent-eventstore-mongodb-update-event-repair-<version>-cli.jar \
  "mongodb://localhost:27017" my-database events repair
```

It can run against a live store, but every instance writing to that collection has to be on 0.34.0 first. The repair
walks `_id` order once and never goes back, so an instance still on 0.33.0 or earlier that calls `updateEvent` on an
event the walk has already passed damages it again, and the run finishes reporting a collection it has left broken.
Finish the deploy, or stop the writers, before you start.

Raise `throttleMillis` to leave more room for production traffic. Run one instance at a time, since two concurrent
runs share one checkpoint document and would resume from the wrong place.

Both the report and the repair read the whole collection, because finding an event whose tag array is missing cannot
use an index. Neither is expensive in writes, but on a large store give them a quiet period.

If the process is killed part way, run it again. It resumes from a checkpoint document, the events it already
repaired stay repaired, and it only touches events that still look damaged, so a repeated run cannot double-apply
anything.

### 5. [you] Deal with what could not be repaired

`result.eventsWithLostPosition()` is the number of events left with DCB tags and no position at all. It is asked of
the collection when the run finishes rather than tallied as the run goes, so it still counts an event whose tag array
an earlier run rebuilt. Rebuilding that array is what stops an event looking damaged, so without this number a
finished run could report a clean collection while a position was still gone.

`result.unrecoverableEventCount()` is the number of events holding damage the tool will not guess at.
`result.unrecoverableEvents()` names the findings by `_id`, and every one is also logged, so a truncated list is not
a lost report. The reasons below are independent, so one event can produce two findings and still count once. The
count is events, because that is the number of events you have to look at.

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
Worth investigating before you do anything to it. The event's tag array is repaired even so, since it does not
depend on the position.

**`POSITION_NOT_POSITIVE`.** The stored position is zero or negative, which no store assigns. Positions start above
zero and every position query reads `position > 0`, so writing the value back would count as a repair and leave the
event just as invisible. Only an update function that set `position` itself produces this, which makes the original
value gone rather than misread. Treat it the way you treat `POSITION_LOST`. The tag array is repaired even so.

**`UNREADABLE`.** The tool could not read the event well enough to repair it, which means its `dcbtags` was edited
outside Occurrent. The run continues past it, so one such event does not hold up the rest.

**Run the repair once more after any hand fix.** A `POSITION_ALREADY_TAKEN` event still has no tag array, because
the rejected update covered both fields together. Setting its position by hand makes it visible to position queries but
not to DCB reads, and it silences the startup warning, which then tells you nothing. A second run rebuilds the tag array.

### 6. [you] Verify

```javascript
db.events.countDocuments({ position: { $type: "string" } })
db.events.countDocuments({ dcbtags: { $exists: true }, dcbTags: { $exists: false } })
```

Both should be `0`, except for the events step 5 left alone deliberately. Restart the application and confirm the
startup warning is gone.

## The damage this cannot find

Two kinds of damage are invisible to both the tool and the queries above, and both come from an update function
that returned a replacement event built from scratch.

If it dropped the `dcbtags` extension, the stored document no longer looks like a DCB event. Nothing distinguishes
it from an ordinary stream event, so nothing counts it and nothing repairs it. If the extension was replaced rather
than dropped, the tool rebuilds the tag array from the replacement tags, because that is all the document has left.

If the event was a plain stream event and the replacement dropped its `position`, the document has neither a
`position` nor `dcbtags`, so it looks exactly like history written before position existed. This case reaches a store
that never used DCB. Your store warns about it as an un-backfilled event, and running the position backfill on it
assigns a position it never had, which nothing undoes. Every backfill message points at this runbook for that reason.
If you called `updateEvent` on 0.33.0 or earlier and you also have events without a position, decide from your own
records which is which before you backfill.

If you know you ran an update function that built replacement events from scratch over DCB events, compare against
an external record of what those events should be. The store cannot tell you.

## The repairs this cannot verify

A position the tool restores is the value the document holds, not one it can check. The old write-back kept whatever
position the update function returned, so a function that set `position` itself left that number behind as a string
like any other. Two of those still get caught. A value another event already holds is refused by the unique index and
reported as `POSITION_ALREADY_TAKEN`, and zero or a negative value is reported as `POSITION_NOT_POSITIVE`. A positive
value that happens to be free, in a gap in the sequence for instance, is indistinguishable from the event's own. The
tool converts it to a number, counts a repair and reports nothing, because nothing in the store records what the
position was.

This matters only if your update functions set `position`. If they did, step 6 passing is not the same as the
positions being right, and the events those functions touched need checking against an external record. If they left
`position` alone, which is the ordinary case, every restored position came from the event itself.

## Rollback considerations

There is nothing to roll back in the tool itself. A repaired event is what a running store would have written, so
0.33.0 reads it exactly as it reads any other event, and downgrading after a repair is safe.

Rolling back to 0.33.0 does bring back the defect. Calling `updateEvent` there damages events again, including ones
you just repaired.
