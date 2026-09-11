# Runbook: repairing events damaged by `updateEvent` before 0.34.0

## Who this is for

You run an existing MongoDB event store, you are upgrading to 0.34.0 from 0.33.0 or earlier, and your application
called `EventStoreOperations.updateEvent` at some point while running one of those versions.

If you never called `updateEvent`, nothing in your store is damaged and you can stop reading.

If you are not sure, step 1's first query is indexed and answers in a second whether any event kept a damaged
position. A `0` there rules out that one kind of damage and nothing else, so run the rest of step 1 before you
conclude anything. Its second query is a collection scan, and even two zero results leave the cases in "The damage
this cannot find".

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

From 0.34.0 a store that writes position also runs the first of these itself when it starts, and logs a warning
naming this runbook when it finds something. It runs only the first, because that is the one that costs nothing, so
a silent startup rules out a damaged position rather than every kind of damage. Run the second query yourself. By
default a store that writes no position runs neither query, so run both yourself there unless you turn on the
setting below.

Set `EventStoreConfig.Builder.requireRepairedEvents(true)` if you would rather the store refused to start than kept
accepting conditional appends against a damaged event until you have run the repair. It is off by default, and it
runs the same first query, so it says nothing about the damage that query cannot see. It applies whether or not the
store writes position, so a store that turned position off over unpositioned history is refused too, at the cost of
a collection scan at startup because such a store has no position index for the query to read.

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

`result.unrecoverableEventCount()` is the number of events this run could not fully repair, carried across a resume
by the checkpoint. It is not the whole of what needs you, and a `0` is not proof that nothing does, so read it
together with the count above rather than on its own.
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

**`POSITION_ABOVE_COUNTER`.** The stored position is above the store's position counter, the highest position it ever
handed out, so the store never assigned it. A read clamps its upper bound to that same counter, so the event is as
invisible as one at or below zero, and a later append reaching that number would collide with it. Treat it the way
you treat `POSITION_LOST`. The tag array is repaired even so. If your store has no counter document there is no
ceiling to compare against and this is never reported.

**`UNREADABLE`.** The tool could not read the event well enough to repair it, which means its `dcbtags` was edited
outside Occurrent. The run continues past it, so one such event does not hold up the rest.

**Run the repair once more after any hand fix.** A `POSITION_ALREADY_TAKEN` event still has no tag array, because
the rejected update covered both fields together. Setting its position by hand makes it visible to position queries but
not to DCB reads, and it silences the startup warning, which then tells you nothing. A second run rebuilds the tag array.

**Write down the range this run reported before you start another one.** The finished-run log line names it,
`Repaired positions ranged from X to Y`, or says `No position was repaired` when the run restored none it could read.
`result.minRepairedPosition()` and `result.maxRepairedPosition()` hold the same two numbers, or both `null`. A run that
finishes deletes its checkpoint, so the next run starts with no range and reports only the positions it repaired
itself. Step 7 needs the range of every run you ran.

**Write down every position you set by hand as well, because the repair will usually never mention it again.**
Setting a position by hand is itself what stops an event looking damaged, so after the fix it matches neither half of
what the repair looks for and no later run reports it. A `POSITION_ALREADY_TAKEN` event with DCB tags can still come
back, because the rejected update left its tag array unwritten too. Working out which of your fixes fall that way buys
you nothing, so record every position you set. Step 7 needs them from you.

### 6. [you] Verify

```javascript
db.events.countDocuments({ position: { $type: "string" } })
db.events.countDocuments({ dcbtags: { $exists: true }, dcbTags: { $exists: false } })
```

Both should be `0`, except for the events step 5 left alone deliberately. Restart the application and confirm the
startup warning is gone.

### 7. [you] Recover consumers that read past a repaired position

Repair fixes documents. It does not touch any subscription's stored checkpoint, so a consumer that had already
resumed past a repaired event's position before the repair reached that event is still past it. A position-ordered
stream read and position-based catch-up both resume strictly after their checkpoint, so a repaired position below that
checkpoint is never delivered to them. This is a live consequence of the same damage "Why this is needed" describes
for a read, not a new kind of damage, and it applies to the same stores, one with stream position on or one reading
DCB. Time-ordered legacy catch-up is unaffected, because the event's time was never touched.

`result.minRepairedPosition()` and `result.maxRepairedPosition()`, the same two numbers the finished-run log line
prints, bound the position of every event that one run repaired and could read a position for.

A killed and resumed repair stores that range in its checkpoint the same way it stores the unrecoverable count, so the
numbers a resumed run reports cover the batches the interrupted call checkpointed as well as the ones that finished it.

The range has two limits, and both of them are why step 7 exists.

The first is that it does not reach past a run that finished. A run deletes its checkpoint once it has walked the
collection, so the next run starts with no range and reports only the positions it repaired itself. That is what the
second run in step 5 does. A first run repairs positions 100 to 5000, you hand-fix one event at 7000, and the second
run reports 7000 to 7000. If you only check consumers at or above 7000, every consumer that had already resumed past an
event between 100 and 5000 keeps missing it permanently. So use the range of every run you ran, not only the last one.
Each finished run logs its own outcome, either `Repaired positions ranged from X to Y` or `No position was repaired`,
so a range you did not write down at the time is still in the logs.

The second is that it does not cover the batch a process died in. The checkpoint is written once per batch, after every
event in that batch has already been updated, so a kill part way through one loses the positions it had just repaired.
Those events no longer look damaged, so no resumed run and no later run finds them again, and nothing records where
they were.

**If any run did not finish on its own, stop here and skip the comparison below.** The range it reports is then
incomplete in a way no number tells you about, so treat every consumer as possibly affected.

The criterion is a run you had to start again, not anything in the log. A run interrupted during its first batch wrote
no checkpoint at all, so the next one loads nothing and prints no `Resuming the repair of collection ...` line, while
still having lost the positions that first batch repaired. Silence there means the checkpoint was gone, not that
nothing was lost. You are the only record of which runs completed, so note it when one does not.

A repair walks `_id` order, which is not position order, so an event the lost batch repaired can sit anywhere in
history. It can sit below the minimum the run did report, and it can sit far below where a consumer had already read,
so neither that minimum nor the consumer's position at the time of the repair narrows anything. Everything the
consumer has already read is a candidate, so replay it from the beginning, or reconcile over its whole positioned
history up to its current checkpoint. The guidance further down says which of the two is safe for a given consumer.

What follows, up to and including the comparison against the lowest number, is for a repair where every run finished.
If one did not, pick up again at "Decide between replaying and reconciling", which applies either way.

A position in one of those ranges can be one the repair restored, or one that was already correct on an event only its
tag array needed rebuilding for, which is what a run after a hand-set fix on an event with DCB tags can look like.

Both numbers are `null` when a run touched nothing with a readable position, whether because it found nothing to
repair or because every event it touched had a position step 5 left you to deal with by hand.

A `null` from every run says nothing about the positions you set by hand in step 5, most of which no run ever reports.
So a repair is clear of consumer work only when every run reported `null` and you set no position by hand. Each one you
did set counts as a range of its own, covering that single position, whether or not a run happened to name it too.

Otherwise, take the lowest number across every run's minimum and every position you set by hand. A consumer whose
checkpoint sits below it has not reached a repaired event yet, so it will pick up every one of them on its own the next
time it resumes and needs nothing from you. One whose checkpoint sits at or above it may already have skipped one, and
that is the one to check next.

Read that checkpoint the way you would for any other purpose, a durable subscription's checkpoint storage collection,
or wherever a hand-rolled consumer keeps the position it last processed, and compare it to that lowest number.

Decide between replaying and reconciling before you touch anything, because a side effect a replay reruns cannot be
taken back afterward. Replaying a consumer redelivers every event from its restart point onward, not only the
repaired one.

Replaying is safe for a consumer that only overwrites or upserts its own state on each delivery, since writing the
same value twice produces the same document as writing it once. Rewind its checkpoint to below that lowest number, or
restart it from the beginning if that is simpler, and let it catch up. A repair where a run did not finish needs the
restart from the beginning rather than the rewind.

Replaying is not safe for a consumer that causes a side effect outside its own state which cannot run twice, sending
an email, charging a card, calling another system, since rewinding it reruns that side effect for every event since
the restart point, not only the repaired one. Reconcile that consumer instead of replaying it.

Read each range directly, one query per range rather than one query spanning all of them, so you do not read the
stretch between two of them that nothing touched. A position you set by hand is a range with the same number at both
ends:

```javascript
db.events.find({ position: { $gte: NumberLong(<minRepairedPosition>), $lte: NumberLong(<maxRepairedPosition>) } })
       .sort({ position: 1 })
```

If any run did not finish on its own there is no usable range, so read everything the consumer has already processed
instead:

```javascript
db.events.find({ position: { $lte: NumberLong(<the consumer's current checkpoint>) } }).sort({ position: 1 })
```

These queries return candidates rather than only repaired events. A range is a floor and a ceiling, so it also returns
every event sitting between the two that the run left alone because nothing was wrong with it.

The ranges can also overlap, since a position you set by hand can sit inside a run's range and two runs' ranges can
cover the same stretch, so one event can come back from more than one query. The sort is there because a consumer's
logic depends on position order and MongoDB returns no particular order without it.

Feed only the ones the consumer actually missed into its logic, once each however many queries returned them, by hand
or with a targeted script, leaving its checkpoint where it is. `NumberLong` matters once a store's position passes
2^53, since mongosh reads a bare number as a JavaScript double and a comparison against a `position` that large
silently rounds.

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
like any other. Three of those still get caught. A value another event already holds is refused by the unique index,
one that is zero or negative is reported as `POSITION_NOT_POSITIVE`, and one above the store's counter is reported as
`POSITION_ABOVE_COUNTER`. What is left is a positive value inside the assigned range that happens to be free, in a gap
in the sequence for instance, and nothing distinguishes it from the event's own. The tool converts it to a number,
counts a repair and reports nothing, because nothing in the store records what the position was.

This matters only if your update functions set `position`. If they did, step 6 passing is not the same as the
positions being right, and the events those functions touched need checking against an external record. If they left
`position` alone, which is the ordinary case, every restored position came from the event itself.

## Rollback considerations

There is nothing to roll back in the tool itself. A repaired event is what a running store would have written, so
0.33.0 reads it exactly as it reads any other event, and downgrading after a repair is safe.

Rolling back to 0.33.0 does bring back the defect. Calling `updateEvent` there damages events again, including ones
you just repaired.
