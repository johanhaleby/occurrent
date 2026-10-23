# 147. A DCB consistency token belongs to the criteria it was read with

Date: 2026-10-23

## Status

Accepted. Decides [#1228](https://github.com/johanhaleby/occurrent/issues/1228) and the first part of
[#1229](https://github.com/johanhaleby/occurrent/issues/1229). Amends
[ADR 21](0021-dcb-write-path-query-scoped-concurrency.md),
[ADR 31](0031-capture-dcb-consistency-token-in-a-single-read.md), [ADR 53](0053-dcb-api-freeze-consistency.md) and
[ADR 88](0088-what-a-dcb-append-condition-guarantees.md).

The second part of #1229 and [#1231](https://github.com/johanhaleby/occurrent/issues/1231) are not decided here,
because both depend on how the MongoDB stores reserve positions.

## Context

A command on a DCB consistency boundary reads the events matching some criteria and gets a `DcbConsistencyToken` back
on the `DcbEventStream`. It then appends with a `DcbAppendCondition` that holds criteria and that token, and the store
is meant to refuse the append with `DcbAppendConditionNotFulfilledException` when an event matching the condition's
criteria was committed after the read.

In 0.34 the token is `record DcbConsistencyToken(long value)`. The in-memory store puts its read head in it and looks
for a matching event above that position. The three MongoDB stores put the sum of the versions of the read criteria's
marker documents in it (ADR 21, ADR 31), and the append compares that sum with the sum over the condition's criteria.
That design has two problems.

**The token doesn't know which criteria it was read with** (#1228). Nothing stops a condition from using other criteria
than the read, and on MongoDB the check then compares sums over different markers. Say a command reads `tags(a)` while
`tag:a` is at 3, so the token is 3, and then appends with `tags(b)`. Another command appends an event tagged `b` in
between and takes `tag:b` from 2 to 3. The check reads 3 for `tag:b`, finds it equal to the token, and lets the append
through although an event matching its criteria was committed after the read. The in-memory store compares positions
instead, so the same code is refused there.

**A type in the criteria makes the boundary store-wide** (#1229). `DcbMarkerModel.queryMarkerKeys` gives every
criteria item a `tag:` key for each of its tags and a `type:` key for each of its types, and every append increments
`type:X` for each type it writes. A command that reads `type("InvoiceIssued").tags(invoice1)` therefore conflicts with
an append of an `InvoiceIssued` tagged `invoice2`, which doesn't match it. The two appends also increment the same
`type:InvoiceIssued` document, so MongoDB doesn't let them commit at the same time. ADR 21 accepted false conflicts
between appends that share a tag or a type, as the boundary the caller chose. For an item with tags, though, the type is shared with every event of that type in the store, and the boundary
the caller chose is the tag.

## Decision

### The token holds the criteria it was read with

`DcbConsistencyToken` becomes a final class with two private fields, the criteria the read used and a version whose
meaning the store defines. The version is the read head in memory and the marker sum on MongoDB. A store creates a
token with `DcbConsistencyToken.of(DcbCriteria, long)`, and `criteria()` returns the read criteria. `version()` is
there for store implementations, the way `value()` is in 0.34, and nothing outside a store should read or keep it. Two
tokens are equal when their criteria are the same, as defined in the next section, and their versions are equal.

These are removed:

- The public constructor, `DcbConsistencyToken.of(long)` and `value()`.
- `DcbEventStream(List, long)` and `DcbDomainEventStream(List, long)`, because they create a token with no criteria.

The token is a class and not a record so that a field can be added later as a new factory overload, which a record's
public canonical constructor and accessors don't allow without breaking. A read position, which #1231 may need, would
be such a field. It isn't added now, since what it should mean depends on the same decision about reserving positions.

### When two criteria are the same

Two criteria are the same when they hold the same set of alternatives:

- `DcbCriteria.all()` is the same only as `DcbCriteria.all()`.
- A `DcbCriterion` is the same as an `anyOf(..)` holding only that criterion.
- Two `anyOf(..)` are the same when they hold the same criteria in any order, so `anyOf(a, b)` is the same as
  `anyOf(b, a)`.

Criteria that select the same events but are built differently aren't the same. `types("A", "B")` and
`anyOf(type("A"), type("B"))` are an example. `DcbCriteria.equals` doesn't change, and it still compares an `anyOf(..)`
in order. `DcbSnapshotKeys.canonicalKey` doesn't change either, so no stored snapshot key moves.

### A condition refuses a token with other criteria

`DcbAppendCondition`'s constructor throws `IllegalArgumentException` when the token's criteria aren't the same as the
condition's criteria. Every factory goes through the constructor, so the refusal happens before any store is called.
No store reserves positions or writes markers for it, and a `DcbEventStore` of your own gets the check without doing
anything. `GenericDcbApplicationService` retries only `DcbAppendConditionNotFulfilledException` on both stacks, so the
`IllegalArgumentException` reaches the caller on the first attempt.

- `DcbAppendCondition.failIfEventsMatch(DcbConsistencyToken)` is added and takes its criteria from the token. Both
  `GenericDcbApplicationService` classes and the `InvoiceService` example switch to it.
- `failIfEventsMatch(DcbCriteria, DcbConsistencyToken)` is deprecated, and its javadoc points at the one-argument
  form. It keeps working and refuses a token with other criteria the same way.
- `wholeStoreLock(DcbConsistencyToken)` stays, since its name says the condition covers the whole store. It refuses a
  token whose criteria aren't `DcbCriteria.all()`.

The rule is the same on every store. Code that reads with one criteria and appends with another, passing the token
the read returned, throws on the in-memory store as it does on MongoDB, so a test against the in-memory store can catch
it before production.

### Query markers for an item with tags

`DcbMarkerModel.queryMarkerKeys` gives an item that has at least one tag only its `tag:` keys. An item without tags gets
its `type:` keys as before, and `DcbCriteria.all()` keeps the `all` key. `eventMarkerKeys` doesn't change, so every
append still increments a marker for each tag and each type of its events.

The marker check stays sound for an item with tags. An event matches the item only when it has all of the item's
tags, so an append of a matching event increments every one of the item's `tag:` markers. A token over those markers
moves with every such append. A conditional append on the item increments the same markers from its query keys, so
the two appends share a document and MongoDB lets only one of them commit, which is the argument ADR 21 rests on. An
item without tags keeps its type keys and ADR 21's argument unchanged, and excluded types never had keys.

Conflicts between criteria with tags and appends that only share a type with them go away. The writes don't. Every
append still increments `type:X`, so each type marker is still written by every append of that type in the store,
which is the second part of #1229.

### Invariants

I1 holds for any token, and I3 is about markers alone. I2 holds only when all of these are true:

- `t` is the token that a read with criteria `Cr` on a store instance `S` returned, used as returned and not rebuilt
  with `of` outside the store.
- The append goes to `S`, in the process and the Occurrent version that did the read.
- No event in `S` was deleted, or changed with `updateEvent`, between the read and the append. The in-memory store
  checks only the events it still holds, so a matching event deleted after the read no longer counts there. The
  MongoDB stores don't lower a marker when an event is deleted or changed, so they still refuse.

The invariants:

- **I1.** For a condition with criteria `Ca` and a token `t` whose criteria are `Ct`, the condition can't be created
  when `Ca` isn't the same as `Ct`. It throws `IllegalArgumentException` and no store is called. When `t` came from a
  read, `Ct` is that read's `Cr`.
- **I2.** When `Ca` is the same as `Cr`, `Cr` isn't `DcbCriteria.all()`, and an event matching `Cr` was committed to
  `S` after the read, `S` refuses the append with `DcbAppendConditionNotFulfilledException`. For `DcbCriteria.all()`
  the MongoDB stores detect only other whole-store appends, as ADR 30 decided.
- **I3.** For an item `i` with tags and an event `e` matching `i`, the markers an append of `e` increments include
  every query marker of `i`. For an item `i` without tags, they include at least one query marker of `i`.

The check binds a token to its criteria and to nothing else. It doesn't know which store instance, process or
Occurrent version read the token, so I2 promises nothing for these, and each of them can let an append through that a
matching event should have refused:

- A token read on one store instance and used on another, when the criteria match and both stores happen to be at the
  same version.
- A token built with `of` outside a store, for example `of(tags(b), token.version())` from a token read with `tags(a)`. `of` is
  public because the stores live in other modules than the token, and a `DcbEventStore` of your own needs it too.
- A version saved and turned back into a token later, which the next section shows for a value saved under 0.34.

### Upgrading

Nothing changes on disk. The marker documents, their ids and what increments them stay the same, so 0.34 and 0.35
nodes can append to the same store. A conditional append from a 0.34 node still increments type markers that 0.35
doesn't read for criteria with tags. Marker versions only go up, so an extra increment can make a check refuse an
append and the command retry, and it can't make a check let one through. Going back to 0.34 is safe for the data.

A token saved under 0.34 falls outside the conditions I2 needs. A token value from 0.34 is a sum over tag and type
markers, while 0.35 sums tag markers only, so a value saved under 0.34 can let an append through on 0.35 that should
have been refused. Say a 0.34 read of `type("X").tags(t)` sums 2 from `tag:t` and 3 from `type:X`, so the value is 5.
After the upgrade three appends tagged `t` take `tag:t` from 2 to 5, and one of them is an `X`. The 0.35 check sums 5
and lets the append through. The migration guide tells anyone who saved a token value, in an ETag or a queued command
for example, to read again instead. Occurrent itself doesn't save a token.

## Alternatives considered

**Allow narrower criteria where the store can check them** (option 2 in #1228). The same code would then pass in memory
and throw on MongoDB.

**Allow an item with tags to keep only some of its types, or to exclude more types.** Once query markers for an item
with tags are its tags only, such a condition has the same markers as the read, so every store could check it. It
would write the marker model's structure into the public API, though, and it removes no conflict on MongoDB. Allowing
it later turns a refusal into an acceptance, which doesn't break anyone, while allowing it now and refusing it later
would.

**Fit the criteria into the `long`, as a hash or as bits.** A hash collision can let an append through, which is the
defect this ADR fixes. Bits for the criteria leave no room in 64 bits for anything else the token may need.

**Add the read position to the token now.** What it should mean depends on how positions get reserved, and it would
repeat `DcbEventStream.lastSequencePosition()` until then.

**Give the token a string form** for sending it to a client and back. Nothing in the repository needs one, and a stable
encoding is a wire format to keep compatible. The final class lets it be added later.

**Let only the store instance that read a token accept it.** Each store would get its own token type, with an id for
the store instance, and check that a condition holds one of its own. That would cover a token used on another store
instance. A `DcbEventStore` of your own still needs a public way to create a token, though, so a caller can still build
one by hand. Every store implementation would need the token type and the check, and the TCK would need tests for it.
#1228 is about code that reads with one criteria and appends with another by mistake, and the criteria in the token
catch that.

## Consequences

- A condition built from a read's token and other criteria than the read's throws, on every store. A token used on
  another store instance, built with `of` outside a store, or saved and restored can still let an append through, as a hand-made
  `long` can in 0.34.
- On the MongoDB stores, criteria with tags no longer conflict with an append that only shares a type with them. The
  in-memory store already behaved this way, so the TCK asserts the same answer for that case on every store.
- `DcbConsistencyToken` becomes a class and the two stream constructors are removed, which breaks store
  implementations and code that creates tokens or streams. The refusal breaks code that appends with other criteria
  than it read with. There is no OpenRewrite recipe for either, because the replacement needs the read criteria,
  which the call site doesn't always have.
- The deprecation warning shows up wherever a caller passes the criteria and the token separately.
- ADR 31 and ADR 53 decided that the token stays a `long`. This ADR reverses that, while ADR 31's single read stands.
- The TCK gets tests for the refusal on every store. It also gets a test on every store that reads
  `type("X").tags(a)`, appends an `X` tagged only `b`, and then appends with the read's token, which succeeds. Its
  control appends an `X` tagged `a` instead, and that append is refused. A store that still reads the type marker fails
  the first test, and a store that checks no marker fails the control.
- `DcbConcurrencyConformance` gets two cases where a condition on a type and a tag races a condition on that type
  alone, and one on that tag alone, over the same event. That event matches both conditions, so these races show that
  the check stays sound and not that the type conflict is gone.
