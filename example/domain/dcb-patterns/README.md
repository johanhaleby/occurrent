# DCB patterns catalog

A catalog of the patterns at [dcb.events/examples](https://dcb.events/examples/), each as a small, tested,
self-contained vignette. Kotlin, no Spring, no Docker: every vignette builds its own
`InMemoryEventStore` (with the DCB capability, no extra configuration needed) plus a `GenericDcbApplicationService`
in its test, in the style of `appointment-scheduling`'s `Bootstrap` but without the storage or web layer -
this module is a catalog, not an application.

## Where all seven dcb.events examples live in this repository

| dcb.events example | Where it lives here |
|---|---|
| Course subscriptions | `course-enrollment` and `hotel-booking` (the two Spring/`dcbDecider` examples) |
| Event-sourced aggregate | `word-guessing-game`'s DCB modules |
| Unique username | [`uniqueusername`](src/main/kotlin/org/occurrent/example/domain/dcbpatterns/uniqueusername) |
| Prevent record duplication | [`idempotency`](src/main/kotlin/org/occurrent/example/domain/dcbpatterns/idempotency) |
| Dynamic product price | [`dynamicproductprice`](src/main/kotlin/org/occurrent/example/domain/dcbpatterns/dynamicproductprice) |
| Double opt-in | [`optintoken`](src/main/kotlin/org/occurrent/example/domain/dcbpatterns/optintoken) |
| Invoice numbers | [`invoicenumber`](src/main/kotlin/org/occurrent/example/domain/dcbpatterns/invoicenumber) |

## The five vignettes here

- **`uniqueusername`** - global uniqueness with a retention period. The boundary is a single tag, the
  username itself. A closed account's name stays reserved for 30 days before it can be reclaimed.
- **`idempotency`** - prevent record duplication. The boundary is an idempotency token, and replaying the
  same command is a no-op (`decide` returns no events) instead of an error.
- **`dynamicproductprice`** - a price change grace period. An order is accepted at the current price, or at
  the just-superseded price if it changed less than 10 minutes before the order.
- **`optintoken`** - double opt-in with a consume-once, expiring one-time password. A second confirmation,
  or one that arrives after the TTL, is rejected.
- **`invoicenumber`** - a gapless, monotonically increasing sequence, done at the event-store level instead
  of through a `DcbDecider` (see below for why).

Each vignette is one decider (or, for `invoicenumber`, one service), plus a JUnit 5 + AssertJ test covering
both the happy path and the rejection/conflict path.

## Two idioms worth calling out

**Global uniqueness and idempotency are just a scoped append condition.** There is no separate "uniqueness
index" or lock table: the DCB boundary (a tag, or a small set of tags) is exactly the set of events that
could conflict with the command, and the append condition rejects the write if any of them showed up since
the read. `uniqueusername` and `idempotency` are the same mechanism applied to two different kinds of
uniqueness (a name, a request).

**Time-based decisions are timestamp-in-payload, now-in-command.** `AccountClosed.closedAt`,
`ProductPriceChanged.changedAt` and `SignUpInitiated.initiatedAt` are plain `Instant` fields on the domain
event. `RegisterAccount.now`, `PlacePriceOrder.orderedAt` and `ConfirmSignUp.confirmedAt` are plain `Instant`
fields on the command. A `DcbDecider`'s `decide`/`evolve` never sees CloudEvent metadata (when the event was
actually stored), only these domain fields, so the same command replayed against the same events always
makes the same decision - which is what makes `execute` retryable in the first place.

## Why `invoicenumber` skips the decider

A `DcbDecider`'s `evolve` folds every event the boundary criteria matches, on every decision. That is the
right cost for a boundary that stays small (one username, one product, one sign-up), and the wrong cost for
a boundary that is, by construction, every invoice ever created: folding thousands of `InvoiceCreated`
events just to find the last one is O(n) for no reason. `InvoiceService` instead reads only the single
highest-position `InvoiceCreated` event with `DcbReadOptions.backwardsLimited(1)`, in one round trip, and
still appends under a `DcbAppendCondition` scoped to `DcbCriteria.type("InvoiceCreated")` qualified by that
read's consistency token - the token reflects the *whole* matching set observed at read time, not just the
one event returned, so the append still fails if any `InvoiceCreated` (not only the last one this call
happened to see) was committed after the read (see ADR 0056).

## Running the tests

```
mvn -Pexamples-module -pl example/domain/dcb-patterns test
```

No Docker or external services required: the in-memory DCB event store is faithful to the real DCB
semantics (append conditions, consistency tokens, tag-scoped reads), so every vignette's conflict/rejection
path is exercised for real, not mocked.
