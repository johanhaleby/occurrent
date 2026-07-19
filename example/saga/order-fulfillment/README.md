# Order-fulfillment saga example

A Docker-free, in-memory demonstration of the saga DSL end to end: an order-fulfillment process shown in both authoring
surfaces, run through `SagaRunner`.

- `org.occurrent.example.saga.orderfulfillment` — the shared domain: `OrderEvent`/`OrderCommand`.
- `org.occurrent.example.saga.orderfulfillment.machine` — the process built with the machine-core `Saga.builder(...)`
  (Java), an explicit per-event-type fold and reaction.
- `org.occurrent.example.saga.orderfulfillment.flow` — the same process built with the Kotlin flow `saga { }` block, a
  linear sequence of steps, branches and a timeout.

The process: `OrderPlaced` reserves payment and arms a payment timeout; `PaymentReserved` ships the order and clears the
timeout; `PaymentFailed` cancels the order; the payment timeout firing (nobody resolving the reservation in time) also
cancels the order.

The machine-core test demonstrates two ways to dispatch the saga's commands:

1. **Decider-free (primary path)** — a plain lambda over an `ApplicationService`, with no decider involved.
2. **Decider adapter** — `CommandDispatchers.decider(...)` wiring a small `Decider` as the alternative.

Both write to a store separate from the one the saga subscribes to, so a dispatched command's own events never feed back
into the saga.

Run it:

```
rtk mvn -pl example/saga/order-fulfillment -am test
```
