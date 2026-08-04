# Hotel room booking (reactive DCB example)

A small, from-scratch showcase of Occurrent's Dynamic Consistency Boundary (DCB) support on the **reactive stack** (Project Reactor + Spring WebFlux), written in Kotlin with the decider pattern.

It mirrors the blocking [course-enrollment](../course-enrollment) example, but every layer is reactive and it is wired by the reactive Spring Boot starter (`spring-boot-starter-mongodb-reactive`) instead of the blocking one.

## Why this example

Booking a room for a guest has to hold two rules at once, and they live on two different entities:

- a room cannot be double-booked (a room rule): the requested stay must not overlap an existing active booking, and
- a guest may hold at most a fixed number of active bookings (a guest rule).

A classic aggregate owns a single entity, so holding both rules at once usually means a saga or a read-then-write race. DCB lets one conditional append span both the room and the guest, so the decision stays atomic and consistent without either of those.

`Stay` is modelled as a half-open interval `[checkIn, checkOut)`, so a guest can check out and another check in on the same day without the two stays counting as overlapping.

## How it maps to Occurrent

- Events carry DCB tags. The `RoomBooked` and `BookingCancelled` events are tagged with BOTH the room and the guest (see `HotelBookingEventTagGenerator`), which is what makes the boundary cross both entities.
- A command reads a DCB query (the decision boundary, see `HotelBookingDcbQueries`), a decider folds that into state and decides, and the application service appends the result conditionally and retries on a conflict.
- The reactive Spring Boot starter auto-configures the reactive `DcbApplicationService`, the DCB query DSL, and the DCB subscription DSL from the beans in `Bootstrap.kt`.

The three deciders show the split: `roomDecider` and `guestDecider` are single-boundary (each over its own narrow event type), while `bookingDecider` is the cross-boundary one, typed over `DomainEvent` because it *reads* room and guest lifecycle events it does not emit.

## Reactive vs blocking

The most visible payoff of the reactive stack is the live activity feed. The blocking version returns an `SseEmitter` and has to manage a named subscription id, register `onCompletion`/`onTimeout`/`onError` cancel callbacks, and call `waitUntilStarted()` so the feed does not miss the first event. The reactive version (`BookingController.activity`) just returns the subscription's `Flux` mapped into `ServerSentEvent`s:

```kotlin
dcbSubscriptions.subscribe(roomBoundary(id)).mapNotNull { event -> ... }
```

WebFlux cancels the underlying DCB subscription automatically when the client disconnects, so none of the manual bookkeeping is needed.

## Running

DCB uses MongoDB transactions, so it needs a replica set (a single-node one is fine). The test uses Testcontainers and needs no setup. To run the app yourself, point `spring.mongodb.uri` at a replica set, or start `TestBootstrap.main` which boots a Testcontainers replica set for you.
