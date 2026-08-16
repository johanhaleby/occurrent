# 127. A subscription is a descriptor, and the annotation stops naming the concept

Date: 2026-08-16

## Status

Accepted. This ADR decides a design and writes no code. The implementation it describes is epic scale, sized at the end,
and registers as its own epic.

It resolves [#725](https://github.com/johanhaleby/occurrent/issues/725) and answers
[#721](https://github.com/johanhaleby/occurrent/issues/721) inside the same decision.

## Context

### A subscription is the only concept that never became data

Every other reactive concept in this library is a value you build, hold, test and hand to a runner. `View` is an
initial state and a fold. `Projection` adds which instance an event updates and which events feed it. `Saga` is state
plus the commands and timers a transition issues. `ProjectionRunner` and `SagaRunner` take one of those values and run
it.

A subscription never got that treatment. `org.occurrent.dsl.subscription.blocking.Subscriptions` declares and starts in
the same call, so `subscribe("id") { .. }` has already contacted the subscription model by the time it returns. The
annotation follows from that. `@Subscription` goes on a `void` method whose body is the handler:

```java
@Subscription(id = "mySubscription")
void mySubscription(MyDomainEvent event) {
    // this body is the subscription
}
```

Two things follow from the handler being a method rather than a value.

The handler cannot be built, inspected or exercised without the framework that invokes it. A `Projection` can be run
over the events a query returns, in a plain unit test, which is what makes running one descriptor two ways and checking
the answers agree the strongest test available for it. A `@Subscription` body has no equivalent.

And the shape of the parameter list becomes an API. `SubscriptionAnnotations` in the Spring Boot autoconfigure module
classifies each parameter as the event, the `EventMetadata`, a `@StreamId` or a `@StreamVersion`, rejects the
combinations that make no sense, and binds an argument array per delivery. All of that exists because the handler is a
method. A typed handler on a value needs none of it, and the mistakes it rejects at startup become mistakes the compiler
rejects instead.

### The name clash is not a projection problem

#721 reports that `@Projection` colliding with `Projection` reads badly in the documentation, and asks how common it is.
Measured across this repository:

- Four framework annotations collide with a type of the same simple name: `@Projection` with
  `org.occurrent.dsl.projection.Projection`, `@Saga` with `org.occurrent.dsl.saga.Saga`, `@Snapshot` with
  `org.occurrent.dsl.snapshot.Snapshot`, and `@Subscription` with `org.occurrent.subscription.api.blocking.Subscription`.
- 33 Java files that import `@Projection` write `org.occurrent.dsl.projection.Projection` in full, 93 times between
  them. For `@Saga` it is 11 files and 51 occurrences. That is 144 fully qualified references in this repository alone,
  every one of them there because two single-type imports cannot share a simple name (JLS 7.5.1).
- Kotlin has an exit that Java does not, an aliased import. It is used once, in
  `example/domain/course-enrollment/.../CourseDashboardProjection.kt`, as `import org.occurrent.dsl.projection.Projection as ProjectionModel`.
- The examples in the `@Projection` and `@Saga` javadoc, and the example quoted in #721, all use both simple names in one
  file. A reader who copies any of them gets a compile error.

A library whose own examples show a form its users cannot type has more than a cosmetic problem.

### Why the two issues are one question

#721 notes that the same thing "applies to Subscriptions, but current subscription is the outlier here because it's not
defined using data so we don't return anything, but maybe we will in the future". #725 is that future.

Once a subscription is a value, a caller holds the value and the running handle in the same statement. With today's
names that statement is `Subscription handle = runner.run("id", subscription)`, where the two `Subscription`s are
unrelated types. Deciding #721 first would settle the smaller half of a question whose larger half arrives in the same
release, and deciding #725 first would add a third meaning to a word that already has two.

### What earlier decisions have already settled

[ADR 73](0073-keep-saga-as-the-name-for-the-process-manager-dsl.md) kept `Saga` over the strictly more accurate
`ProcessManager`, and named what it was protecting: "a core vocabulary of crisp single nouns: `Decider`, `Projection`,
`View`, `Snapshot`, `Subscription`, `SideEffect`". Renaming the descriptors to break the clash is the option that ADR
already declined, and it declined it for a rename with a stronger case than breaking a name collision.

[ADR 26](0026-rename-subscription-annotation-to-stream-subscription.md) renamed `@Subscription` to
`@StreamSubscription` and recorded the mechanism a Java annotation rename has to use: "Java annotations also cannot
alias or meta-annotate each other, so the two names have to be two annotation types". A renamed annotation is a new type
plus a deprecated old one, never an alias.

`framework/annotations` depends on nothing in this repository. No module under `dsl/` depends on it either. So nesting
the annotations inside the types they collide with, `Projection.Registered` and the like, would invert the module graph.
That option is closed on structure rather than on taste.

## Decision

### 1. A subscription is a value, and the value is called `Subscription`

Add a descriptor holding exactly two things, which events it wants and what to do with each one:

```java
@OccurrentSubscription(id = "notify-customer")
Subscription<OrderEvent> notifyCustomer() {
    return Subscription.<OrderEvent>builder()
        .on(OrderShipped.class, (metadata, event) -> mailer.shipped(event))
        .on(OrderCancelled.class, (metadata, event) -> mailer.cancelled(event))
        .build();
}
```

Nothing about running it belongs in the value. The id, start position, resume behaviour, startup mode and capability
stay on the annotation, or become arguments to the runner, exactly the split
`ProjectionRunner.project(subscriptionId, projection, view, startAt)` already uses. An explicit `Filter` overrides the
type-derived selector, the way `Projection.filter()` does.

**The split cuts the other way too, so the selector attributes leave the annotations.** `eventTypes` on all four
subscription annotations and `tags` on `@DcbSubscription` describe which events are wanted, which is the descriptor's
half, and leaving them on the annotation would give one subscription two places to say it with no rule for which wins.
The new annotations do not have them. The recipe moves a declared `eventTypes` into the descriptor's handler
registrations and a declared `tags` into its criteria, and this is the one part of the rewrite that changes a
selector rather than moving a body, so the migration guide calls it out.

A third concept is right here, rather than pushing subscriptions into the two that exist. A subscription that sends
mail, publishes to a broker or warms a cache keeps no state and issues no command. `Projection` requires a fold and a
store it does not have, and `Saga` requires per-instance state and a command dispatcher it does not have. Reusing either
because the machinery would accept it is a modelling error.

**The descriptor is per stack, and this is where the mirror with `Projection` stops.** `Projection` is shared between
the blocking and reactor modules because a `View` fold is pure and synchronous, so both runners can call it. A
subscription handler is an effect, and a reactive handler has to return a `Mono<Void>` rather than block. So there are
two types, `Subscription<E>` in `dsl/subscription-dsl/blocking` and `ReactiveSubscription<E>` in
`dsl/subscription-dsl/reactor`, following the naming the rest of the reactive API already uses
(`ReactiveProjectionRunner`, `ReactiveSnapshotStore`). The selector logic they share goes in
`dsl/subscription-dsl/common`, beside the existing `SubscriptionFilters`, the same way `ProjectionFilters` and
`SagaFilters` sit in their DSLs' common modules.

DCB gets its own pair, `DcbSubscription<E>` and `ReactiveDcbSubscription<E>`, because DCB delivers `DcbEventMetadata`
and selects on tags as well as types. This is the same reason `DcbProjection` sits beside `Projection` today.

**The two DCB mechanisms that exist disagree about how the selector is built, and the new descriptor follows the
annotation.** `DcbProjection` takes a `DcbCriteria` and uses it verbatim, to the point of rejecting a wrapped projection
that also has an explicit filter. `@DcbSubscription` does the opposite, combining the cloud event types derived from the
handler with the declared tags through `SubscriptionAnnotations.buildDcbCriteria`. The descriptor keeps the annotation's
behaviour, so declared tags narrow the handled types rather than replacing them, because that is what the annotation
already promises its users and a subscription has a handler to derive types from where a `DcbProjection` has a fold and
a separate read boundary.

A caller who wants the criteria used verbatim asks the builder for it, `DcbSubscription.builder().criteria(c)`, and
that replaces the derived selector instead of narrowing it. Refusing the combination is not open to it, since every
handler registration derives a type, so a refusal rule would make the verbatim path unreachable. An event the criteria
admits that no handler handles is ignored, which is the contract `Projection` already states for a fold meeting an
event type it does not handle. What still fails the delivery is an event the `CloudEventConverter` cannot turn into an
`E`, also as it does today.

**There are four runners, not two, because DCB does not share a start position with the other capabilities.** A runner
takes an id, a descriptor and an optional start position, and returns a `SubscriptionHandle`. That handle has started
when the caller asked to wait for it, which is the same promise `ProjectionRunner` makes, since passing
`waitUntilStarted = false` returns before the replay finishes and a manual-start model hands back a handle for a
subscription nobody has started yet. The two
non-DCB ones follow `ProjectionRunner`'s shape, the `agnostic` and `stream` factories that fix the capability and a
`StartAt`. The two DCB ones follow `DcbProjectionRunner` and `ReactiveDcbProjectionRunner` instead, a single `create`
factory and a `DcbStartAt`, because DCB positions are not global positions and `DcbSubscriptions` is a different entry
point. None of the four needs `ProjectionRunner`'s store arguments, because a descriptor already has its handler where a
`Projection` needs somewhere to put its state.

The `waitUntilStarted` argument is on the blocking runners only. The reactor handle's `waitUntilStarted()` returns a
`Mono<Void>`, so a reactive runner that honoured such an argument would have to block to do it, and
`ReactiveProjectionRunner` already avoids that by returning its handle straight away. The reactive runners do the same
and the caller composes `handle.waitUntilStarted()` when it wants to wait.

### 2. The running handle becomes `SubscriptionHandle`

`org.occurrent.subscription.api.blocking.Subscription` has three methods, `id()`, `waitUntilStarted(Duration)` and a
no-argument `waitUntilStarted()` default overload. It is
what a caller holds after starting a subscription, not the subscription itself, and `SubscriptionHandle` says so. The
reactor twin renames the same way.

This is the rename AGENTS.md asks for by name: "Existing structure is not a constraint to design around. A `final`, a
class layout or an interface shape that makes the right design awkward is itself a candidate for change. Say what the
right shape is, then adjust what is in the way, rather than contorting the new code to fit." The interface in the way is
the one whose name was already wrong, and the alternative is inventing a second-choice noun for the concept that owns
the word.

The rename touches 52 files importing the blocking type and 26 importing the reactor one. It is a plain type rename, so
a declarative `ChangeType` recipe covers it, which is what `renames-0_33.yml` already does for five subscription
interfaces.

### 3. The framework annotations are prefixed, and the descriptors keep their nouns

The annotation and the descriptor name the same concept, so one of them has to stop naming it. The descriptor is what a
user builds, tests and passes around in code that runs on its own. The annotation does one thing on top of that, which
is hand the value to a framework to run, so it is the one that changes:

| Today | New name |
| --- | --- |
| `@Projection` | `@OccurrentProjection` |
| `@Saga` | `@OccurrentSaga` |
| `@Snapshot` | `@OccurrentSnapshot` |
| `@Subscription` | `@OccurrentSubscription` |
| `@StreamSubscription` | `@OccurrentStreamSubscription` |
| `@DcbSubscription` | `@OccurrentDcbSubscription` |
| `@SynchronousSubscription` | `@OccurrentSynchronousSubscription` |

All seven move, including the three with no collision today. `@DcbSubscription` acquires one as soon as
`DcbSubscription<E>` exists, so it is really two annotations that stay clean, `@StreamSubscription` and
`@SynchronousSubscription`, and a set where five annotations are prefixed and two are bare is harder to remember than
either uniform choice. Neither of those two gets a descriptor of its own, since a stream-scoped or synchronous
subscription is an ordinary `Subscription<E>` and the capability is chosen on the annotation. Per ADR 26 each is a new
annotation type with the old one deprecated `forRemoval`, and the bean post processor reads both until the old ones go.

The prefix is the library's own name rather than a framework's, and that is deliberate. `occurrent-annotations` is
Spring-free by design, only an optional jspecify dependency, so that a Quarkus or other integration can reuse the same
annotations and write its own registrars. Only the registrars are Spring. `@OccurrentProjection` stays true under any
of them where `@SpringProjection` would not.

Prefixing at all is what the JVM ecosystem already does when a framework annotation would otherwise take a generic
word, as in `@KafkaListener`, `@RabbitListener` and `@JmsListener`. It costs nine characters at each use, once per
declaration, against 144 qualified references in this repository and an unknown number outside it.

Three alternatives were examined and rejected.

**Nesting them in a container** would give `@Occurrent.Projection`. It keeps the concept word exactly and needs one
import for all seven, but it saves no characters over the prefix and has almost no precedent as a primary annotation API
on the JVM, so it buys unfamiliarity for nothing.

**A verb prefix** would give `@RunProjection` and `@RunSubscription`. It reads well and can never collide with a noun,
but `@RunSnapshot` and `@RunSynchronousSubscription` misdescribe what those two do, and a scheme that only fits five of
seven members is not a scheme.

**A single `@Occurrent(id = ..)`** dispatching on the return type becomes possible for the first time once every concept
has a descriptor, which is why it was worth examining. It fails on attributes. Even after the selector attributes move
to the descriptors, `store`, `storeName`, `source`, `catchup`, `capability`, `mode`, `startAtGlobalPosition`,
`startAtDcbPosition`, `startAtISO8601`, `subscriptionModel` and the saga's command dispatcher apply to different
subsets, so one annotation means most of its attributes being wrong for any given method, checked at startup rather
than by the compiler. That trades a naming problem for a worse one.

### 4. The `void` handler method goes with the old annotation names

**The deprecation is one thing, not two, and that is what keeps the release coherent.** A new annotation takes a
descriptor and nothing else. A deprecated one keeps accepting a `void` handler and behaves exactly as it does today,
which is the same promise ADR 26 made when it froze `@Subscription` rather than changing it under its users. So the
parameter classification in `SubscriptionAnnotations` lives exactly as long as the deprecated annotations do and is
deleted together with them, in the release that removes them. There is no window where an application has to have
both.

Retiring that classification is the point of the change rather than a side effect of it, since a typed handler turns
its startup failures into compile errors.

An OpenRewrite recipe rewrites the common case, moving the method body into a lambda inside a factory method, mapping
`@StreamId` and `@StreamVersion` parameters onto the metadata the lambda receives, and moving a declared `eventTypes`
or `tags` into the descriptor.

**A reactor handler needs one extra step.** The reactor registrar accepts a `void` method as readily as a
`Mono`-returning one, wrapping the first in an empty `Mono`, while a `ReactiveSubscription` handler has to return
`Mono<Void>`. So a `void` reactor body moved straight into a lambda does not compile, and the recipe wraps it in
`Mono.fromRunnable`. It picks the stack from which autoconfigure module the application depends on, the same thing
that decides which registrar reads the annotation today.

**The recipe refuses an advised synchronous handler rather than rewriting it, and refuses nothing else.** Spring advice reaches
exactly one of the four annotations today. `processSynchronousSubscribeAnnotation` looks the bean up by name at dispatch
time, and its own comment says why, because the bean post processor runs before Spring wraps the bean in its AOP proxy,
so the instance it was handed is the raw target. The other three paths invoke that raw target, so a `@Transactional` on
a `@Subscription`, `@StreamSubscription` or `@DcbSubscription` handler does not run today and has never run.

So a body moved into a lambda loses working advice only on `@SynchronousSubscription`, and that is the case the recipe
refuses and flags for a human, the way `FlagObjectTypedCapabilityLookup` already flags call sites a rename cannot safely
rewrite.

**The recipe looks for any advice, not only `@Transactional`.** Because that path invokes the proxy, everything Spring
advises works there, so a class-level or meta-annotated `@Transactional` counts, and so does `@Retryable`, `@Cacheable`
or anything else advised. A recipe that looked for a method-level `@Transactional` would rewrite those and drop the
advice silently, which is the failure it exists to prevent. So it refuses every `@SynchronousSubscription` handler
whose class or method has any advice annotation it can see, and refuses rather than guesses when it cannot tell.
Advice attached by an external pointcut is invisible to a source rewrite whatever it looks for, so the migration guide
says to check those by hand rather than the recipe pretending to catch them.

**The three asynchronous paths silently ignoring advice is a pre-existing defect, and the descriptor form is what fixes
it.** Today a user writes `@Transactional` on a `@Subscription` handler, everything compiles, the tests pass, and no
transaction is ever opened. There is nothing in the API that could tell them. Once the handler is a lambda inside a
factory method, nobody expects method-level advice on it, and a handler that needs a transaction takes a
`TransactionTemplate` on the blocking stack or a `TransactionalOperator` on the reactor one, and says so in the code.
The gap stops being silent because the shape of the API stops inviting the mistake.

### 5. This is an epic

The work this ADR describes, listed so it can be scoped as its own epic:

1. `Subscription<E>`, `ReactiveSubscription<E>`, `DcbSubscription<E>` and `ReactiveDcbSubscription<E>` with their
   builders, plus the shared selector logic in `dsl/subscription-dsl/common`.
2. Four runners, two following `ProjectionRunner` and `ReactiveProjectionRunner`, two following `DcbProjectionRunner`
   and `ReactiveDcbProjectionRunner`.
3. The `SubscriptionHandle` rename across `subscription/api/blocking` and `subscription/api/reactor`, 78 importing files.
4. Reworking both `SubscriptionAnnotationRegistrar` classes, 217 and 234 lines, onto descriptors, and deleting the
   parameter classification once nothing calls it.
5. Seven new annotation types, seven deprecations, and normalization of both sets in the bean post processors.
6. Recipes, declarative for the type and annotation renames, a Java visitor for the body rewrite with its refusal case.
7. A section in `doc/migration/upgrading-to-0.34.0.md`, changelog entries, and a docs branch.
8. Updating 35 test classes, 13 example files, and the 79 lines of the documentation site's reference page on `main`
   that mention one of these annotations.

## Consequences

A subscription can be built and tested without Spring and without a subscription model, which is what the other four
concepts already allow and the reason #725 was opened.

The parameter classification, and the class of startup failures it produces, goes away for subscriptions. A handler with
the wrong shape stops compiling instead.

Every application using these annotations changes, which is the cost. The word `@Subscription` moves for the second
time. ADR 26 renamed the original stream annotation to `@StreamSubscription`, the name was then reused for the
capability-agnostic annotation that exists today, and this ADR moves that one to `@OccurrentSubscription` while also
changing what its method returns. So a user who has been on this library since 0.30 sees the same word mean three
things. The recipe covers the mechanical part, and the migration guide covers what it cannot, which is Kotlin sources,
every `@SynchronousSubscription` handler the recipe refused for visible advice, and any handler advised by a pointcut
the recipe cannot see.

Two names for one word disappear. `Subscription` means the thing a user declares, `SubscriptionHandle` means the thing
they hold afterwards, and neither can be confused with an annotation.

The descriptors stay pure JVM types with no Spring coupling, so the annotations are still the only place the framework
appears. That is what makes it right for the annotations to be the ones named after the framework.

The blocking and reactor descriptors are separate types, unlike `Projection`, so a subscription cannot be written once
and run on both stacks. That is a genuine loss against the projection precedent. It comes from the handler being an
effect, and there is no honest way to give a blocking handler to a reactive runner, so a shared type would only be
possible by making one of the two stacks pretend to be the other.
