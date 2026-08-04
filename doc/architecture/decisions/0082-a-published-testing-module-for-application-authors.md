# 82. A published testing module for application authors

Date: 2026-07-30

## Status

Accepted. This pull request ships the two modules in the layout below. The follow-up work named under
Consequences is decided as direction, not as code.

## Context

Occurrent has three kinds of test code and only two homes for it.

`test-support` holds the fixtures this repository's own tests share, the `Name` domain, `DomainEvent`,
`TimeConversion`, `ReplicaSetReadyMongoDBContainer`, `FlushMongoDBExtension`. It is deliberately
unpublished: #138 closed to keep it that way, ADR 55 excluded it from the `occurrent-` prefix on the
grounds that it is never published, and ADR 77 restated the decision when it would have been convenient
to reverse it.

`occurrent-tck-*` is published, and ADR 77 built it for one specific reader: somebody implementing an
Occurrent SPI, an event store or a subscription model, in a repository that is not this one.

Neither serves the third reader, who is much more common than the second: somebody who builds an
application on Occurrent and needs help testing that application. That reader is unserved today, and
the evidence is concrete rather than assumed. The Parkster push-notification service independently wrote
a ten-line JUnit 5 extension for a subscription testing pattern that nothing in this repository uses,
not one test and not one example.

The pattern is deny-by-default subscriptions. `SubscriptionModelLifeCycle.stop()` runs in `@BeforeEach`
and `@AfterEach`, so no subscription is live when a test starts, and each test opts in to the ones it
needs with `resumeSubscription(id).waitUntilStarted()`. What it buys is that a test declares its blast
radius instead of inheriting it, so adding a subscription to the application cannot quietly change what
an existing test asserts. The `AfterEach` half is not redundant with the `BeforeEach` half, because a
Spring test context is cached across test classes, so a subscription that one class resumed is still
running when the next class starts.

The API already anticipates this. `SubscriptionModelLifeCycle.pauseSubscription`'s own javadoc says it is
"useful for testing purposes when you want to write events to an event store without triggering this
particular subscription". What is missing is not capability, it is a home, a name, and a documented
default. The documentation site currently teaches the inverse default under
`#testing-subscription-lifecycle`, pause the subscriptions you do not want, which does not survive
somebody adding a subscription to the application.

## Decision

**A third home exists, `occurrent-testing-*`, published, aimed at application authors.** It is not
`test-support`, which stays unpublished and keeps its existing role. It is not `occurrent-tck-*`, whose
reader is different and whose contents would be actively wrong on an application's test path: an
application author has no conformance suite to run and should not have one on their compile path.

**The framework-neutral module owns the mechanism, and the Spring Boot module is a thin wrapper over
it.** The layout is two leaves under a flat `testing/` aggregator, matching the shape ADR 77 chose for
`tck/`:

| Directory | Artifact | Compile-path cost to a consumer |
|---|---|---|
| `testing/junit-jupiter` | `occurrent-testing-junit-jupiter` | JUnit Jupiter, `occurrent-subscription-api-blocking` |
| `testing/spring-boot` | `occurrent-testing-spring-boot` | the above, plus Spring |

The first leaf was renamed to `testing/junit-jupiter-blocking` and `occurrent-testing-junit-jupiter-blocking` before 0.32.0 shipped, once a reactive twin was confirmed, so the pair carries the explicit stack suffix every other paired leaf in the repository uses (#529, #530). The table records the layout as decided here, and nothing else about it changed.

The ordering constraint is the load-bearing part, not the file layout. Occurrent ships small composable
libraries and assumes Spring nowhere else, so its test tooling must not either. Concretely: anything
`occurrent-testing-spring-boot` does has to be expressible against the neutral module, and the neutral
module has to be usable from plain Java with no container and no framework. The Spring leaf's job is
wiring, an `@EnableOccurrentTesting` annotation that exposes the neutral extension as a bean so a
`@SpringBootTest` gets it autowired rather than hand-constructed, following the `@EnableOccurrent`
precedent in `occurrent-mongodb-spring-boot-starter`. If a capability can only be built in the Spring
leaf, that is a signal the neutral API is missing something, not a licence to put the mechanism in the
wrapper.

**Naming is `occurrent-testing-*`.** ADR 55's standing rule, that every new published leaf gets the
`occurrent-` prefix, is honoured, and only the stem is new. It is `testing` rather than `test` to keep
distance from the unpublished `test-support`, which would otherwise read as the same family. The Spring
leaf is `occurrent-testing-spring-boot`, not `-spring-boot-starter`, because it is not a dependency
aggregator, and it does not lead with `spring-boot-`, which ADR 55 records as reserved for Spring's own
starters.

**JUnit lands on a consumer's compile path, and that is inherent.** This is the same cost ADR 77
accepted for the TCK, for the same unavoidable reason: an extension's public surface *is* JUnit
annotations, and there is no way to publish a `BeforeEachCallback` without publishing JUnit. It is a
smaller cost here than it was there, because this artifact's consumer is an application developer who
already has JUnit on the test path and will depend on this in test scope only.

**`SubscriptionModelLifeCycle.stop()` gets its contract specified before anything is published on top of
it.** Today the javadoc promises only that a stopped model delivers no events and that `start()` resumes
it. It does not promise that each subscription is individually resumable afterwards. Both
`SpringMongoSubscriptionModel.stop()` and `InMemorySubscriptionModel.stop()` happen to move every running
subscription into their paused collection, and that coincidence is the only reason a per-id
`resumeSubscription` works after a model-wide `stop`. Two implementations agreeing is not a contract, and
a published extension cannot rest on one. So the guarantee is written onto the interface as part of this
change, and pinned by the subscription conformance suite when #395 lands.

**Await conventions stay with the TCK, and this module does not grow a second copy.** ADR 77 put
`Conformance.await()` in `occurrent-tck-subscription-blocking` specifically to keep Awaitility off an
event-store implementer's compile path. `occurrent-testing-junit-jupiter` does not need it: `start(id)`
waits through `Subscription.waitUntilStarted()`, which is what makes forgetting the wait impossible
rather than merely discouraged, and ADR 78's `InMemorySubscriptionModel.waitUntilAllEventsProcessed()`
covers drain for the in-memory case. If the testing module ever does need a general await convention, the
move is to relocate the convention here and have the subscription TCK depend on it, not to duplicate it.
Recording that direction now, while #395 is still unbuilt, costs nothing. Discovering it afterwards would
mean moving a published type.

**Scope boundary: nothing store-specific ships in this round.** No MongoDB, no Testcontainers. In
particular `test-support`'s `FlushMongoDBExtension` is not promoted here, and neither is the
collection-scoped variant the push-notification service uses. That variant encodes a real and non-obvious
constraint, that a test must delete documents rather than drop collections or the database because
dropping them breaks a live Mongo change stream, so it is a genuine candidate. It is deferred because it
would pull Testcontainers and the Mongo driver onto a new artifact's compile path, and that trade
deserves its own decision rather than riding along with this one.

## Consequences

Two more artifacts join the release train, and once released their API is permanent under the rules
AGENTS.md sets out. The bookkeeping is the standard set for a new publishable leaf: both leaves get a
`bom/pom.xml` entry and stay off `<excludeArtifacts>`, the `testing` aggregator goes onto
`<excludeArtifacts>` like every other aggregate pom, and `testing` joins a CI shard. It joins the `misc`
shard, for the same reason ADR 77's `tck` lodges there: these tests run against the in-memory event store
and subscription model, so they boot no container and cost close to nothing.

The shipped extension is small, and that is on purpose. The artifact exists so the pattern has a name, a
documented default, and somewhere for the next tool to go. It does not exist because ten lines were hard
to write. The honest risk of the decision is the opposite of overengineering the code: it is that a
permanent public API is being minted for a small mechanism. That risk is accepted because the alternative
observed in practice is every application author writing the same extension slightly differently, which
is what happened here.

One cost is not fixed by this decision. Deny-by-default still lets every subscription start once during
context boot before the first `@BeforeEach` stops them, so a Spring Boot test still pays for opening and
closing every change stream. The real fix is for subscriptions never to start in a test at all, and
`SpringMongoSubscriptionModel.subscribe` is already written for it: when the model is not running it
registers the subscription straight into `pausedSubscriptions`. What is missing is a way to have the model
be stopped before the annotation bean post-processors register anything, which no JUnit extension can do
because it runs after context refresh. That needs its own decision, because a model that starts stopped
interacts with `StartupMode.WAIT_UNTIL_STARTED` and with the `waitUntilStarted()` calls the bean
post-processors make, so it is a feature rather than a flag. Filed as #481, which carries the interactions to work
through and needs its own ADR.

Nothing on `SubscriptionModelLifeCycle` enumerates subscription ids. Only `isRunning(String)` and
`isPaused(String)` answer per-id questions, so an extension cannot discover what a model knows and can only
track what it has been told about. Two things follow, and the second is a shipped API decision rather than a
follow-up.

A wrong subscription id in a test fails with a message listing the ids the extension was told about rather
than the ids that exist, which helps less than it sounds when the id was mistyped in the only place it
appears. Exposing the ids the annotation bean post-processors already collect in their private
`registeredIds` set would fix it properly, and is filed as #482.

And there is deliberately no `startAll()`. It was written, then removed before this shipped. It could only
ever resume the ids already named through `alwaysStart` or `start`, so on a fresh extension it would do
nothing at all and say nothing about it, which is the worst behavior a test helper can have. A method whose
name promises more than the API underneath can deliver is not worth shipping now and removing later, because
removing a published method breaks callers while adding one does not. A test that needs several subscriptions
names each of them. Once the registered ids are exposed, a real `startAll()` can be added without breaking
anybody.

Separately, whether `FlushMongoDBExtension` graduates into a store-specific testing leaf is left open above, and filed as #483.
