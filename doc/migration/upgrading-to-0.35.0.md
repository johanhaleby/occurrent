# Upgrading to Occurrent 0.35.0

Each section describes one 0.35.0 change that requires action from a caller on 0.34.0, what the
`UpgradeToOccurrent_0_35` OpenRewrite recipe rewrites for you, and what you have to do by hand.

All seven framework annotations are renamed to an `Occurrent`-prefixed name, and the four subscription annotations
also stop taking a `void` handler method. Nothing breaks at compile time when you upgrade, because the old
annotations are deprecated rather than deleted and keep behaving exactly as they did in 0.34.0. Read
[section 1](#1-the-seven-framework-annotations-get-an-occurrent-prefix).

## 1. The seven framework annotations get an `Occurrent` prefix

Every annotation in `org.occurrent.annotation` that marks a method has a new name:

| Old | New |
|---|---|
| `@Projection` | `@OccurrentProjection` |
| `@Saga` | `@OccurrentSaga` |
| `@Snapshot` | `@OccurrentSnapshot` |
| `@Subscription` | `@OccurrentSubscription` |
| `@StreamSubscription` | `@OccurrentStreamSubscription` |
| `@DcbSubscription` | `@OccurrentDcbSubscription` |
| `@SynchronousSubscription` | `@OccurrentSynchronousSubscription` |

The old ones are deprecated for removal and still work, so an application that upgrades and changes nothing still
compiles and still runs its projections, sagas, snapshots and subscriptions. A later release removes them.

Four of the old names collide with a type of the same simple name. `@Projection` goes on a method returning a
`Projection`, `@Saga` on one returning a `Saga`, and `@Subscription` names both the annotation and the running
handle the subscription API gives back. Two single-type imports cannot share a simple name in Java
(JLS 7.5.1), so wherever a file needs both, one of the two has to be written out in full, which is what every
file in this repository that uses both already does. `@DcbSubscription` gets the same collision in this release,
when the `DcbSubscription` descriptor arrives. The remaining three, `@Snapshot`, `@StreamSubscription` and
`@SynchronousSubscription`, collide with nothing and are renamed anyway, so that the whole set is named one way.
[ADR 127](../architecture/decisions/0127-a-subscription-is-a-descriptor-and-the-annotation-stops-naming-the-concept.md)
has the reasoning, including the three alternatives that were examined and rejected.

`@OccurrentStreamSubscription` keeps its own nested `StartPosition` enum, with the same `BEGINNING_OF_TIME`, `NOW`
and `DEFAULT` constants it has today. It is still a different type from the top-level
`org.occurrent.annotation.StartPosition` the other annotations use.

### The four subscription annotations also change what the method returns

`@OccurrentProjection`, `@OccurrentSaga` and `@OccurrentSnapshot` are a plain rename. Their attributes and the
factory method they mark are unchanged, so the import and the name are the whole change.

The four subscription annotations are not a plain rename. The old ones go on a `void` handler method, the new ones
go on a no-arg factory method returning a `Subscription` or a `DcbSubscription` descriptor, the same way
`@OccurrentProjection` and `@OccurrentSaga` already work:

```java
// Before
@Subscription(id = "notifyCustomer")
void notifyCustomer(OrderShipped event) {
    mailer.shipped(event);
}

// After
@OccurrentSubscription(id = "notifyCustomer")
Subscription<OrderEvent> notifyCustomer() {
    return Subscription.<OrderEvent>builder()
        .on(OrderShipped.class, (metadata, event) -> mailer.shipped(event))
        .build();
}
```

The reactor stack returns a `ReactiveSubscription` instead, whose handlers return `Mono<Void>`, and DCB returns a
`DcbSubscription` or a `ReactiveDcbSubscription`.

Two attributes go with the old annotations rather than moving across. The new subscription annotations declare no
`eventTypes`, and `@OccurrentDcbSubscription` declares no `tags`. Both say which events the subscription wants,
which is now the descriptor's half of the split, and leaving them on the annotation would give one subscription
two places to say it with no rule for which one wins. Every other attribute is the same on the new annotation, so
`id`, `startAt`, `startAtGlobalPosition`, `startAtDcbPosition`, `startAtTimeEpochMillis`, `startAtISO8601`,
`resumeBehavior` and `startupMode` mean what they meant before.

### Run the recipe

```xml
<plugin>
    <groupId>org.openrewrite.maven</groupId>
    <artifactId>rewrite-maven-plugin</artifactId>
    <configuration>
        <activeRecipes>
            <recipe>org.occurrent.UpgradeToOccurrent_0_35</recipe>
        </activeRecipes>
    </configuration>
    <dependencies>
        <dependency>
            <groupId>org.occurrent</groupId>
            <artifactId>occurrent-rewrite</artifactId>
            <version>0.35.0</version>
        </dependency>
    </dependencies>
</plugin>
```

```bash
mvn rewrite:run
```

It changes the import and the annotation name at every use, in Java and Kotlin alike, and it moves a
`StreamSubscription.StartPosition` reference to the new annotation's nested enum with it. On a
`@Projection`, `@Saga` or `@Snapshot` that is the whole upgrade, and the module compiles and behaves as before.

### By hand, for a subscription

A renamed subscription annotation still sits on a `void` handler method after the recipe has run, and a declared
`eventTypes` or `tags` no longer has an attribute to live in, so the module does not compile until you move the
handler into a descriptor. Do that in three steps:

1. **Keep the handler method and delete its annotation.** Anything that calls it directly, or references it as a
   method reference, keeps working.
2. **Add a factory method beside it** returning `Subscription<E>` (or `DcbSubscription<E>`, or the reactor twin),
   annotated with the new name and repeating the old annotation's `id` and start-position attributes.
3. **Register one handler per event type**, each one calling the method you kept. Where the old method declared a
   `@StreamId` or a `@StreamVersion` parameter, read the same value off the `EventMetadata` the handler receives.

A declared `eventTypes` becomes the set of types you register handlers for. On DCB, declared `tags` go into the
builder's `tags(..)`, which narrows the types the handlers already select, exactly as the old annotation's query
did. Do not put them into `criteria(..)` instead. That one replaces the derived selection rather than narrowing it,
so the subscription would start receiving every type those tags admit.

Three kinds of handler need a decision from you rather than a translation:

- **A handler with Spring advice on a `@SynchronousSubscription`.** That is the one of the four annotations whose
  path goes through the Spring proxy, so a `@Transactional`, `@Retryable` or `@Cacheable` on it, or on its class,
  runs today. A body called from a lambda has no proxy in front of it, so the advice stops running. Take a
  `TransactionTemplate` in the handler instead, or keep the work in an ordinary Spring bean the handler calls.
- **A handler that declares a checked exception.** The registrars invoke reflectively today, so a `void` handler may
  declare `throws`. A descriptor's handler cannot, so either catch inside the handler or change what the method throws.
- **An application on both the blocking and the reactor stack.** Both bean post processors scan the same annotations,
  so a `void` handler in such an application is registered twice and nothing in the source says which stack owns it.
  Write the two descriptors by hand, one per stack, to keep both registrations.

Advice attached by an external pointcut is invisible to a source rewrite, so check those by hand whichever
annotation they reach.

### The three asynchronous annotations never ran your advice

`@Subscription`, `@StreamSubscription` and `@DcbSubscription` are
registered before Spring wraps the bean in its AOP proxy, and their dispatch invokes that raw target, so a
`@Transactional` on one of their handlers has never opened a transaction. Moving such a handler into a descriptor
loses nothing, and the new code says so plainly, because a handler that needs a transaction takes a
`TransactionTemplate` on the blocking stack or a `TransactionalOperator` on the reactor one.

### When a synchronous subscription is delivered changes

A `@OccurrentSynchronousSubscription` is registered later in startup than the old `@SynchronousSubscription`, after
the singletons are instantiated, which is where `@Projection`, `@Saga` and `@Snapshot` already register. A write
executed during startup, in between the two points, reaches the old annotation's handler and does not reach the new
one. That is the correct order rather than a regression, since a synchronous handler whose collaborators are not
wired yet cannot run safely, but it is a behaviour change and an application that writes during startup should know
about it.
