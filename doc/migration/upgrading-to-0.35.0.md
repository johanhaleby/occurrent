# Upgrading to Occurrent 0.35.0

Each section describes one 0.35.0 change that requires action from a caller on 0.34.0, what the
`UpgradeToOccurrent_0_35` OpenRewrite recipe rewrites for you, and what you have to do by hand.

All seven framework annotations are renamed to an `Occurrent`-prefixed name. Nothing breaks at compile time when
you upgrade, because the old annotations are deprecated rather than deleted and keep behaving exactly as they did
in 0.34.0. Read [section 1](#1-the-seven-framework-annotations-get-an-occurrent-prefix).

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

### What each new annotation expects

`@OccurrentProjection`, `@OccurrentSaga` and `@OccurrentSnapshot` are a plain rename. Their attributes and the
factory method they mark are unchanged, so the import and the name are the whole change.

The four subscription annotations are more than a rename. The old ones go on a `void` handler method, the new ones
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

Turning a `void` handler into a descriptor is a change of its own, with its own recipe support and its own section
in this guide. This section covers the rename.

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
`StreamSubscription.StartPosition` reference to the new annotation's nested enum with it. On a `@Projection`,
`@Saga` or `@Snapshot` that is the whole upgrade, and the module compiles and behaves as before.

### What the rename does not do to a subscription

The rename alone does not make a subscription stop compiling. A renamed annotation on a `void` handler method is
still valid source, because the new annotations target a method the same way the old ones did. What does fail
compilation is a use of `eventTypes`, or of `tags` on `@OccurrentDcbSubscription`, since the new annotations do
not declare them. Delete those two and the handler compiles again.

A handler that compiles again is still not what the new annotation is for. It has the new name on a method
returning `void`, where the annotation expects a factory method returning a descriptor, so the conversion is
still owed. Finish it rather than reading a green compile as a finished migration.
