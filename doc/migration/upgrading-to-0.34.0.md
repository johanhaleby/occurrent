# Upgrading to Occurrent 0.34.0

Each section describes one 0.34.0 change that requires action from a caller on 0.33.0, what the
`UpgradeToOccurrent_0_34` OpenRewrite recipe rewrites for you, and what you have to do by hand.

Two things break. At compile time, if you use the flow saga's deprecated `join` or Kotlin's `expect<T>`, both are
gone. Read [section 1](#1-a-flow-sagas-join-kotlins-expectt-and-expectation-are-removed). At startup, if you set a
MongoDB collection name, a MongoDB time representation, or whether a subscription restarts after losing
change-stream history, through `OccurrentProperties`, four configuration keys are deprecated and have a recipe
that rewrites them for you. Read [section 2](#2-four-mongodb-only-keys-move-under-mongodb).

## 1. A flow saga's `join`, Kotlin's `expect<T>` and `Expectation` are removed

`StepBuilder.join`, Kotlin's `expect<T>`/`join`, and the `Expectation` type are gone. `join` was already deprecated
in 0.33.0 in favor of `on(StepCondition, ...)` with `allOf(...)`, and that replacement is what every caller now
needs. An expectation of `n` events of a type becomes `event(type, n)`, and the whole list becomes one `allOf(...)`
tree:

Java, before and after:

```java
// Before
step.join(List.of(Expectation.of(PlayerReady.class, 2)), Continuation.end());

// After
step.on(StepCondition.allOf(StepCondition.event(PlayerReady.class, 2)), Continuation.end());
```

Kotlin, before and after:

```kotlin
// Before
join(expect<PlayerReady>(2), then = end)

// After
on(allOf(event<PlayerReady>(2)), then = end)
```

`whenFulfilled`, or the trailing reaction lambda in Kotlin, carries over unchanged. It still reads
`ReceivedEvents`, not a single triggering event.

[ADR 125](../architecture/decisions/0125-a-lowered-joins-reaction-reads-its-own-window-not-the-whole-retained-history.md)
had rejected removing `join` outright. No recipe covered it, so removal would have broken every caller with no
automated fix. That recipe now exists, so this release acts on the decision ADR 125 already reasoned through
rather than relitigating it. See [#707](https://github.com/johanhaleby/occurrent/issues/707) and
[#806](https://github.com/johanhaleby/occurrent/issues/806).

### Run the recipe

`UpgradeToOccurrent_0_34` rewrites the shapes it can prove, both `join` overloads.

A `join` call rewrites when its expecting argument is a literal `List.of(...)` or `Arrays.asList(...)`, and every
element of that list is itself a literal `Expectation.of(Class)` or `Expectation.of(Class, int)` call. The recipe
cannot see what a variable or a method call contains, so a call built either way is left alone.

Two expectations naming the same type collapse to the higher of their counts, the same way `join` itself always
did, but only when both counts are integer literals. A count that is a variable or an expression cannot be
compared at rewrite time, so a duplicate-typed pair with a non-literal count is also left alone.

Every call the recipe leaves alone stops compiling once `join` is removed, so the compiler finds it for you. Fix
it by hand using the Java example above, generalized to your own list contents.

The recipe is Java only. `expect<T>` is an inline reified Kotlin function with no call site left in the compiled
class to match, and Kotlin's `join` takes named arguments and a trailing lambda, syntax the Java-template
machinery behind this recipe cannot rewrite. Every Kotlin call site needs the by-hand translation below.

### By hand

Translate a Java call the recipe left alone, or any Kotlin call, using the shapes above. Two more cases are worth
naming directly.

A `join` built from a variable or a method call, rather than a literal list, translates the same way once you
have the list of expectations in front of you. Build the `StepCondition` tree from that same list by hand, and
`on(allOf(...), ...)` replaces `join(...)` exactly as it does above.

A duplicate-typed pair whose count is not a literal needs you to work out which count wins before you write the
`event(...)` leaf. `join(List.of(Expectation.of(Type.class, a), Expectation.of(Type.class, b)), ...)` always meant
whichever of `a` and `b` is larger, so `event(Type.class, Math.max(a, b))` is the direct translation in Java, and
the Kotlin equivalent reads the same way.

## 2. Four MongoDB-only keys move under `mongodb`

`occurrent.event-store.collection`, `occurrent.event-store.time-representation`, `occurrent.subscription.collection`
and `occurrent.subscription.restart-on-change-stream-history-lost` never configured anything but a MongoDB event
store or a MongoDB subscription model, even though the module they live in (`occurrent-spring-boot-autoconfigure`)
dropped its `mongodb` name back in 0.30.0 because the rest of its code is store-neutral.

A second store, the SQL event store, is coming, and its own starter would otherwise inherit four keys promising a
collection and a change stream it does not have.

Each key now has the `mongodb` qualifier that was always true of it:

| Old | New |
|---|---|
| `occurrent.event-store.collection` | `occurrent.event-store.mongodb.collection` |
| `occurrent.event-store.time-representation` | `occurrent.event-store.mongodb.time-representation` |
| `occurrent.subscription.collection` | `occurrent.subscription.mongodb.collection` |
| `occurrent.subscription.restart-on-change-stream-history-lost` | `occurrent.subscription.mongodb.restart-on-change-stream-history-lost` |

Each old key still works and is deprecated, so nothing breaks if you upgrade without touching your configuration.
Every one of them is removed in the release after next.

Setting both the old and the new key is allowed while they agree, which is deliberate. A recipe rewrites
configuration files but cannot reach an environment variable, so an application mid-migration can legitimately have
both set. Setting both so they contradict each other fails at startup, naming both keys.

### Run the recipe

```xml
<plugin>
    <groupId>org.openrewrite.maven</groupId>
    <artifactId>rewrite-maven-plugin</artifactId>
    <configuration>
        <activeRecipes>
            <recipe>org.occurrent.UpgradeToOccurrent_0_34</recipe>
        </activeRecipes>
    </configuration>
    <dependencies>
        <dependency>
            <groupId>org.occurrent</groupId>
            <artifactId>occurrent-rewrite</artifactId>
            <version>0.34.0</version>
        </dependency>
    </dependencies>
</plugin>
```

```bash
mvn rewrite:run
```

It rewrites `.properties` and `.yaml` alike, and it is deliberately not restricted to `application.properties` or
`application.yml`, so it also reaches a profile file, a `config/` directory, and anything you pull in with
`spring.config.import`. Expect the diff to cover every configuration file that sets one of the four keys, wherever
it lives.

Unlike the `occurrent.subscription.enabled` migration in 0.32.0, no value changes here, only the key, so the recipe
is a plain rename in `.properties`. In `.yaml` it renames the key in place rather than expanding it into a nested
`mongodb:` block, so `event-store.collection: events` becomes `event-store.mongodb.collection: events` on one line
rather than a new nested mapping.

Spring's relaxed binding resolves either shape to the same property name, so this only changes how the file reads,
not what it configures. Restructure it into a nested block yourself if you prefer that layout.

### What the recipe leaves for you

Two cases, both of which it steps around on purpose rather than guessing:

- **An environment variable or anything outside your configuration files.** `OCCURRENT_EVENT_STORE_COLLECTION` is
  invisible to a source rewrite. Search your deployment configuration for it by hand. This is exactly why setting
  both the old and the new key is tolerated while they agree.
- **A file that already sets both the old and the new key.** The recipe drops the old one and keeps the
  `mongodb`-qualified key, on the assumption that the key you migrated to is the one you meant.
