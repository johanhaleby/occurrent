# 119. `SubscriptionModelCapability` gains instance-side `capability(..)` and `hasCapability(..)` lookups

Date: 2026-08-10

## Status

Accepted

## Context

`RepositionableSubscriptions.findIn`, `ReplayAwareSubscriptions.findIn` and `IntrospectableSubscriptions.findIn`
(blocking) each ask "does this specific facet exist somewhere in the model I am holding". A caller that instead holds
a `Class<? extends SubscriptionModelCapability>`, chosen at runtime or supplied generically, has no way to ask the
same question. It would have to switch on the class and call the matching static method by name, which defeats the
point of holding the type as a value.

Each `findIn` also carries its own copy of the same three-line search, checking whether the argument already
implements the facet, otherwise unwrapping one `SubscriptionModelWrapper` layer and recursing, otherwise returning
empty. Three facets carry three identical copies.

## Decision

Add two default methods directly to `SubscriptionModelCapability`, on both stacks:

```java
default <T extends SubscriptionModelCapability> Optional<T> capability(Class<T> type) { ... }

default boolean hasCapability(Class<? extends SubscriptionModelCapability> type) {
    return capability(type).isPresent();
}
```

`capability(type)` performs the search a `findIn` performs, generalised over the requested type instead of
hard-coded to one facet. On blocking it walks a `SubscriptionModelWrapper` chain the same way `findIn` does. On
reactor, which has no wrapper type, it reduces to a direct `instanceof` check against the receiver, confirmed by
reading `SubscriptionModelWrapper`'s absence from that stack rather than assumed from the blocking shape.

A default method, not a static one, because interface static methods are not inherited by subinterfaces or
implementing classes. `findIn` had to live on each facet interface for that exact reason. A static declared on
`SubscriptionModelCapability` would not have been reachable through `RepositionableSubscriptions` or any
implementer. A default method is inherited, so every `SubscriptionModelCapability`, and therefore every
subscription model, gets `capability(..)` and `hasCapability(..)` for free the moment it implements the marker
interface, with no facet-by-facet declaration required.

The three static `findIn` methods on blocking now delegate to the new default method instead of repeating the walk:

```java
static Optional<RepositionableSubscriptions> findIn(SubscriptionModelCapability subscriptionModel) {
    return subscriptionModel.capability(RepositionableSubscriptions.class);
}
```

This collapses three copies of the same search into one, defined once on `SubscriptionModelCapability` and reused by
every facet's static entry point, rather than adding a fourth copy inside `capability(..)` itself. A dedicated
utility class for the walk was the other option, and it was rejected. The default method already lives on the one
type every caller and every facet already reaches through, so a separate class would only add a name to look up
without removing any of the duplication the default method already removes.

`hasCapability(type)` is a one-line delegate to `capability(type).isPresent()`. It exists for the same reason `Map`
ships `containsKey` beside `get`. A caller asking a yes-or-no question should not have to name the value it does not
want. It takes no type parameter, since a `boolean` return carries nothing to infer a type from, unlike
`capability(..)`, whose return type is the reason it needs one.

The Kotlin reified counterparts,

```kotlin
inline fun <reified T : SubscriptionModelCapability> SubscriptionModelCapability.capability(): T?
inline fun <reified T : SubscriptionModelCapability> SubscriptionModelCapability.hasCapability(): Boolean
```

live in `dsl/subscription-dsl/{blocking,reactor}`, not in `subscription/api/{blocking,reactor}`. Those two API
modules ship no Kotlin file today and stay that way, so a caller who wants only the Java surface pulls no Kotlin
runtime dependency. `dsl/subscription-dsl/{blocking,reactor}` already depends on the matching API module and already
carries Kotlin sugar over its types, `streamSubscriptions`/`subscriptions` in `Subscriptions.kt`, so a caller who
wants the reified form already depends on it. The file itself, one standalone `.kt` file of extensions named after
what it extends, follows `DcbSubscriptions.kt` in `dsl/dcb-dsl/blocking`, which sugars the class-based
`DcbSubscriptions` the same way.
`capability()` returns `T?` rather than `Optional<T>`, converted with `.orElse(null)`, following the same conversion
`DcbApplicationServiceExtensions.executeOrNull` and `DcbApplicationServiceDeciderExtensions` already use elsewhere in
this repository for a Kotlin-facing counterpart to an `Optional`-returning Java method.

## Consequences

* A caller holding a `Class<? extends SubscriptionModelCapability>` can now ask for it directly,
  `subscriptionModel.capability(SomeFacet.class)`, without switching on the class to reach the matching static
  method.
* `RepositionableSubscriptions.findIn`, `ReplayAwareSubscriptions.findIn` and `IntrospectableSubscriptions.findIn`
  keep their existing signatures and behaviour, now expressed as one line each instead of the three-line search
  repeated per facet. A future facet needs no `findIn` of its own at all unless it wants one for call-site symmetry
  with the existing three. `capability(FutureFacet.class)` already reaches it the moment it extends
  `SubscriptionModelCapability`.
* The reactor stack gains the same two default methods with no wrapper chain to walk, matching the asymmetry ADR 118
  already recorded between the two stacks.
* This is purely additive to a type that has not shipped (`SubscriptionModelCapability` is new in 0.33.0), so it
  needs no migration recipe and no upgrade-guide entry.
