# 48. Annotation-driven DCB tag generator

Date: 2026-07-05

## Status

Accepted

## Context

A DCB application must tell Occurrent which tags each domain event carries, by supplying a `TagGenerator<E>` whose
`tags(E)` returns the event's `Set<Tag>` (see [ADR 47](0047-dcb-criteria-tag-type-and-typed-class-construction.md) for
the `Tag` type). Today every application hand-writes that generator, typically a `switch` or `when` over the event type
returning the relevant tags. That is explicit and keeps the domain model free of any Occurrent dependency, but it is
boilerplate that restates, in a second place, information that already lives on the event's own fields.

Axon's DCB support offers an alternative: an `@EventTag` annotation on the fields of an event, from which the framework
derives the tags. It reads well. The open question for Occurrent is whether to offer the same convenience, given that
Occurrent deliberately keeps domain models independent of the library: annotating a domain event with an Occurrent
annotation couples that model to Occurrent, which the hand-written generator does not.

## Decision

Offer the convenience as a strictly optional, opt-in addition, structured so that teams who want domain independence
pay nothing and are not pulled toward the annotation.

**A `@DcbTag` annotation, in the existing dependency-free `annotations` jar.** `@DcbTag` (package
`org.occurrent.annotation`) marks a record component, field, or accessor whose value becomes a tag. Its single
attribute `key()` defaults to the member's name, so `@DcbTag String email` produces the tag `email:<value>` and
`@DcbTag(key = "customer") String customerId` produces `customer:<value>`. It lives in the `annotations` module, which
has no dependencies (not even Spring), so a domain event that opts in depends only on a tiny annotation jar. Null and
blank values are skipped.

**An `AnnotationTagGenerator`, in a new optional module.** `AnnotationTagGenerator<E> implements TagGenerator<E>`
(module `dcb-annotation-taggenerator`, package `org.occurrent.application.service.dcb.annotation`) reflects over an
event to read its `@DcbTag` members and build the tags. It is production-quality: it scans each concrete event class
once and caches the result in a `ConcurrentHashMap`, and it reads values through cached `MethodHandle`s bound to the
public accessors (record accessors and getters), not private fields, so it needs no `setAccessible` and no
module-system opens. Records are read through `getRecordComponents()`; other classes (including Kotlin `data class`es)
through their annotated fields and getters, deduplicated by resolved key so a Kotlin property annotated on both its
backing field and its getter yields one tag. An event with no `@DcbTag` yields an empty set. Value conversion is
`toString()`, and only top-level members are read; a pluggable value converter and nested traversal are possible later
extensions, deliberately not built now.

**Opt-in Spring wiring that is never dragged in.** The Spring Boot starters gain a conditional bean that supplies an
`AnnotationTagGenerator` as the default `TagGenerator` only when the generator module is on the classpath
(`@ConditionalOnClass`) and the application has not defined its own `TagGenerator` (`@ConditionalOnMissingBean`). The
starters declare the generator module as an optional dependency, so it is not transitively placed on any application's
classpath. A team activates the feature by explicitly adding the module. When it is absent the starters behave exactly
as before.

## Consequences

Teams that want the convenience annotate their event fields, add one module, and get automatic tagging with no
hand-written generator. Teams that want their domain model free of Occurrent keep writing a `TagGenerator` by hand,
which stays a first-class, fully supported path and remains the default the documentation leads with. The annotation
jar being dependency-free keeps the coupling cost of opting in as small as it can be: a domain event that carries
`@DcbTag` depends on a pure annotation and nothing else.

The reflection is done once per concrete event type and cached, so steady-state cost is a `ConcurrentHashMap` lookup
plus a handful of `MethodHandle` invocations per event. Reading through public accessors rather than private fields
avoids the access and module-system fragility that field reflection would introduce, at the cost of requiring an
accessor for an annotated field (records and Kotlin `data class`es always have one).

The word "tag" now appears both as the `Tag` value type and as the `@DcbTag` annotation, but they sit in different
modules and read together naturally (`@DcbTag` produces a `Tag`). This is a small, contained optional surface that does
not change any existing behavior when unused.
