# 87. A seam for reading a payload, rather than a JSON dependency

Date: 2026-08-02

## Status

Accepted. Resolves #58, open since 2020-12-31.

## Context

`Filter.data("name", eq("x"))` worked on the three MongoDB stores and threw on the in-memory one. That asymmetry
lands where it hurts most, because the in-memory store is what people test against, and
`occurrent-testing-junit-jupiter` had just shipped to make that easier. A user wrote a test, got an
`IllegalArgumentException`, and the same code worked in production.

The TCK made this visible rather than causing it. `supportsDataFilter()` declares which stores can reach into a
payload, and the suite asserts filtering on one side and a refusal on the other.

Issue #58's entire body was "Classpath scan for JsonPath?". Both halves of that hint turn out to be wrong for this
codebase, which is most of what this decision records.

### Almost nothing about a data filter was actually specified

One behaviour was tested against a real store anywhere in the repository: a top-level string field with `eq`, on a
flat payload. Nested paths, numbers, arrays, the remaining operators and non-object payloads were all unverified,
including on MongoDB itself. Building a second implementation against that would have produced two stores agreeing
by luck.

So the behaviour was measured first, with a throwaway probe against `mongo:8.0`:

| Filter | Payload | Result |
|---|---|---|
| `eq("Malmo")` on `person.city` | `{"person":{"city":"Malmo"}}` | matches |
| `eq(42)` on `amount` | `{"amount":42}` | matches |
| `eq("42")` on `amount` | `{"amount":42}` | no match |
| `gt(10)` on `amount` | `42` and `42.5` | matches both |
| `gt("10")` on `amount` | `{"amount":42}` | no match, and no exception |
| `eq("red")` on `tags` | `{"tags":["red","blue"]}` | matches |
| absent field, path past a scalar, root array by index | | no match |

Three of those were not predictable from reading the code: numbers compare across Java types by value, a comparison
across a type boundary returns nothing rather than failing, and an array field matches when any element does.

## Decision

### A seam, not a dependency

`DataFieldReader` reads a field out of a payload, and lives in `common/inmemory/filter-matching`. A store handed one
can answer a data filter. A store handed none refuses, as before.

The alternative was a JSON library at compile scope in that module, which depends only on `occurrent-filter` and
`jspecify` today. It is compile scope for both the in-memory event store and the in-memory subscription model, and
through the store it reaches the published `occurrent-hederlig`, so the cost lands on everyone who never filters on a
payload.

A classpath scan, as the issue suggested, has no precedent here. The repository's patterns are an optional dependency
that fails loudly at the call site, and Spring's `@ConditionalOnClass` inside the starters. Inventing a probe for a
plain-Java module would be a first, to avoid a decision the caller can simply make.

### Jackson, and a dotted path rather than JsonPath

Occurrent ships a Jackson-backed reader in `occurrent-common-inmemory-filter-matching-jackson`, because the type
rules below are subtle enough that everyone writing their own would get them wrong differently.

Jackson rather than JsonPath because it is already centrally managed and already compile scope in two published
modules, while JsonPath has no footprint in the tree at all.

**The reader resolves a dotted path, and nothing more.** `Filter.data` concatenates `"data." + name` in
storage-agnostic code, and MongoDB resolves the result with its own dot notation, which is a strict subset of
JsonPath. Supporting wildcards or array predicates in memory would mean a filter that passes in a test and fails in
production, which is worse than the refusal it replaces.

### A payload value keeps its type

A number stays a number, so `eq("42")` does not match `42`, matching MongoDB. An array is handed to the matcher as a
list, so the matcher can apply the any-element rule rather than the reader guessing.

A comparison across a type boundary matches nothing. It used to throw `ClassCastException`, and that was reachable
beyond payloads: comparing a `Number` extension with a different `Number` type through a range operator, as in
`Filter.filter("streamversion", gt(5))`, crashed. It now compares by value.

Anything the reader cannot resolve answers empty, and empty means no match. That covers an absent field, a path
continuing past a scalar, a payload whose root is not a JSON object, a content type that is not JSON, and bytes that
do not parse. **None of them throw**, because a single malformed payload would otherwise break every query against
the store.

### Wired only where a caller already configures something

> **Superseded in part by [ADR 92](0092-a-subscription-model-owns-its-filter.md) (#499).** This section treated the
> three remaining refusals as one problem needing one answer. They were two. `ReactorStreamCatchupSubscriptionModel`
> needed no configuration surface at all, because it was re-applying a filter it had already handed to the wrapped
> subscription model, which is why its refusal reached every backend rather than only in-memory. It no longer matches
> in process. `RegisteringSubscribable` does own its matching and takes a reader.

`InMemoryEventStore` and `InMemorySubscriptionModel` take a reader. `SubscriptionFilterMatcher`,
`RegisteringSubscribable` and `ReactorStreamCatchupSubscriptionModel` keep refusing, because they wrap any store and
have no configuration surface to hang a reader on. The last of those is worth knowing about: it is not in-memory
specific, so a data filter on the live tail of a reactive catch-up subscription refuses regardless of backend.

## Consequences

The in-memory store answers a data filter the way MongoDB does, verified by the same suite rather than by inspection.
`supportsDataFilter()` stays, because it now separates in-memory-with-a-reader from in-memory-without-one, and
because an out-of-tree store may genuinely be unable to read a payload.

Since every store shipping with Occurrent can now filter on a payload, the suite's refusal branch would have had no
real store left to run against.
`InMemoryEventStoreQueriesWithoutDataReaderConformanceTest` runs the same suite against a store built without a
reader, which is what a caller gets by default, so both documented outcomes stay exercised.

A caller who wants the feature adds one dependency and one call. A caller who does not pays nothing, which is the
whole point of the seam.

What this does not settle: `ne` against an array is implemented as "no element equals", which is what MongoDB does,
but the suite does not pin it. Neither does it pin what happens when a payload's JSON root is an array on a store
that could in principle index into it. Both are unspecified rather than decided.
