# 47. DCB criteria vocabulary, first-class Tag, and typed-Class construction

Date: 2026-07-05

## Status

Accepted

## Context

DCB reads and append conditions are both expressed by the same value: a selection over DCB metadata. Occurrent
modelled that value as `DcbQuery`, a sealed type of `MatchAll`, `Items` (an OR of alternatives), and `DcbQueryItem`
(one alternative: types matched any-of, tags matched all-of, excluded types none-of). Two things about that model were
worth revisiting before DCB ships.

First, the name. `DcbQuery` reads naturally for `eventStore.read(query)`, but the same value is also the consistency
condition passed to `DcbAppendCondition.failIfEventsMatch(...)`, where it is not a query at all but the criteria that
define a consistency boundary. [ADR 32](0032-fluent-dcb-query-construction.md) fixed the construction ergonomics but
kept the `Query` name. "Criteria" describes both roles.

Second, tags were bare strings with an unenforced `"key:value"` convention. `TagGenerator.tags(E)` returned
`Set<String>`, `DcbQuery.tags(...)` took strings, and the stored `dcbtags` extension is a newline-separated set of
`key:value` strings. Nothing checked that a tag actually had a key and a value, or that it did not contain the
newline that separates tags. The convention lived in documentation and in every caller's string concatenation. This
is the kind of stringly-typed value that a record makes safe.

Separately, a DCB criterion selects by CloudEvent *type string*, and that string is produced at write time by the
configured `CloudEventTypeMapper`. Callers who wanted to select by a domain event `Class` had to spell the type string
themselves (in Kotlin, `NameDefined::class.qualifiedName!!`), which is both ugly and wrong in general: the mapper does
not have to map a class to its fully qualified name, so a hand-written FQN can silently fail to match what was written.

DCB is unreleased (everything sits under `changelog.md` "Changelog next version"), so the public API can change with
no deprecation shim and no stored-data migration.

## Decision

**Rename to criteria.** `DcbQuery` becomes `DcbCriteria` and `DcbQueryItem` becomes `DcbCriterion`. The `MatchAll` and
`Items` inner records keep their names (`Items` now holds `List<DcbCriterion>`). The construction verbs are unchanged:
`all()`, `anyOf(...)`, `tags(...)`, `tagsAnyOf(...)`, `types(...)`, `type(...)`, `excludingTypes(...)`. We deliberately
did not adopt Axon's `havingTags(...).andBeingOneOfTypes(...)` phrasing; the terse verbs read as well and match the
rest of Occurrent. The word "boundary" is dropped from the API surface where it was a name rather than prose:
`DcbCloudEvents.boundaryTags(...)` becomes `tagsOf(...)`, and the `generateStreamId` / `canonicalize` parameters become
`tags`.

**First-class `Tag`.** Tags are a `Tag(String key, String value)` record with `Tag.of(key, value)`, a `canonical()`
form of `"key:value"`, and `Tag.parse(String)`. `TagGenerator` returns `Set<Tag>`, `DcbCriterion` holds `Set<Tag>`,
and the DCB DSL takes `Tag`. The core query API takes only `Tag`, so there is one tag vocabulary; the sole place a
string is unavoidable is the `@DcbSubscription` annotation, whose attributes must be compile-time constants (see
below).

- Separator between key and value is `:`; the set separator in the `dcbtags` extension stays newline. A `Tag`
  therefore forbids a newline in either part and a `:` in the key.
- `Tag.parse` splits on the *first* `:`. The key may not contain `:`; the value may. This keeps real values such as
  emails and URLs legal (`Tag.of("email", "a:b@x")` round-trips through `parse(canonical())`), while keeping the parse
  a total inverse of `canonical()` because the key is separator-free. The alternative, forbidding `:` in the value, was
  rejected as hostile to ordinary values.
- The stored wire form is unchanged: `Tag::canonical()` produces exactly the `"key:value"` string the old convention
  used, and the set is still encoded as newline-joined sorted canonical strings. The Mongo `dcbTags` index array and
  the `"tag:"` marker keys still hold canonical strings; `Tag` is an in-memory representation converted to strings at
  the storage boundary.

**Typed-`Class` construction lives in the DSL, bound to the write-time mapper.** Because a criterion matches on the
type string the `CloudEventTypeMapper` produces, a `Class -> type-string` resolution must go through that same mapper,
never `Class.getName()`. The core `eventstore-api-dcb` module has no mapper, so the core keeps only
`DcbCriterion.types(String...)`. A new `DcbCriteriaBuilder<E>` in `dcb-dsl-common` is constructed from the application's
`CloudEventTypeMapper<E>` or `CloudEventConverter<E>` and resolves `type(Class)` / `types(Class...)` through
`getCloudEventType(Class)`. The DSL entry points (`DcbDomainEventQueries`, `DcbSubscriptions`, blocking and reactor)
expose a pre-bound `criteria()` accessor, and Kotlin gets a reified `typeOf<E>()` extension (named `typeOf` rather than
`type` so it does not shadow the Java `type(Class)` member, per [ADR 12](0012-avoid-kotlin-extension-name-collisions-with-java-applicationservice-members.md)).
This is the same resolution path `@DcbSubscription(eventTypes = ...)` already uses for its `Class<?>[]` attribute.

**The annotation string boundary.** `@DcbSubscription` attributes are compile-time constants, so `tagsAllOf()` stays
`String[]` in the documented `"key:value"` form. The bean post processors parse each value with `Tag.parse` at startup
and fail fast, naming the offending subscription id and tag, so a malformed tag is a startup error rather than a silent
non-match.

## Consequences

The same word, criteria, now covers both DCB roles (a read selection and a consistency condition), and a tag is a
validated value instead of a convention. Illegal tags (no key, no value, a stray newline) are unrepresentable rather
than caught late or not at all. Callers gain compile-time-checked, refactor-safe type selection via
`criteria().types(SomeEvent.class)` / `typeOf<SomeEvent>()` without ever hand-writing a type string.

Because DCB is unreleased there is no deprecation surface and no stored-format change: the `dcbtags` extension, the
Mongo `dcbTags` index field, and the `"tag:"` marker prefix are byte-identical to before. Every in-repository consumer
(stores, DSLs, application services, the annotation processors, and the examples) is updated in this change.

This extends [ADR 17](0017-introduce-dcb-as-shared-cloudevent-capability.md) and [ADR 32](0032-fluent-dcb-query-construction.md):
the OR-of-items model and its construction ergonomics are unchanged in shape; only the vocabulary (criteria, tag) and
the typed-`Class` convenience are new. The `@DcbSubscription` `tagsAllOf` attribute name is left as-is here; aligning it
with the DSL's unqualified `tags` default is a separate follow-up.
