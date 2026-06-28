# 32. Fluent DCB query construction over the OR-of-items model

Date: 2026-06-28

## Status

Accepted

## Context

A `DcbQuery` selects DCB events. Its model is an OR of items, where each item matches CloudEvent types as any-of, DCB tags as all-of, and excluded types as none-of (ADR 0021 relies on that shape for the per-attribute conflict markers).

The original construction surface had grown a row of telescoping static factories: `typeAndTagsAllOf`, `tagsAllOfExcludingTypes`, `typeAndTagsAllOfExcludingTypes`, plus `anyOf(Collection<DcbQueryItem>)`. Every new facet doubled the factory list, and OR-ing alternatives forced callers to name `DcbQueryItem` and wrap it in a list. The course-enrollment example read:

```
DcbQuery.anyOf(listOf(
    DcbQueryItem.tagsAllOf(listOf(CourseTags.course(courseId))),
    DcbQueryItem.tagsAllOf(listOf(StudentTags.student(studentId)))))
```

DCB is unreleased, so the surface can change with no deprecation cost. The question was how far to take the redesign. One option was to import the `Filter` and `Condition` style of a fully general recursive `and`/`or`/`not` tree.

## Decision

Make construction fluent over the existing OR-of-items model, and do not adopt a general boolean tree.

- `DcbQuery.type(...)`, `types(...)`, and `tags(...)` return a single-alternative query that is refined fluently, for example `DcbQuery.type("OrderPlaced").tags("order:1")`. They replace the `typeAndTags*` and `*ExcludingTypes` factories.
- `DcbQueryItem` now implements `DcbQuery`, so a single alternative is itself a query. Callers no longer name or hand-build it. It stays as the internal model the matcher, the Mongo converter, and the marker derivation consume.
- `anyOf(...)` takes alternatives directly and flattens them. A single alternative collapses to that alternative, and a `MatchAll` argument collapses the whole query. `tagsAnyOf(a, b)` is the shortcut for `anyOf(tags(a), tags(b))`.

A general `Filter`/`Condition` tree was rejected for two reasons.

- A CloudEvent has one type but many tags. So types are naturally any-of and tags are naturally all-of. A general tree invites `and(type("X"), type("Y"))`, which reads as "type is X and Y" (unsatisfiable) while the item model treats several types as any-of. Same spelling, opposite meaning.
- The skew-safety markers are per-attribute and flat (ADR 0021), and excluded types carry no marker. A general `not(...)` over the tree would have no defined consistency meaning.

A boolean tree also adds no expressiveness here: `tags(a, b)` is the all-of, `anyOf(tags(a), tags(b))` (or `tagsAnyOf`) is the or, and `anyOf(tags(a, b), tags(c))` is the mixed case. Everything expressible in disjunctive normal form over tags is already reachable.

## Consequences

- The combinatorial factory names are gone and OR-ing alternatives no longer mentions `DcbQueryItem` or a list. The example boundary above becomes `DcbQuery.tagsAnyOf(CourseTags.course(courseId), StudentTags.student(studentId))`.
- The semantic model, the matcher, the server-side converter, and the marker and skew-safety logic are unchanged. This is a construction-surface change only, so existing behavior tests carry over.
- `DcbQuery` now has a third sealed case, the single-alternative `DcbQueryItem`. Code that switches over a query handles it alongside `Items` and `MatchAll`.
