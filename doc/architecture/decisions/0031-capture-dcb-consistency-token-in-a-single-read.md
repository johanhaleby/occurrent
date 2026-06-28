# 31. Capture the DCB consistency token in a single consistent read

Date: 2026-06-28

## Status

Accepted

## Context

ADR 0021 defines the DCB consistency token as the sum of the versions of a query's conflict markers, captured when the query is read and rechecked inside the append transaction. The append recheck reads the markers inside its transaction, so it sees one consistent snapshot. The read-time capture did not: it read each marker with a separate `findById`, outside any session or transaction.

For a query with more than one marker, for example `tagsAllOf("t1","t2")` whose markers are `tag:t1` and `tag:t2`, an append committing between the per-marker reads produces a token that belongs to no single point in time. Because the token is a scalar sum, a torn capture can equal a later genuine sum and let the append recheck pass when it should fail, which is a missed conflict and so a write skew. The scalar sum is sound only when the markers are read as one consistent snapshot, because then any matching append strictly increases the sum.

## Decision

Capture the token by reading all of a query's markers in a single read, so the per-marker versions come from one point in time. The token stays a `long`, since a sum over a consistent snapshot is a sound "did any of my markers move" check and needs no per-marker version map.

## Consequences

- The multi-marker write-skew window is closed. A single matching append now always moves the captured sum.
- The token representation is unchanged, so `DcbConsistencyToken` and the in-transaction recheck are untouched.
- A regression test drives a multi-marker query with an interleaved scoped append, which the previous per-marker capture could not catch.
