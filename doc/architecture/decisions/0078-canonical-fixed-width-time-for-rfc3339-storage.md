# 78. A canonical fixed-width time string for RFC_3339_STRING storage

Date: 2026-07-29

## Status

Accepted. Changes the stored form of the CloudEvent `time` attribute for the three MongoDB event stores under
`TimeRepresentation.RFC_3339_STRING`, which has shipped. Resolves #463 and #468.

## Context

`TimeRepresentation.RFC_3339_STRING` persists the CloudEvent `time` attribute as a string, so MongoDB compares it byte
by byte. Occurrent rendered that string in two places with two different renderers.

On the way in, `DocumentCloudEventWriter` used `OffsetDateTime.toString()`. That is deliberately variable-length: it
omits `:ss` when second and nano are both zero, and omits the fraction when nano is zero. On the way out,
`SpecialFilterHandling` rendered a filter's instant with `RFC_3339_DATE_TIME_FORMATTER`, which always writes seconds and
writes the fraction in groups of 0, 3, 6 or 9 digits.

Two defects fell out of that, and the TCK found the first one.

`Filter.time(instant)` did not match an event written at exactly that instant when the instant's seconds and nanos were
both zero, because the store held `2026-07-28T12:00Z` while the filter looked for `2026-07-28T12:00:00Z`. It also
affected MongoDB subscriptions, since the subscription models convert their filters through the same converters.

Separately, a variable-length form cannot sort chronologically, so range queries on `time` were unsound.
`"12:00Z"` sorts after `"12:00:30Z"` because `Z` sorts after `:`, and `"12:00:00Z"` sorts after `"12:00:00.5Z"` for the
same reason. `RFC_3339_STRING`'s own javadoc already said range queries were not supported, and three per-store tests
pinned a knowingly wrong result with a comment saying so.

The equality defect hid for a long time because range conditions looked healthy. At an hour boundary the two renderings
happen to fall the right way, and every existing time test used a range with a timestamp from `LocalDateTime.now()`,
where a whole-minute value essentially never occurs. The TCK's fixed instant has zero seconds, which is why one suite
caught what four stores' worth of hand-written tests did not.

## Decision

Store one canonical form: fixed-width, always seconds, always nine fractional digits, so
`2026-07-28T12:00:00.000000000Z`. Render filter values with the same formatter, so the write path and the query path
cannot disagree.

A new formatter carries this. `RFC_3339_DATE_TIME_FORMATTER` is untouched, because it is also the serialization format
for durable catch-up subscription checkpoints, and changing it would alter strings already persisted by
`TimeBasedCheckpoint`.

One canonical form fixes both defects at once, since always writing nine fractional digits implies always writing
seconds. Equality works because both sides render identically, and ordering works because every value has the same
shape.

### The stored form deliberately deviates from the CloudEvents SDK

The SDK's own `Time.writeTime` uses `ISO_OFFSET_DATE_TIME`, whose fraction is variable, so it cannot be the storage
form for a field that has to sort. Deviating is acceptable because this string is Occurrent's storage representation
rather than a CloudEvents wire format: it is valid RFC 3339, and it parses back to the identical instant, so the
`CloudEvent` a caller reads is unchanged.

This is worth stating because the first analysis argued the opposite, that the writer should be changed precisely
because it disagreed with the SDK. Aligning with the SDK would have fixed equality and left ordering broken.

### Fixed forward, with an optional backfill rather than a required one

Events already stored keep whatever shape they were written with. New events are canonical.

That leaves one narrow caveat: an exact-boundary or equality filter against an event written before the upgrade can miss
it, because the filter renders the canonical form and the stored value does not match it. It is narrow because the
general case was already broken. Shape varied per value before this change, so a collection that mixes shapes is not a
new condition.

The alternative was a required backfill of every event document. That was rejected as disproportionate: it puts a
production data rewrite on every user, including those who never query on `time` and would gain nothing. The backfill is
offered instead of required, in [upgrading to 0.31.1](../../migration/upgrading-to-0.31.1.md), which also records that
the aggregation-pipeline form of it truncates to milliseconds and writes three fractional digits, so anyone needing
exact matching against pre-upgrade events has to do the rewrite in application code.

### The ordering guarantee is scoped to a consistent offset

Fixed width is necessary for chronological sorting but not sufficient. `2026-07-28T14:00:00.000000000+02:00` and
`2026-07-28T12:00:00.000000000Z` are the same instant and sort differently whatever the width.

Normalizing to UTC on write would fix that and is explicitly not done, because `RFC_3339_STRING` exists to preserve
timezone. Its javadoc recommends it for applications that need to keep that information, so discarding the offset to buy
sortability would defeat the representation.

So the guarantee is that range queries are sound for a collection whose events carry a consistent offset, which is the
common case. Getting both offset preservation and universal sortability needs a separate normalized sort field, which is
a schema addition and is not done here.

## Consequences

`Filter.time` equality works on the MongoDB stores and on MongoDB subscriptions. Range queries become sound for
consistent-offset data, so `RFC_3339_STRING` is no longer categorically unable to answer them, and the three per-store
tests that pinned a wrong range result now assert the right one.

Stored documents are slightly larger, since a whole-second timestamp now carries nine zero digits it did not before.
That is the price of a sortable fixed-width key.

A collection written across the upgrade holds both shapes. Ordering within each shape is fine and ordering between them
is not, which is the caveat above, and the reason the backfill exists for anyone who needs it.

Precision behaviour is now something the TCK asserts rather than something nobody checked: nanoseconds survive under
`RFC_3339_STRING`, and `DATE` keeps milliseconds because it stores a 64-bit epoch value. Neither was covered before, so
a store silently dropping nanos would not have failed a test.

`TimeRepresentation.DATE` is unaffected throughout. It stores a BSON Date and compares numerically, so it already
ordered correctly and remains the recommendation for anyone who wants range queries without thinking about string
shapes.
