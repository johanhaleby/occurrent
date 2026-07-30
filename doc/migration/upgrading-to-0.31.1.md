# Upgrading to Occurrent 0.31.1

0.31.1 has no breaking API changes, so there is no OpenRewrite recipe and nothing to rewrite in your code.

It does change how the MongoDB event stores persist the CloudEvent `time` attribute when
`TimeRepresentation.RFC_3339_STRING` is configured. Nothing has to be migrated for the fix to work, and there is one
optional cleanup if you want the fix to cover events you have already stored. If you use `TimeRepresentation.DATE`, or
you never filter on `time`, you can stop reading here.

## What changed

The `time` attribute used to be written with `OffsetDateTime.toString()`, which omits parts of the value when they are
zero. The same instant could therefore be stored in several shapes:

```
2026-07-28T12:00Z                    the seconds and the fraction are both zero
2026-07-28T12:00:30Z                 the fraction is zero
2026-07-28T12:00:30.123456789Z       nothing is omitted
```

It is now always written in one shape, with seconds and with nine fractional digits:

```
2026-07-28T12:00:00.000000000Z
2026-07-28T12:00:30.000000000Z
2026-07-28T12:00:30.123456789Z
```

Nanosecond precision is unaffected. The only difference is that parts which used to be omitted are now written out.

That representation stores `time` as a string, so MongoDB compares it character by character. A single shape is what
makes those comparisons behave, which fixes two things: an exact filter such as `Filter.time(instant)` now matches an
event written at that instant even when its timestamp falls on a whole minute, and range filters now order correctly.
Both applied to MongoDB subscriptions as well, since they convert their filters through the same code.

## The one thing to know

Events already in your database keep the shape they were written with. Queries over events written by 0.31.1 and later
are correct, and a query whose boundary lands exactly on an older event can still miss it, because the filter is
rendered in the new shape and the stored value is in the old one.

If that matters to you, rewrite the field once. This is optional.

## Optional: rewrite the stored values

Run this against your event collection, with the server on MongoDB 4.2 or later so `$dateToString` is available in an
aggregation pipeline update. Replace `events` with your collection name, and take a backup first.

```js
db.events.updateMany(
  { time: { $type: "string" } },
  [
    {
      $set: {
        time: {
          $dateToString: {
            date: { $dateFromString: { dateString: "$time" } },
            format: "%Y-%m-%dT%H:%M:%S.%LZ"
          }
        }
      }
    }
  ]
)
```

Two caveats worth reading before you run it.

`$dateFromString` parses into a BSON date, which holds milliseconds, so this loses any precision finer than a
millisecond that was in the stored string. If your events carry microseconds or nanoseconds and you need them, do the
rewrite in application code instead, reading each `time` with `OffsetDateTime.parse` and writing it back formatted with
nine fractional digits.

`%L` emits three fractional digits, not nine, so the rewritten values are canonical in shape but not identical to what
0.31.1 writes for new events. Comparisons still behave, because all rewritten values share one shape and sort against
each other correctly, but an exact filter built from a nanosecond instant will not match a rewritten value. Use the
application-code route if you need exact matching against these older events.

## Time range queries and UTC offsets

One limit did not change. Chronological ordering of a string comparison also depends on every value carrying the same
UTC offset. These are the same instant and do not sort the same way:

```
2026-07-28T14:00:00.000000000+02:00
2026-07-28T12:00:00.000000000Z
```

Occurrent does not normalise the offset away, because preserving the timezone is the reason to choose
`RFC_3339_STRING` in the first place. So range queries are sound for a collection whose events carry a consistent
offset, which is the case if you store UTC. If your events span several offsets and you need range queries over them,
use `TimeRepresentation.DATE`, which compares numerically, or keep a separate attribute holding the instant as a
`Date`.

Rationale in [ADR 78](../architecture/decisions/0078-canonical-fixed-width-time-for-rfc3339-storage.md).
