# Upgrading to Occurrent 0.32.0

0.32.0 has no breaking API changes. Nothing you compile against moved, and no type or method was renamed.

Two things are worth reading. One configuration property is deprecated and has a recipe that rewrites it for you, and
the MongoDB event stores changed how they persist the CloudEvent `time` attribute under
`TimeRepresentation.RFC_3339_STRING`.

## 1. `occurrent.subscription.enabled` becomes `occurrent.subscription.mode`

`occurrent.subscription.enabled` was a boolean. Its replacement is an enum with three values, because there is now a
third thing you can ask for:

| Old | New | What it means |
|---|---|---|
| `occurrent.subscription.enabled=false` | `occurrent.subscription.mode=disabled` | No subscription beans at all |
| `occurrent.subscription.enabled=true` | `occurrent.subscription.mode=auto` | Subscriptions are created and started, the default |
| no equivalent | `occurrent.subscription.mode=manual` | Every subscription is registered, none of them runs until you start it |

The old property still works and is deprecated, so nothing breaks if you upgrade without touching your configuration.
It is removed in the release after next. Setting both is allowed while they agree, which is deliberate: a rewritten
configuration file plus a leftover environment variable should not fail your application. Setting both so they
contradict each other fails at startup, naming both values.

### Run the recipe

```xml
<plugin>
    <groupId>org.openrewrite.maven</groupId>
    <artifactId>rewrite-maven-plugin</artifactId>
    <configuration>
        <activeRecipes>
            <recipe>org.occurrent.UpgradeToOccurrent_0_32</recipe>
        </activeRecipes>
    </configuration>
    <dependencies>
        <dependency>
            <groupId>org.occurrent</groupId>
            <artifactId>occurrent-rewrite</artifactId>
            <version>0.32.0</version>
        </dependency>
    </dependencies>
</plugin>
```

```bash
mvn rewrite:run
```

It rewrites `.properties` and `.yaml` alike, and it is deliberately not restricted to `application.properties` or
`application.yml`, so it also reaches a profile file, a `config/` directory, and anything you pull in with
`spring.config.import`. Expect the diff to cover every configuration file that sets the key, wherever it lives.

### What the recipe leaves for you

Three cases, all of which it steps around on purpose rather than guessing:

- **A value it cannot read as a boolean**, `occurrent.subscription.enabled=${SUBSCRIPTIONS_ON}` for example. It leaves
  the whole entry alone, deprecated key included. Renaming the key there would leave
  `occurrent.subscription.mode=${SUBSCRIPTIONS_ON}` resolving to `true`, which is not a mode and fails to bind. Change
  the property and whatever supplies the value together.
- **An environment variable or anything outside your configuration files.** `OCCURRENT_SUBSCRIPTION_ENABLED` is
  invisible to a source rewrite. Search your deployment configuration for it by hand. This is exactly why setting both
  properties is tolerated while they agree.
- **A file that already sets both keys.** The recipe drops the deprecated one and keeps `occurrent.subscription.mode`,
  on the assumption that the key you migrated to is the one you meant.

## 2. The CloudEvent `time` attribute

The rest of this page is the `time` change. Nothing has to be migrated for the fix to work, and there is one optional
cleanup if you want the fix to cover events you have already stored. If you use `TimeRepresentation.DATE`, or you never
filter on `time`, you can stop reading here.

### What changed

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

### The one thing to know

Events already in your database keep the shape they were written with. Queries over events written by 0.32.0 and later
are correct, and a query whose boundary lands exactly on an older event can still miss it, because the filter is
rendered in the new shape and the stored value is in the old one.

If that matters to you, rewrite the field once. This is optional.

### Optional: rewrite the stored values

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
0.32.0 writes for new events. Comparisons still behave, because all rewritten values share one shape and sort against
each other correctly, but an exact filter built from a nanosecond instant will not match a rewritten value. Use the
application-code route if you need exact matching against these older events.

### Time range queries and UTC offsets

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

Rationale in [ADR 79](../architecture/decisions/0079-canonical-fixed-width-time-for-rfc3339-storage.md).
