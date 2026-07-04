# Global position catch-up example

This example demonstrates the unified global position feature. Every event written to a
position-enabled event store, regardless of which stream it belongs to, is assigned a single
monotonically increasing position. This gives a single global ordering across all streams,
which is useful for building projections that need to see events in the exact order they were
written, not grouped stream by stream.

The example is a JUnit test, `GlobalPositionCatchupTest`, that uses an in-memory event store
with position tracking enabled. It shows three things.

1. `events_written_to_different_streams_can_be_read_back_in_a_single_global_position_order`
   writes events to three different streams (one per person) and reads them back through
   `DomainEventQueries`, using `readInPositionOrder` with a `PositionRange` and `afterPosition`,
   and shows the events come back in write order across streams rather than per stream.

2. `a_projection_can_be_rebuilt_from_scratch_by_replaying_events_in_global_position_order_across_streams`
   rebuilds a small read model, `NameProjection`, by replaying events read in global position
   order. This is a position-based catch-up: the projection is fed events from multiple streams
   in the single order they were actually written, not one stream at a time.

3. `a_store_that_opts_out_of_stream_position_does_not_carry_a_position_and_rejects_position_reads`
   shows the other side of the feature. A store built with `withoutStreamPosition()` does not
   carry a position on its events, and the position-based read APIs on `DomainEventQueries`
   throw `UnsupportedOperationException` instead of silently doing the wrong thing. Ordinary
   reads are unaffected.

Run the test with:

```
mvn -pl example/projection/global-position-catchup -am -Pexamples-module test
```
