package org.occurrent.subscription.blocking.durable.catchup.lifecycle;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.filter.Filter;

import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

/**
 * A fixed history whose reads can wait on a latch and take a set time, so a replay stays in flight as long as a schedule needs.
 */
final class GatedHistory implements EventStoreQueries, CatchupReads {
    private final List<CloudEvent> events;
    volatile CountDownLatch gate = new CountDownLatch(0);
    volatile int delayMicros = 0;
    final CountDownLatch queried = new CountDownLatch(1);
    final AtomicInteger readsInFlight = new AtomicInteger();

    GatedHistory(CloudEvent... events) {
        this.events = List.of(events);
    }

    static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("test.event").withTime(OffsetDateTime.now()).build();
    }

    @Override
    public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        readsInFlight.incrementAndGet();
        try {
            queried.countDown();
            try {
                gate.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (delayMicros > 0) {
                LockSupport.parkNanos(delayMicros * 1000L);
            }
            return events.stream().skip(skip).limit(limit);
        } finally {
            readsInFlight.decrementAndGet();
        }
    }

    @Override
    public CatchupReadScope openCatchupReadScope(@Nullable String notBefore) {
        return CatchupReads.unscoped(this, new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                throw new AssertionError("The time-ordered path must not read in position order");
            }

            @Override
            public long currentPosition() {
                throw new AssertionError("The time-ordered path must not read the position");
            }

            @Override
            public boolean writesPosition() {
                return false;
            }
        }).openCatchupReadScope(notBefore);
    }

    @Override
    public long count(Filter filter) {
        return events.size();
    }

    @Override
    public boolean exists(Filter filter) {
        return !events.isEmpty();
    }
}
