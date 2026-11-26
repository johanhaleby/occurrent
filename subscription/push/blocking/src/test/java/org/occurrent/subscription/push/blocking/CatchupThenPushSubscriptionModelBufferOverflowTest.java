package org.occurrent.subscription.push.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.CatchupThenLiveOptions;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class CatchupThenPushSubscriptionModelBufferOverflowTest {

    /**
     * An event written after the replay took its snapshot, while the live buffer is full, is in no buffer and in no
     * history the replay reads. The catch-up would otherwise go live, and write its marker, without that event, and the
     * next subscribe would skip the replay that could still have found it (dec-0685).
     */
    @Test
    void an_event_written_while_the_live_buffer_is_full_fails_the_catch_up_without_a_marker_and_later_events_are_refused() throws Exception {
        // Given
        List<RoutingOutcome> observed = new CopyOnWriteArrayList<>();
        PushSubscriptionModel feed = new PushSubscriptionModel(DataFieldReader.refusing(), (CloudEvent ce, RoutingOutcome outcome) -> observed.add(outcome));
        InMemoryEventStore store = new InMemoryEventStore(feed::accept);
        store.write("stream", List.of(event("1")));
        CountDownLatch replaying = new CountDownLatch(1);
        CountDownLatch releaseReplay = new CountDownLatch(1);
        PositionOrderedReader gated = new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return store.readInPositionOrder(filter, range).peek(event -> {
                    replaying.countDown();
                    await(releaseReplay);
                });
            }

            @Override
            public long currentPosition() {
                return store.currentPosition();
            }

            @Override
            public boolean writesPosition() {
                return store.writesPosition();
            }
        };
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(gated, feed, marker, new CatchupThenLiveOptions(10, 2));
        List<String> handled = new CopyOnWriteArrayList<>();
        var subscription = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> handled.add(e.getId()));
        try {
            assertThat(replaying.await(5, TimeUnit.SECONDS)).as("the replay is reading the history").isTrue();

            // When
            observed.clear();
            store.write("stream", List.of(event("l1")));
            store.write("stream", List.of(event("l2")));
            catchThrowable(() -> store.write("stream", List.of(event("l3"))));
            releaseReplay.countDown();
            Throwable startFailure = catchThrowable(subscription::waitUntilStarted);
            Throwable laterWrite = catchThrowable(() -> store.write("stream", List.of(event("l4"))));

            // Then
            assertThat(startFailure).as("the catch-up that found the buffer full").isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Catch-up failed").cause().hasMessageContaining("buffer overflowed");
            assertThat(marker.exists("proj")).as("catch-up marker").isFalse();
            assertThat(handled).as("events handled, none of the live ones that were lost to the overflow or came after it").doesNotContain("l3", "l4");
            assertThat(laterWrite).as("a later event written to the subscription that missed one, refused so the source redelivers it")
                    .isNotNull().hasMessageContaining("Catch-up failed");
            assertThat(observed).as("what the live feed reported for l1, l2, l3 and the later l4, the last two refused")
                    .containsExactly(RoutingOutcome.DELIVERED, RoutingOutcome.DELIVERED, RoutingOutcome.REFUSED, RoutingOutcome.REFUSED);
        } finally {
            releaseReplay.countDown();
            model.shutdown();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Created").build();
    }
}
