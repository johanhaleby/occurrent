package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class CatchupThenPushSubscriptionModelMarkerDeleteFailureTest {

    @Test
    void a_subscribe_after_a_cancel_whose_marker_delete_failed_replays_the_history() {
        // Given
        MarkerWhoseDeleteFails marker = new MarkerWhoseDeleteFails(1);
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(historyOf("1", "2"), feed, marker);
        caughtUp(model, marker);

        // When
        model.cancelSubscription("sub");
        List<String> afterTheCancel = subscribe(model);
        feed.accept(event("live")).block(Duration.ofSeconds(5));

        // Then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(afterTheCancel).contains("live"));
        assertThat(afterTheCancel).as("events delivered to the subscription made after a cancel whose marker delete failed").containsExactly("1", "2", "live");
    }

    @Test
    void a_subscribe_after_a_cancel_replays_the_history_while_deleting_the_marker_keeps_failing() {
        // Given
        MarkerWhoseDeleteFails marker = new MarkerWhoseDeleteFails(Integer.MAX_VALUE);
        PushSubscriptionModel feed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(historyOf("1", "2"), feed, marker);
        caughtUp(model, marker);

        // When
        model.cancelSubscription("sub");
        List<String> afterTheCancel = subscribe(model);
        feed.accept(event("live")).block(Duration.ofSeconds(5));

        // Then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(afterTheCancel).contains("live"));
        assertThat(afterTheCancel).as("events delivered to the subscription made after a cancel whose marker delete keeps failing").containsExactly("1", "2", "live");
        assertThat(marker.deleteAttempts.get()).as("attempts to delete the marker, by the cancel and again by the subscribe after it").isEqualTo(2);
    }

    private static void caughtUp(CatchupThenPushSubscriptionModel model, MarkerWhoseDeleteFails marker) {
        subscribe(model);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(marker.read("sub").hasElement().block()).as("catch-up marker written").isTrue());
    }

    private static List<String> subscribe(CatchupThenPushSubscriptionModel model) {
        List<String> delivered = new CopyOnWriteArrayList<>();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), e -> Mono.fromRunnable(() -> delivered.add(e.getId()))).waitUntilStarted().block(Duration.ofSeconds(5));
        return delivered;
    }

    private static PositionOrderedReader historyOf(String... ids) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.fromArray(ids).map(CatchupThenPushSubscriptionModelMarkerDeleteFailureTest::event);
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just((long) ids.length);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Created").build();
    }

    private static class MarkerWhoseDeleteFails implements CheckpointStorage {
        private final InMemoryCheckpointStorage backing = new InMemoryCheckpointStorage();
        private final AtomicInteger failuresLeft;
        private final AtomicInteger deleteAttempts = new AtomicInteger();

        private MarkerWhoseDeleteFails(int failures) {
            this.failuresLeft = new AtomicInteger(failures);
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return backing.read(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition writeCondition) {
            return backing.save(subscriptionId, checkpoint, writeCondition);
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return backing.writeVersion(subscriptionId);
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.defer(() -> {
                deleteAttempts.incrementAndGet();
                return failuresLeft.getAndDecrement() > 0 ? Mono.error(new RuntimeException("The marker store is unavailable")) : backing.delete(subscriptionId);
            });
        }
    }
}
