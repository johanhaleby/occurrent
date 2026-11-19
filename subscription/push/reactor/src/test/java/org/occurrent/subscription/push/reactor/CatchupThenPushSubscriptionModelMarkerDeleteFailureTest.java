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
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

class CatchupThenPushSubscriptionModelMarkerDeleteFailureTest {

    @Test
    void a_subscribe_after_a_cancel_whose_marker_delete_failed_replays_the_whole_history_again() {
        // Given
        MarkerWhoseDeleteFails marker = new MarkerWhoseDeleteFails(1);
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryReactivePositionOrderedReader reader = historyOf("1", "2");
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, marker);
        caughtUp(model, marker);

        // When
        model.cancelSubscription("sub");
        reader.append(List.of(event("3")));
        List<String> afterTheCancel = subscribe(model);
        feed.accept(event("live")).block(Duration.ofSeconds(5));

        // Then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(afterTheCancel).contains("live"));
        assertThat(afterTheCancel).as("events delivered to the subscription made after a cancel whose marker delete failed").containsExactly("1", "2", "3", "live");
    }

    @Test
    void a_subscribe_after_a_cancel_replays_the_whole_history_and_tries_the_marker_delete_again_while_it_keeps_failing() {
        // Given
        MarkerWhoseDeleteFails marker = new MarkerWhoseDeleteFails(Integer.MAX_VALUE);
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryReactivePositionOrderedReader reader = historyOf("1", "2");
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(reader, feed, marker);
        caughtUp(model, marker);

        // When
        model.cancelSubscription("sub");
        reader.append(List.of(event("3")));
        List<String> afterTheCancel = subscribe(model);
        feed.accept(event("live")).block(Duration.ofSeconds(5));

        // Then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(afterTheCancel).contains("live"));
        assertThat(afterTheCancel).as("events delivered to the subscription made after a cancel whose marker delete keeps failing").containsExactly("1", "2", "3", "live");
        assertThat(marker.deleteAttempts.get()).as("attempts to delete the marker, the cancel and then the subscribe").isEqualTo(2);
    }

    @Test
    void an_event_the_replay_did_not_read_is_delivered_to_the_next_subscribe_in_the_same_model_when_the_marker_write_failed_after_storing_the_head_and_the_marker_delete_failed_too() {
        // Given
        MarkerWhoseDeleteFails marker = new MarkerWhoseDeleteFails(Integer.MAX_VALUE);
        marker.failTheFirstSaveAfterStoringIt();
        PushSubscriptionModel feed = new PushSubscriptionModel();
        InMemoryReactivePositionOrderedReader store = historyOf("1", "X");
        AtomicBoolean firstReplay = new AtomicBoolean(true);
        // The head already covers X while the first replay does not read it, as if X committed after the head was read.
        PositionOrderedReader lateCommit = new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                Flux<CloudEvent> read = store.readInPositionOrder(filter, range);
                return firstReplay.compareAndSet(true, false) ? read.filter(e -> !e.getId().equals("X")) : read;
            }

            @Override
            public Mono<Long> currentPosition() {
                return store.currentPosition();
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(lateCommit, feed, marker);
        List<String> delivered = new CopyOnWriteArrayList<>();
        Function<CloudEvent, Mono<Void>> handler = e -> Mono.fromRunnable(() -> delivered.add(e.getId()));

        // When the catch-up fails after the marker was stored, so its failure forgets the marker and that delete fails
        Throwable failure = catchThrowable(() -> model.subscribe("sub", null, StartAt.subscriptionModelDefault(), handler).waitUntilStarted().block(Duration.ofSeconds(5)));
        assertThat(failure).as("failure of the first catch-up").hasMessageContaining("The marker store accepted the head and then failed");
        assertThat(marker.deleteAttempts.get()).as("attempts to delete the marker by the failure").isGreaterThanOrEqualTo(1);
        // The same id is restarted in the same model by a cancel and a subscribe
        model.cancelSubscription("sub");
        delivered.clear();
        model.subscribe("sub", null, StartAt.subscriptionModelDefault(), handler).waitUntilStarted().block(Duration.ofSeconds(5));

        // Then
        assertThat(delivered).as("events delivered to the subscription made after the failure whose marker delete failed").contains("X");
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

    private static InMemoryReactivePositionOrderedReader historyOf(String... ids) {
        InMemoryReactivePositionOrderedReader reader = new InMemoryReactivePositionOrderedReader();
        reader.append(java.util.Arrays.stream(ids).map(CatchupThenPushSubscriptionModelMarkerDeleteFailureTest::event).toList());
        return reader;
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Created").build();
    }

    private static class MarkerWhoseDeleteFails implements CheckpointStorage {
        private final InMemoryCheckpointStorage backing = new InMemoryCheckpointStorage();
        private final AtomicInteger failuresLeft;
        private final AtomicInteger deleteAttempts = new AtomicInteger();
        private final AtomicBoolean firstSaveFailsAfterStoring = new AtomicBoolean();

        private MarkerWhoseDeleteFails(int failures) {
            this.failuresLeft = new AtomicInteger(failures);
        }

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return backing.read(subscriptionId);
        }

        private void failTheFirstSaveAfterStoringIt() {
            firstSaveFailsAfterStoring.set(true);
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition writeCondition) {
            return backing.save(subscriptionId, checkpoint, writeCondition).flatMap(saved -> firstSaveFailsAfterStoring.compareAndSet(true, false)
                    ? Mono.error(new RuntimeException("The marker store accepted the head and then failed")) : Mono.just(saved));
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
