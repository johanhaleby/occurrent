/*
 * Copyright 2026 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CatchupListener;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * Two things that belong to one catch-up attempt rather than to the subscription id (ADR 179). Whether the stored
 * checkpoint was refused as not being a catch-up marker, and whether the {@link CatchupListener} hears the attempt.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupThenPushSubscriptionModelMarkerAttemptTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final InMemoryReactivePositionOrderedReader store = new InMemoryReactivePositionOrderedReader();
    private final PushSubscriptionModel feed = new PushSubscriptionModel();
    private final CatchupSignals signals = new CatchupSignals();

    // The refusal an earlier attempt saw is not this attempt's. Attempt 1 reads a checkpoint another component stored
    // and is still waiting for the answer when the id is cancelled. The cancel's delete fails once, so the next
    // subscribe deletes again and replays from the beginning. Attempt 2's marker write is stored and then reports a
    // failure, so its catch-up fails and has to delete the marker it wrote.
    @Test
    void a_marker_the_later_attempt_wrote_is_deleted_when_its_catch_up_fails_although_an_earlier_attempt_refused_a_foreign_checkpoint_under_the_same_id() throws Exception {
        store.append(List.of(event("1"), event("2")));
        HoldingStorage storage = new HoldingStorage();
        storage.save("X", new StringBasedCheckpoint("resume-token-xyz")).block(TIMEOUT);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, storage);

        Subscription first = model.subscribe("X", null, StartAt.subscriptionModelDefault(), e -> Mono.empty());
        assertThat(storage.firstReadStarted.await(5, TimeUnit.SECONDS)).as("the first attempt reads the stored checkpoint").isTrue();
        storage.deleteFailsOnce.set(true);
        assertThat(catchThrowable(() -> model.cancelSubscription("X").block(TIMEOUT))).as("the cancel whose delete failed").hasStackTraceContaining("The marker store is unavailable");
        // The first attempt's read answers only now, with what it read before the cancel, and refuses it
        storage.releaseFirstRead.complete(null);
        assertThat(catchThrowable(() -> first.waitUntilStarted().block(TIMEOUT))).as("the first attempt").hasStackTraceContaining("not a catch-up marker");

        storage.saveFailsAfterStoring.set(true);
        Throwable second = catchThrowable(() -> model.subscribe("X", null, StartAt.subscriptionModelDefault(), e -> Mono.empty()).waitUntilStarted().block(TIMEOUT));

        assertThat(second).as("the second attempt, whose marker write failed after it stored the head").hasStackTraceContaining("The marker store accepted the head and then failed");
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(storage.read("X").hasElement().block(TIMEOUT))
                .as("marker the second attempt wrote, after its catch-up failed").isFalse());
    }

    @Test
    void a_start_without_a_stored_marker_tells_the_listener_it_started_and_that_the_history_was_read() {
        store.append(List.of(event("1"), event("2")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, new InMemoryCheckpointStorage());
        model.listenForCatchup("proj", signals);

        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.empty()).waitUntilStarted().block(TIMEOUT);

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(signals.names).containsExactly("catchupStarted", "historyRead"));
    }

    @Test
    void a_start_without_a_marker_storage_tells_the_listener_it_started_and_that_the_history_was_read() {
        store.append(List.of(event("1")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, null);
        model.listenForCatchup("proj", signals);

        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.empty()).waitUntilStarted().block(TIMEOUT);

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(signals.names).containsExactly("catchupStarted", "historyRead"));
    }

    @Test
    void a_replay_from_the_beginning_tells_the_listener_which_payloads_it_already_delivered() {
        store.append(List.of(event("1"), event("2")));
        PositionOrderedReader overlapping = deliveringLiveWhileReplaying("2");
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(overlapping, feed, new InMemoryCheckpointStorage());
        model.listenForCatchup("proj", signals);

        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.empty()).waitUntilStarted().block(TIMEOUT);

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(signals.names).containsExactly("catchupStarted", "historyRead", "alreadyDeliveredByReplay:2"));
    }

    @Test
    void a_start_that_finds_a_marker_tells_the_listener_nothing_even_when_a_live_payload_was_also_replayed() {
        store.append(List.of(event("1"), event("2")));
        InMemoryCheckpointStorage marker = new InMemoryCheckpointStorage();
        new CatchupThenPushSubscriptionModel(store, new PushSubscriptionModel(), marker)
                .subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(marker.read("proj").hasElement().block(TIMEOUT)).isTrue());
        store.append(List.of(event("3"), event("4")));
        List<String> delivered = new CopyOnWriteArrayList<>();
        CatchupThenPushSubscriptionModel restarted = new CatchupThenPushSubscriptionModel(deliveringLiveWhileReplaying("4"), feed, marker);
        restarted.listenForCatchup("proj", signals);

        restarted.subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.fromRunnable(() -> delivered.add(e.getId())))
                .waitUntilStarted().block(TIMEOUT);

        assertThat(delivered).as("replayed after the marker, the payload both replayed and live once").containsExactly("3", "4");
        assertThat(signals.names).as("signals heard by the listener of a replay after a stored marker").isEmpty();
    }

    // The cancel comes before the attempt has read the marker, so the attempt no longer owns the id when it learns
    // that it replays from the beginning. A start it announced then would arrive after its replacement's.
    @Test
    void an_attempt_cancelled_before_it_read_the_marker_does_not_tell_the_listener_it_started() throws Exception {
        store.append(List.of(event("1")));
        HoldingStorage storage = new HoldingStorage();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, storage);
        model.listenForCatchup("proj", signals);

        Subscription cancelled = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.empty());
        assertThat(storage.firstReadStarted.await(5, TimeUnit.SECONDS)).as("the attempt is reading the marker").isTrue();
        model.cancelSubscription("proj").block(TIMEOUT);
        storage.releaseFirstRead.complete(null);
        catchThrowable(() -> cancelled.waitUntilStarted().block(TIMEOUT));

        assertThat(signals.names).as("signals heard from the attempt that was cancelled before it read the marker").isEmpty();
    }

    private PositionOrderedReader deliveringLiveWhileReplaying(String eventId) {
        return decorated(store, replay -> replay.doOnNext(e -> {
            if (e.getId().equals(eventId)) {
                feed.accept(e).subscribe();
            }
        }));
    }

    private static long storedPosition(InMemoryCheckpointStorage storage) {
        Checkpoint stored = storage.read("X").block(TIMEOUT);
        return stored == null ? -1 : GlobalCheckpoint.positionOf(stored);
    }

    private static PositionOrderedReader decorated(PositionOrderedReader delegate, UnaryOperator<Flux<CloudEvent>> onRead) {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.defer(() -> onRead.apply(delegate.readInPositionOrder(filter, range)));
            }

            @Override
            public Mono<Long> currentPosition() {
                return delegate.currentPosition();
            }

            @Override
            public boolean writesPosition() {
                return delegate.writesPosition();
            }
        };
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Updated").build();
    }

    // Answers the first read with what was stored when it started, but only once released, fails the first delete when
    // asked to, and fails a save after storing it when asked to.
    private static final class HoldingStorage extends InMemoryCheckpointStorage {
        final CountDownLatch firstReadStarted = new CountDownLatch(1);
        final CompletableFuture<Void> releaseFirstRead = new CompletableFuture<>();
        final AtomicBoolean deleteFailsOnce = new AtomicBoolean();
        final AtomicBoolean saveFailsAfterStoring = new AtomicBoolean();
        private final AtomicBoolean firstRead = new AtomicBoolean(true);

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.defer(() -> {
                Mono<Checkpoint> stored = super.read(subscriptionId);
                if (!firstRead.compareAndSet(true, false)) {
                    return stored;
                }
                return stored.materialize()
                        .doOnSubscribe(__ -> firstReadStarted.countDown())
                        .delayUntil(__ -> Mono.fromFuture(releaseFirstRead))
                        .dematerialize();
            });
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition writeCondition) {
            return super.save(subscriptionId, checkpoint, writeCondition).flatMap(saved -> saveFailsAfterStoring.compareAndSet(true, false)
                    ? Mono.<Checkpoint>error(new RuntimeException("The marker store accepted the head and then failed")) : Mono.just(saved));
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.defer(() -> deleteFailsOnce.compareAndSet(true, false)
                    ? Mono.<Void>error(new RuntimeException("The marker store is unavailable")) : super.delete(subscriptionId));
        }
    }

    private static final class CatchupSignals implements CatchupListener {
        final List<String> names = new CopyOnWriteArrayList<>();

        @Override
        public void catchupStarted(Object episode) {
            names.add("catchupStarted");
        }

        @Override
        public void historyRead(Object episode) {
            names.add("historyRead");
        }

        @Override
        public void alreadyDeliveredByReplay(CloudEvent event) {
            names.add("alreadyDeliveredByReplay:" + event.getId());
        }
    }
}
