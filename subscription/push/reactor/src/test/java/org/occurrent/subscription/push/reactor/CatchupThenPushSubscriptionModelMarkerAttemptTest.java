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
import reactor.core.publisher.Sinks;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
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
        // alreadyDeliveredByReplay is sent from boundedElastic, so a signal still on its way would miss a plain check
        await().during(Duration.ofMillis(500)).atMost(TIMEOUT).untilAsserted(() ->
                assertThat(signals.names).as("signals heard by the listener of a replay after a stored marker").isEmpty());
    }

    // The first attempt replays from the beginning and is stopped while it replays, after it announced its start and
    // before it read the history. A marker is stored while the model is stopped, so the attempt that start(true)
    // launches resumes after it and announces nothing. It has to end the episode the stopped attempt left open, or the
    // listener reads history for good and a recorder never records again.
    @Test
    void a_stopped_replay_from_the_beginning_has_its_episode_ended_by_the_next_attempt_that_resumes_after_a_marker_before_it_replays_anything() throws Exception {
        store.append(List.of(event("1"), event("2")));
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, storage);
        EpisodeSignals episodes = new EpisodeSignals();
        model.listenForCatchup("proj", episodes);
        CountDownLatch firstEventReached = new CountDownLatch(1);
        CountDownLatch releaseFirstEvent = new CountDownLatch(1);
        List<String> delivered = new CopyOnWriteArrayList<>();
        AtomicReference<List<String>> signalsWhenEventTwoWasDelivered = new AtomicReference<>();
        Function<CloudEvent, Mono<Void>> holdingTheFirstEvent = e -> Mono.fromRunnable(() -> {
            if (e.getId().equals("1")) {
                firstEventReached.countDown();
                awaitQuietly(releaseFirstEvent);
            } else {
                signalsWhenEventTwoWasDelivered.set(List.copyOf(episodes.names));
            }
            delivered.add(e.getId());
        });

        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), holdingTheFirstEvent);
        assertThat(firstEventReached.await(5, TimeUnit.SECONDS)).as("the first attempt is replaying").isTrue();
        model.stop();
        releaseFirstEvent.countDown();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(model.isCatchingUp("proj")).as("the stopped replay has unwound").isFalse());
        storage.save("proj", GlobalCheckpoint.of(1)).block(TIMEOUT);
        model.start(true);

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).as("replayed after the marker").containsExactly("1", "2"));
        assertThat(episodes.names).as("signals heard from the stopped attempt and the one that resumed after a marker")
                .containsExactly("catchupStarted#1", "historyRead#1");
        assertThat(signalsWhenEventTwoWasDelivered.get()).as("signals heard when the resumed attempt first replayed an event")
                .containsExactly("catchupStarted#1", "historyRead#1");
    }

    // The first attempt replays from the beginning and its handler fails on the first event, after it announced its
    // start and before it read the history. The cancel deletes the marker, so the next subscribe replays from the
    // beginning too. It has to end the episode the failed attempt left open before it announces its own, or the
    // listener hears two starts and one end.
    @Test
    void a_failed_replay_from_the_beginning_has_its_episode_ended_before_the_next_attempt_that_replays_from_the_beginning_announces_its_own() {
        store.append(List.of(event("1"), event("2")));
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, storage);
        EpisodeSignals episodes = new EpisodeSignals();
        model.listenForCatchup("proj", episodes);

        Subscription failing = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.error(new IllegalStateException("handler failed")));
        assertThat(catchThrowable(() -> failing.waitUntilStarted().block(TIMEOUT))).as("the attempt whose handler failed").isNotNull();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(model.isCatchingUp("proj")).as("the failed replay has unwound").isFalse());
        assertThat(episodes.names).as("signals heard from the failed attempt").containsExactly("catchupStarted#1");
        model.cancelSubscription("proj").block(TIMEOUT);
        assertThat(storage.read("proj").hasElement().block(TIMEOUT)).as("marker stored after the cancel").isFalse();

        List<String> delivered = new CopyOnWriteArrayList<>();
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.fromRunnable(() -> delivered.add(e.getId()))).waitUntilStarted().block(TIMEOUT);

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(delivered).as("replayed from the beginning").containsExactly("1", "2"));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(episodes.names).as("signals heard from the failed attempt and the one that replayed from the beginning")
                .containsExactly("catchupStarted#1", "historyRead#1", "catchupStarted#2", "historyRead#2"));
    }

    // The first attempt replays from the beginning and is held in the handler of its last event, so the cancel comes
    // after its last ownership check and it still reaches the end of its history. The next subscribe replays from the
    // beginning too and ends the episode the cancelled attempt left open, before it announces its own. Only then is
    // the first attempt let go, and its end of history comes after that episode was ended by someone else.
    @Test
    void a_cancelled_replay_from_the_beginning_that_reaches_the_end_of_its_history_after_the_next_attempt_ended_its_episode_does_not_tell_the_listener_a_second_time_that_the_history_was_read() throws Exception {
        store.append(List.of(event("1")));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, new InMemoryCheckpointStorage());
        EpisodeSignals episodes = new EpisodeSignals();
        model.listenForCatchup("proj", episodes);
        CountDownLatch lastEventReached = new CountDownLatch(1);
        CountDownLatch releaseLastEvent = new CountDownLatch(1);

        Subscription first = model.subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.fromRunnable(() -> {
            lastEventReached.countDown();
            awaitQuietly(releaseLastEvent);
        }));
        assertThat(lastEventReached.await(5, TimeUnit.SECONDS)).as("the first attempt is replaying its last event").isTrue();
        model.cancelSubscription("proj").block(TIMEOUT);
        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), e -> Mono.empty()).waitUntilStarted().block(TIMEOUT);
        assertThat(episodes.names).as("signals heard before the first attempt reaches the end of its history")
                .startsWith("catchupStarted#1", "historyRead#1", "catchupStarted#2");
        releaseLastEvent.countDown();
        catchThrowable(() -> first.waitUntilStarted().block(TIMEOUT));

        assertThat(episodes.names).as("signals heard from the cancelled attempt and the one that replaced it")
                .containsExactlyInAnyOrder("catchupStarted#1", "historyRead#1", "catchupStarted#2", "historyRead#2")
                .startsWith("catchupStarted#1", "historyRead#1", "catchupStarted#2");
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

    // The first attempt replays and, from its last event, has another thread ask for a second catch-up on the same
    // handover. The second reads no marker, then a marker is stored while it waits for the first one's turn, so its
    // read on that turn finds one. The replay it runs follows that read and starts after the marker, so the listener
    // hears nothing from it, and the first catch-up's start and end are the only ones it hears.
    @Test
    void a_catch_up_that_reads_no_marker_when_asked_and_a_marker_on_its_turn_tells_the_listener_nothing_since_it_replays_after_the_marker() throws Exception {
        store.append(List.of(event("1"), event("2")));
        ReadCountingStorage storage = new ReadCountingStorage();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, storage);
        EpisodeSignals episodes = new EpisodeSignals();
        model.listenForCatchup("proj", episodes);
        AtomicReference<Sinks.One<Boolean>> second = new AtomicReference<>();
        AtomicBoolean asked = new AtomicBoolean();
        Function<CloudEvent, Mono<Void>> askingAfterTheLastEvent = e -> Mono.fromRunnable(() -> {
            if (e.getId().equals("2") && asked.compareAndSet(false, true)) {
                second.set(askedFromOutsideTheFold(model, "proj"));
                awaitQuietly(storage.secondReadAnswered);
                storage.save("proj", GlobalCheckpoint.of(2)).block(TIMEOUT);
            }
        });

        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), askingAfterTheLastEvent).waitUntilStarted().block(TIMEOUT);
        Boolean secondCaughtUp = second.get().asMono().block(TIMEOUT);

        assertThat(secondCaughtUp).as("the second catch-up finished").isTrue();
        assertThat(episodes.names).as("signals heard from the first catch-up, and from the second, which read no marker when asked and one on its turn")
                .containsExactly("catchupStarted#1", "historyRead#1");
    }

    // The first attempt replays after a marker and, from its last event, has another thread ask for a second
    // catch-up on the same handover, which reads that marker too. The marker is deleted while the second waits for
    // the first one's turn, as a failed catch-up would, so its read on that turn finds none. Its replay starts from
    // the beginning that the read on its turn gave, so the listener hears it start and end, and hears nothing from
    // the first, which replayed after the marker.
    @Test
    void a_catch_up_that_reads_a_marker_when_asked_and_none_on_its_turn_tells_the_listener_it_started_and_that_the_history_was_read_and_replays_from_the_beginning() throws Exception {
        store.append(List.of(event("1"), event("2")));
        ReadCountingStorage storage = new ReadCountingStorage();
        storage.save("proj", GlobalCheckpoint.of(1)).block(TIMEOUT);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, feed, storage);
        EpisodeSignals episodes = new EpisodeSignals();
        model.listenForCatchup("proj", episodes);
        List<String> delivered = new CopyOnWriteArrayList<>();
        AtomicReference<Sinks.One<Boolean>> second = new AtomicReference<>();
        AtomicBoolean asked = new AtomicBoolean();
        Function<CloudEvent, Mono<Void>> askingAfterTheLastEvent = e -> Mono.fromRunnable(() -> {
            delivered.add(e.getId());
            if (e.getId().equals("2") && asked.compareAndSet(false, true)) {
                second.set(askedFromOutsideTheFold(model, "proj"));
                awaitQuietly(storage.secondReadAnswered);
                storage.delete("proj").block(TIMEOUT);
            }
        });

        model.subscribe("proj", null, StartAt.subscriptionModelDefault(), askingAfterTheLastEvent).waitUntilStarted().block(TIMEOUT);
        Boolean secondCaughtUp = second.get().asMono().block(TIMEOUT);

        assertThat(secondCaughtUp).as("the second catch-up finished").isTrue();
        assertThat(episodes.names).as("signals heard from a catch-up that read a marker when asked and none on its turn")
                .containsExactly("catchupStarted#1", "historyRead#1");
        assertThat(delivered).as("the first catch-up replays after its marker, the second from the beginning").containsExactly("2", "1", "2");
    }

    // From another thread, since a catch-up asked from the fold's own thread is answered at once and replays nothing
    // when the running one goes live, which is not the path under test.
    private static Sinks.One<Boolean> askedFromOutsideTheFold(CatchupThenPushSubscriptionModel model, String subscriptionId) {
        return CompletableFuture.supplyAsync(() -> launcherOf(model, subscriptionId).get()).orTimeout(5, TimeUnit.SECONDS).join();
    }

    @SuppressWarnings("unchecked")
    private static Supplier<Sinks.One<Boolean>> launcherOf(CatchupThenPushSubscriptionModel model, String subscriptionId) {
        try {
            Field launchers = CatchupThenPushSubscriptionModel.class.getDeclaredField("interruptibleReplays");
            launchers.setAccessible(true);
            return ((ConcurrentMap<String, Supplier<Sinks.One<Boolean>>>) launchers.get(model)).get(subscriptionId);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).as("the condition the handler waits for").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private PositionOrderedReader deliveringLiveWhileReplaying(String eventId) {
        return decorated(store, replay -> replay.doOnNext(e -> {
            if (e.getId().equals(eventId)) {
                feed.accept(e).subscribe();
            }
        }));
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

    // Answers a read once it has the stored value, and counts down when the second read has
    private static final class ReadCountingStorage extends InMemoryCheckpointStorage {
        final CountDownLatch secondReadAnswered = new CountDownLatch(1);
        private final AtomicInteger reads = new AtomicInteger();

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return super.read(subscriptionId).doOnSuccess(__ -> {
                if (reads.incrementAndGet() == 2) {
                    secondReadAnswered.countDown();
                }
            });
        }
    }

    // Names each signal after the catch-up it came from, numbered in the order the catch-ups started
    private static final class EpisodeSignals implements CatchupListener {
        final List<String> names = new CopyOnWriteArrayList<>();
        private final List<Object> episodes = new CopyOnWriteArrayList<>();

        @Override
        public synchronized void catchupStarted(Object episode) {
            episodes.add(episode);
            names.add("catchupStarted#" + episodes.size());
        }

        @Override
        public synchronized void historyRead(Object episode) {
            names.add("historyRead#" + (episodes.indexOf(episode) + 1));
        }

        @Override
        public void alreadyDeliveredByReplay(CloudEvent event) {
            names.add("alreadyDeliveredByReplay:" + event.getId());
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
