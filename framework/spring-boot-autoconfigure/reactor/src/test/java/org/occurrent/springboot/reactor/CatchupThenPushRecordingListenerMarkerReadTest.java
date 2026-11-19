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

package org.occurrent.springboot.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.dsl.projection.reactor.Projections;
import org.occurrent.dsl.projection.reactor.RecordingReactiveUpdate;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.push.reactor.CatchupThenPushSubscriptionModel;
import org.occurrent.subscription.push.reactor.PushSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the recording listener of an applied-append store holds once a catch-up ends, when the catch-up reads the
 * marker a second time on its turn and the second read disagrees with the first (ADR 179). The replay range follows
 * the second read, so whether the listener hears the attempt has to follow it too. The listener is
 * {@link Projections#recordingAppliedAppends}, the one the annotation registrar attaches to the reactor catch-up model.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupThenPushRecordingListenerMarkerReadTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final String PROJECTION = "proj";

    private static final AppendId APPEND_1 = AppendId.of(new UUID(0, 1));
    private static final AppendId APPEND_2 = AppendId.of(new UUID(0, 2));
    private static final AppendId APPEND_3 = AppendId.of(new UUID(0, 3));
    private static final AppendId RECORDED_BEFORE_THE_REPLAY = AppendId.of(new UUID(0, 99));

    private final AppendedLog log = new AppendedLog();
    private final PushSubscriptionModel feed = new PushSubscriptionModel();
    private final AppliedAppendStore store = AppliedAppendStore.inMemory();
    private final List<String> applied = new CopyOnWriteArrayList<>();
    private final RecordingReactiveUpdate<String> recording = Projections.recordingAppliedAppends(
            (metadata, eventId) -> Mono.fromRunnable(() -> applied.add(eventId)), PROJECTION, store);

    // The first attempt replays from the beginning and, from its last event, has another thread ask for a second
    // catch-up on the same handover. The second reads no marker, then a marker at the head is stored while it waits
    // for the first one's turn, so its read on that turn finds one and its replay after the marker has nothing to
    // read. Both appends are in the store only because the first attempt's replay saw a live copy of each. Had the
    // second attempt been announced from its first read, its catch-up would have replaced the first one's and
    // dropped what that one rebuilt, and nothing replayed after the marker would put it back.
    @Test
    void a_catch_up_that_reads_no_marker_when_asked_and_a_marker_on_its_turn_leaves_the_appends_the_replay_from_the_beginning_recorded() throws Exception {
        log.append(event("1", APPEND_1), event("2", APPEND_2));
        MarkerStorage storage = new MarkerStorage();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(replayingWithLiveCopies(), feed, storage);
        model.listenForCatchup(PROJECTION, recording);
        AtomicReference<Sinks.One<Boolean>> second = new AtomicReference<>();
        AtomicBoolean asked = new AtomicBoolean();
        Function<CloudEvent, Mono<Void>> askingAfterTheLastEvent = e -> applyThen(e, () -> {
            if (e.getId().equals("2") && asked.compareAndSet(false, true)) {
                second.set(askedFromOutsideTheFold(model, PROJECTION));
                awaitQuietly(storage.secondReadAnswered);
                storage.save(PROJECTION, GlobalCheckpoint.of(2)).block(TIMEOUT);
            }
        });

        model.subscribe(PROJECTION, null, StartAt.subscriptionModelDefault(), askingAfterTheLastEvent).waitUntilStarted().block(TIMEOUT);
        Boolean secondCaughtUp = second.get().asMono().block(TIMEOUT);
        feed.accept(event("3", APPEND_3)).block(TIMEOUT);
        waitUntil(() -> store.hasApplied(PROJECTION, APPEND_3));

        assertThat(secondCaughtUp).as("the second catch-up finished").isTrue();
        assertThat(applied).as("events the fold applied, the second catch-up replaying nothing after the marker").containsExactly("1", "2", "3");
        assertThat(store.hasApplied(PROJECTION, APPEND_1)).as("append at or below the marker, rebuilt by the replay from the beginning").isTrue();
        assertThat(store.hasApplied(PROJECTION, APPEND_2)).as("append at the marker, rebuilt by the replay from the beginning").isTrue();
        assertThat(store.hasApplied(PROJECTION, APPEND_3)).as("append applied live after the catch-ups, which is only recorded once catch-up mode is closed").isTrue();
    }

    // The first attempt replays after a marker and, from its last event, has another thread ask for a second
    // catch-up on the same handover, which reads that marker too. The marker is deleted while the second waits for
    // the first one's turn, so its read on that turn finds none and it replays from the beginning. That replay
    // rebuilds the store from nothing, which drops a record the store held from before it and that the rebuilt
    // view may no longer hold. Had the second attempt been left unannounced from its first read, the store would
    // keep that record and a wait for the append it stands for would answer true.
    @Test
    void a_catch_up_that_reads_a_marker_when_asked_and_none_on_its_turn_rebuilds_the_recorded_appends_from_its_replay_from_the_beginning() throws Exception {
        log.append(event("1", APPEND_1), event("2", APPEND_2));
        MarkerStorage storage = new MarkerStorage();
        storage.save(PROJECTION, GlobalCheckpoint.of(1)).block(TIMEOUT);
        store.recordApplied(PROJECTION, RECORDED_BEFORE_THE_REPLAY);
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(replayingWithLiveCopies(), feed, storage);
        model.listenForCatchup(PROJECTION, recording);
        AtomicReference<Sinks.One<Boolean>> second = new AtomicReference<>();
        AtomicBoolean asked = new AtomicBoolean();
        Function<CloudEvent, Mono<Void>> askingAfterTheLastEvent = e -> applyThen(e, () -> {
            if (e.getId().equals("2") && asked.compareAndSet(false, true)) {
                second.set(askedFromOutsideTheFold(model, PROJECTION));
                awaitQuietly(storage.secondReadAnswered);
                storage.delete(PROJECTION).block(TIMEOUT);
            }
        });

        model.subscribe(PROJECTION, null, StartAt.subscriptionModelDefault(), askingAfterTheLastEvent).waitUntilStarted().block(TIMEOUT);
        Boolean secondCaughtUp = second.get().asMono().block(TIMEOUT);
        feed.accept(event("3", APPEND_3)).block(TIMEOUT);
        waitUntil(() -> store.hasApplied(PROJECTION, APPEND_3));

        assertThat(secondCaughtUp).as("the second catch-up finished").isTrue();
        assertThat(applied).as("events the fold applied, the first replaying after its marker and the second from the beginning").containsExactly("2", "1", "2", "3");
        assertThat(store.hasApplied(PROJECTION, RECORDED_BEFORE_THE_REPLAY)).as("append recorded before the replay from the beginning, which the replay's catch-up has to drop").isFalse();
        assertThat(store.hasApplied(PROJECTION, APPEND_1)).as("append rebuilt by the replay from the beginning").isTrue();
        assertThat(store.hasApplied(PROJECTION, APPEND_2)).as("append rebuilt by the replay from the beginning").isTrue();
        assertThat(store.hasApplied(PROJECTION, APPEND_3)).as("append applied live after the catch-ups, which is only recorded once catch-up mode is closed").isTrue();
    }

    private Mono<Void> applyThen(CloudEvent event, Runnable afterwards) {
        return recording.apply(EventMetadata.from(event), event.getId()).then(Mono.fromRunnable(afterwards));
    }

    // A replay from the beginning also sees a live copy of each event it reads, which is how a replay of an append
    // that was live at the same time ends up recorded. A replay after a marker sees none.
    private PositionOrderedReader replayingWithLiveCopies() {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                Flux<CloudEvent> read = log.readInPositionOrder(range);
                return range.afterPosition().isPresent() ? read : read.doOnNext(e -> feed.accept(e).subscribe());
            }

            @Override
            public Mono<Long> currentPosition() {
                return log.currentPosition();
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
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

    // Waits for a condition the test then asserts on its own, so a missing record fails the assertion that names it
    // rather than a timeout here.
    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
    }

    private static CloudEvent event(String id, AppendId appendId) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Updated")
                .withExtension(OccurrentCloudEventExtension.APPEND_ID, appendId.toString()).build();
    }

    // An event's position is its one-based place in the append order, and a range excludes its lower bound, which is
    // what the real stores do.
    private static final class AppendedLog {
        private final List<CloudEvent> events = new CopyOnWriteArrayList<>();

        void append(CloudEvent... appended) {
            events.addAll(List.of(appended));
        }

        Flux<CloudEvent> readInPositionOrder(PositionRange range) {
            return Flux.defer(() -> {
                List<CloudEvent> snapshot = List.copyOf(events);
                long after = range.afterPosition().orElse(0L);
                long upTo = Math.min(range.upToPosition().orElse((long) snapshot.size()), snapshot.size());
                return Flux.fromIterable(snapshot.subList((int) after, (int) Math.max(after, upTo)));
            });
        }

        Mono<Long> currentPosition() {
            return Mono.fromSupplier(() -> (long) events.size());
        }
    }

    // Keeps checkpoints in a map and evaluates write conditions as the in-memory storage does, and counts down once the
    // second read has answered.
    private static final class MarkerStorage implements CheckpointStorage {
        final CountDownLatch secondReadAnswered = new CountDownLatch(1);
        private final AtomicInteger reads = new AtomicInteger();
        private final Map<String, Checkpoint> checkpoints = new HashMap<>();
        private final Map<String, Long> versions = new HashMap<>();

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.fromSupplier(() -> {
                Checkpoint stored;
                synchronized (this) {
                    stored = checkpoints.get(subscriptionId);
                }
                if (reads.incrementAndGet() == 2) {
                    secondReadAnswered.countDown();
                }
                return stored;
            });
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.fromSupplier(() -> {
                synchronized (this) {
                    if (condition instanceof CheckpointWriteCondition.NotOlderThan notOlderThan) {
                        Long stored = versions.get(subscriptionId);
                        if (stored != null && stored > notOlderThan.writeVersion()) {
                            throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.of(stored), condition);
                        }
                        versions.put(subscriptionId, notOlderThan.writeVersion());
                    } else if (condition instanceof CheckpointWriteCondition.IfAbsent && checkpoints.containsKey(subscriptionId)) {
                        Long stored = versions.get(subscriptionId);
                        throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, stored == null ? OptionalLong.empty() : OptionalLong.of(stored), condition);
                    }
                    checkpoints.put(subscriptionId, checkpoint);
                    return checkpoint;
                }
            });
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return true;
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return Mono.fromSupplier(() -> {
                synchronized (this) {
                    return versions.get(subscriptionId);
                }
            });
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.fromRunnable(() -> {
                synchronized (this) {
                    checkpoints.remove(subscriptionId);
                    versions.remove(subscriptionId);
                }
            });
        }
    }
}
