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

package org.occurrent.dsl.saga.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.*;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Two callers releasing the same quarantined saga instance at once, with each interleaving driven by latches in the
 * state store so it is deterministic. Whichever caller wins, the input the instance was quarantined on, and the inputs
 * it skipped, are applied exactly once.
 */
@DisplayName("Two callers releasing the same quarantined saga instance")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaReleaseConcurrentCallersTest {

    private static final String ORDER = "o1";
    private static final Duration BUDGET = Duration.ofMinutes(1);
    private static final SagaRunnerConfig CONFIG = SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET)
            .withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT);

    sealed interface OrderEvent permits OrderPlaced, ItemAdded {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record ItemAdded(String eventId, String orderId, String item) implements OrderEvent {
    }

    record Pack(String orderId, String item) {
    }

    record Order(String orderId, List<String> items) {
        Order with(String item) {
            List<String> added = new ArrayList<>(items);
            added.add(item);
            return new Order(orderId, List.copyOf(added));
        }
    }

    private volatile boolean placedFails = false;
    private volatile boolean poisonFails = false;
    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    private final InMemoryEventStore eventStore = new InMemoryEventStore();
    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final List<Object> dispatched = new CopyOnWriteArrayList<>();
    private final AtomicInteger eventIds = new AtomicInteger();
    private final InMemorySagaStateStore<Order> inMemory = new InMemorySagaStateStore<>();
    private final SagaEnvelopes<Order> envelopes = SagaEnvelopes.of(inMemory, null);

    private Saga<OrderEvent, Order, Object> saga() {
        return Saga.<OrderEvent, Order, Object>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(e.orderId(), List.of()))
                .react(OrderPlaced.class, (state, e) -> {
                    if (placedFails) {
                        throw new IllegalStateException("cannot place");
                    }
                    return List.of();
                })
                .evolve(ItemAdded.class, (state, e) -> state.with(e.item()))
                .react(ItemAdded.class, (state, e) -> {
                    if (poisonFails && e.item().equals("poison")) {
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.orderId(), e.item())));
                })
                .isTerminal(state -> Collections.frequency(state.items(), "x") >= 2)
                .build();
    }

    private SagaExecution<OrderEvent, Order, Object> execution(SagaStateStore<Order> store) {
        return new SagaExecution<>("orders", saga(), store, dispatched::add, converter, CONFIG, now::get);
    }

    private CloudEvent append(OrderEvent event) {
        eventStore.write(ORDER, converter.toCloudEvents(List.of(event)));
        return eventStore.read(ORDER).eventList().getLast();
    }

    private String nextEventId() {
        return "e" + eventIds.incrementAndGet();
    }

    private SagaEnvelope<Order> instance() {
        return envelopes.find(ORDER).orElseThrow();
    }

    private String recordsLeft(String quarantineId) {
        return inMemory.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE).toString();
    }

    private void quarantineOn(SagaExecution<OrderEvent, Order, Object> execution, CloudEvent event) {
        catchThrowable(() -> execution.onCloudEvent(event));
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> execution.onCloudEvent(event));
        assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED);
    }

    @Test
    void a_second_release_call_starting_the_release_after_the_first_finished_it_loses_and_does_not_lose_the_input() throws Exception {
        StartRacingStore store = new StartRacingStore();
        SagaExecution<OrderEvent, Order, Object> execution = execution(store);
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        quarantineOn(execution, append(new ItemAdded(nextEventId(), ORDER, "poison")));
        String quarantineId = instance().quarantineId();
        poisonFails = false;

        CompletableFuture<ReleaseResult> b = CompletableFuture.supplyAsync(() -> {
            store.b = Thread.currentThread();
            return execution.release(ORDER);
        });
        await(store.bHasRead);
        ReleaseResult aResult = execution.release(ORDER);
        store.aFinished.countDown();
        ReleaseResult bResult = b.get(10, TimeUnit.SECONDS);

        SagaEnvelope<Order> after = instance();
        String evidence = "A returned " + aResult + ", B returned " + bResult + ", status " + after.status() + ", items " + after.state().items()
                          + ", dispatched " + dispatched + ", records left under the quarantine id: " + recordsLeft(quarantineId);
        assertAll(
                () -> assertThat(after.state().items()).as("the input the instance was quarantined on is applied once. " + evidence).containsExactly("poison"),
                () -> assertThat(dispatched).as("the commands it produced. " + evidence).containsExactly(new Pack(ORDER, "poison")),
                () -> assertThat(store.bStartWon).as("B's start of the release loses its version compare-and-set. " + evidence).isFalse(),
                () -> assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE)).as("no record is left. " + evidence).isEmpty()
        );
    }

    @Test
    void a_second_release_call_starting_the_release_while_the_first_has_not_replayed_its_first_record_loses_and_the_input_is_applied_once_and_before_every_later_record() throws Exception {
        HeldReadStore store = new HeldReadStore();
        SagaExecution<OrderEvent, Order, Object> execution = execution(store);
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        CloudEvent poison = append(new ItemAdded(nextEventId(), ORDER, "poison"));
        quarantineOn(execution, poison);
        poisonFails = false;
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "a")));
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "b")));
        String quarantineId = instance().quarantineId();

        CompletableFuture<ReleaseResult> b = CompletableFuture.supplyAsync(() -> {
            store.b = Thread.currentThread();
            return execution.release(ORDER);
        });
        await(store.bHasRead);
        ReleaseResult aResult = execution.release(ORDER);
        store.aFinished.countDown();
        ReleaseResult bResult = b.get(10, TimeUnit.SECONDS);

        SagaEnvelope<Order> after = instance();
        String evidence = "A returned " + aResult + ", B returned " + bResult + ", status " + after.status() + ", items " + after.state().items()
                          + ", dispatched " + dispatched + ", records left under the quarantine id: " + recordsLeft(quarantineId);
        assertAll(
                () -> assertThat(after.state().items()).as("the input the instance was quarantined on is applied once and before the inputs it skipped. " + evidence).containsExactly("poison", "a", "b"),
                () -> assertThat(dispatched).as("the commands it produced. " + evidence).containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b")),
                () -> assertThat(store.bStartWon).as("B's start of the release loses its version compare-and-set. " + evidence).isFalse(),
                () -> assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE)).as("no record is left. " + evidence).isEmpty()
        );
    }

    @Test
    void a_release_call_reading_the_instance_before_the_start_event_is_replayed_does_not_discard_the_input_recorded_after_it() throws Exception {
        PollerRacingStore store = new PollerRacingStore();
        SagaExecution<OrderEvent, Order, Object> execution = execution(store);
        placedFails = true;
        quarantineOn(execution, append(new OrderPlaced(nextEventId(), ORDER)));
        String quarantineId = instance().quarantineId();
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "r1")));
        assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE)).as("the input it was quarantined on and r1").hasSize(2);
        placedFails = false;

        CompletableFuture<ReleaseResult> r = CompletableFuture.supplyAsync(() -> {
            store.r = Thread.currentThread();
            return execution.release(ORDER);
        });
        await(store.rHasStartedTheRelease);
        CompletableFuture<ReleaseResult> p = CompletableFuture.supplyAsync(() -> {
            store.p = Thread.currentThread();
            return execution.release(ORDER);
        });
        ReleaseResult rResult = r.get(10, TimeUnit.SECONDS);
        ReleaseResult pResult = p.get(10, TimeUnit.SECONDS);

        SagaEnvelope<Order> after = instance();
        String evidence = "R returned " + rResult + ", P returned " + pResult + ", status " + after.status() + ", started " + after.started()
                          + ", items " + after.state().items() + ", dispatched " + dispatched + ", records left: " + recordsLeft(quarantineId);
        assertAll(
                () -> assertThat(rResult).as("R. " + evidence).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(pResult).as("P. " + evidence).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(after.state().items()).as("the input recorded after the start event is applied once. " + evidence).containsExactly("r1"),
                () -> assertThat(dispatched).as("its command is dispatched at least once and only its command. " + evidence).isNotEmpty().containsOnly(new Pack(ORDER, "r1"))
        );
    }

    @Test
    void a_release_call_replaying_a_record_another_call_applied_after_it_read_it_does_not_apply_it_again()
            throws Exception {
        CloudEvent streamless = withoutStream(append(new ItemAdded("streamless", ORDER, "a")));
        StaleRecordStore store = new StaleRecordStore(streamless.getId());
        SagaExecution<OrderEvent, Order, Object> execution = execution(store);
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        quarantineOn(execution, append(new ItemAdded(nextEventId(), ORDER, "poison")));
        poisonFails = false;
        execution.onCloudEvent(streamless);
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "b")));
        String quarantineId = instance().quarantineId();
        assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE))
                .as("poison, the input with no stream and b").hasSize(3);

        CompletableFuture<ReleaseResult> a = CompletableFuture.supplyAsync(() -> {
            store.a = Thread.currentThread();
            try {
                return execution.release(ORDER);
            } finally {
                store.aFinished.countDown();
            }
        });
        await(store.aHasReadTheRecordWithNoStream);
        ReleaseResult bResult = execution.release(ORDER);
        ReleaseResult aResult = a.get(10, TimeUnit.SECONDS);

        SagaEnvelope<Order> after = instance();
        String evidence = "A returned " + aResult + ", B returned " + bResult + ", status " + after.status()
                          + ", items " + after.state().items() + ", dispatched " + dispatched
                          + ", records left under the quarantine id: " + recordsLeft(quarantineId);
        assertAll(
                () -> assertThat(after.state().items())
                        .as("the input with no stream is applied once, by B. " + evidence)
                        .containsExactly("poison", "a", "b"),
                () -> assertThat(store.staleReplayWon)
                        .as("A's write of the record B applied already. " + evidence).isFalse(),
                () -> assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE))
                        .as("no record is left. " + evidence).isEmpty()
        );
    }

    @Test
    void a_release_call_replaying_a_record_another_call_applied_that_would_complete_the_instance_when_applied_again_loses_and_the_later_records_are_applied()
            throws Exception {
        CloudEvent streamless = withoutStream(append(new ItemAdded("streamless", ORDER, "x")));
        StaleRecordStore store = new StaleRecordStore(streamless.getId());
        SagaExecution<OrderEvent, Order, Object> execution = execution(store);
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        quarantineOn(execution, append(new ItemAdded(nextEventId(), ORDER, "poison")));
        poisonFails = false;
        execution.onCloudEvent(streamless);
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "y")));
        String quarantineId = instance().quarantineId();
        assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE))
                .as("poison, the input with no stream and y").hasSize(3);

        CompletableFuture<ReleaseResult> a = CompletableFuture.supplyAsync(() -> {
            store.a = Thread.currentThread();
            try {
                return execution.release(ORDER);
            } finally {
                store.aFinished.countDown();
            }
        });
        await(store.aHasReadTheRecordWithNoStream);
        ReleaseResult bResult = execution.release(ORDER);
        ReleaseResult aResult = a.get(10, TimeUnit.SECONDS);

        SagaEnvelope<Order> after = instance();
        String evidence = "A returned " + aResult + ", B returned " + bResult + ", status " + after.status()
                          + ", items " + after.state().items() + ", dispatched " + dispatched
                          + ", records left under the quarantine id: " + recordsLeft(quarantineId);
        assertAll(
                () -> assertThat(store.completingWriteWon)
                        .as("A's write that would complete the instance by applying x again. " + evidence).isFalse(),
                () -> assertThat(after.state().items())
                        .as("x is applied once and y, recorded after it, is applied. " + evidence)
                        .containsExactly("poison", "x", "y"),
                () -> assertThat(after.status())
                        .as("the instance is not completed by a second application of x. " + evidence)
                        .isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE))
                        .as("no record is left. " + evidence).isEmpty()
        );
    }

    @Test
    void a_release_call_replaying_a_record_another_call_applied_that_no_longer_fits_does_not_quarantine_the_instance_again_on_it()
            throws Exception {
        CloudEvent streamless = withoutStream(append(new ItemAdded("streamless", ORDER, "a")));
        NoLongerFittingStore store = new NoLongerFittingStore(streamless.getId());
        SagaExecution<OrderEvent, Order, Object> execution = execution(store);
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        quarantineOn(execution, append(new ItemAdded(nextEventId(), ORDER, "poison")));
        poisonFails = false;
        execution.onCloudEvent(streamless);
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "b")));
        String quarantineId = instance().quarantineId();
        assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE))
                .as("poison, the input with no stream and b").hasSize(3);

        CompletableFuture<ReleaseResult> a = CompletableFuture.supplyAsync(() -> {
            store.a = Thread.currentThread();
            try {
                return execution.release(ORDER);
            } finally {
                store.aFinished.countDown();
            }
        });
        await(store.aHasReadTheRecordWithNoStream);
        ReleaseResult bResult = execution.release(ORDER);
        ReleaseResult aResult = a.get(10, TimeUnit.SECONDS);

        SagaEnvelope<Order> after = instance();
        String evidence = "A returned " + aResult + ", B returned " + bResult + ", status " + after.status()
                          + ", items " + after.state().items() + ", dispatched " + dispatched
                          + ", records left under the quarantine id: " + recordsLeft(quarantineId);
        assertAll(
                () -> assertThat(store.aSavedWithoutState)
                        .as("A quarantined the instance B changed again on a record B deleted. " + evidence).isFalse(),
                () -> assertThat(aResult).as("A's result. " + evidence).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(bResult).as("B's result. " + evidence).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(after.quarantineId()).as("the instance is not quarantined. " + evidence).isNull(),
                () -> assertThat(after.state().items())
                        .as("the input with no stream is applied once. " + evidence)
                        .containsExactly("poison", "a", "b"),
                () -> assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE))
                        .as("no record is left. " + evidence).isEmpty()
        );
    }

    private static boolean holdsATwice(List<String> items) {
        return Collections.frequency(items, "a") > 1;
    }

    private static CloudEvent withoutStream(CloudEvent event) {
        return CloudEventBuilder.v1(event)
                .withoutExtension(OccurrentCloudEventExtension.STREAM_ID)
                .withoutExtension(OccurrentCloudEventExtension.STREAM_VERSION)
                .build();
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("latch not released in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private class DelegatingStore implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
        @Override
        public boolean fits(StoredSaga candidate) {
            return inMemory.fits(candidate);
        }

        @Override
        public boolean fitsWithoutState(StoredSaga saga, long expectedVersion) {
            return inMemory.fitsWithoutState(saga, expectedVersion);
        }

        @Override
        public void requireAtomicTransitions() {
            inMemory.requireAtomicTransitions();
        }

        @Override
        public SagaStateCodec<Order> stateCodec() {
            return inMemory.stateCodec();
        }

        @Override
        public Optional<StoredSaga> find(String sagaId) {
            return inMemory.find(sagaId);
        }

        @Override
        public Optional<StoredSaga> findWithoutState(String sagaId) {
            return inMemory.findWithoutState(sagaId);
        }

        @Override
        public boolean compareAndSave(StoredSaga saga, long expectedVersion) {
            return inMemory.compareAndSave(saga, expectedVersion);
        }

        @Override
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            return inMemory.compareAndSaveWithoutState(saga, expectedVersion);
        }

        @Override
        public List<StoredSaga> findWithDueTimers(Instant now, int limit) {
            return inMemory.findWithDueTimers(now, limit);
        }

        @Override
        public List<String> findReleasingSagaIds(@Nullable String after, int limit) {
            return inMemory.findReleasingSagaIds(after, limit);
        }

        @Override
        public boolean compareAndSaveSkipped(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput input) {
            return inMemory.compareAndSaveSkipped(saga, expectedVersion, quarantineId, input);
        }

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            return inMemory.compareAndSaveReplayed(saga, expectedVersion, quarantineId, replayed);
        }

        @Override
        public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
            return inMemory.compareAndSaveQuarantineEnded(saga, expectedVersion, quarantineId);
        }

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            return inMemory.findSkippedInputs(sagaId, quarantineId, limit);
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInput> findWithFirstSkippedInput(String sagaId) {
            return inMemory.findWithFirstSkippedInput(sagaId);
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInputKey> findWithFirstSkippedInputKey(String sagaId) {
            return inMemory.findWithFirstSkippedInputKey(sagaId);
        }

        @Override
        public void delete(String sagaId) {
            inMemory.delete(sagaId);
        }
    }

    // Call order: B reads, A reads, A starts the release and finishes it, B resumes and tries to start the release it read
    // the instance for, which must lose
    private final class StartRacingStore extends DelegatingStore {
        volatile @Nullable Thread b;
        final CountDownLatch bHasRead = new CountDownLatch(1);
        final CountDownLatch aFinished = new CountDownLatch(1);
        final AtomicBoolean bStartWon = new AtomicBoolean(true);
        private final AtomicInteger bReads = new AtomicInteger();

        private boolean isB() {
            return Thread.currentThread() == b;
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInput> findWithFirstSkippedInput(String sagaId) {
            Optional<StoredSagaWithFirstSkippedInput> found = super.findWithFirstSkippedInput(sagaId);
            if (isB() && bReads.incrementAndGet() == 1) {
                bHasRead.countDown();
            }
            return found;
        }

        @Override
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            if (isB()) {
                await(aFinished);
                boolean saved = super.compareAndSaveWithoutState(saga, expectedVersion);
                bStartWon.set(saved);
                return saved;
            }
            return super.compareAndSaveWithoutState(saga, expectedVersion);
        }
    }

    // Call order: B reads the instance, A starts the release and pauses before its first read of the replay, B tries to start the
    // release it read the instance for and pauses, A reads and finishes the release, B resumes
    private final class HeldReadStore extends DelegatingStore {
        volatile @Nullable Thread b;
        final CountDownLatch bHasRead = new CountDownLatch(1);
        final CountDownLatch aIsAboutToRead = new CountDownLatch(1);
        final CountDownLatch bTriedToStart = new CountDownLatch(1);
        final CountDownLatch aFinished = new CountDownLatch(1);
        final AtomicBoolean bStartWon = new AtomicBoolean(true);
        private final AtomicInteger bReads = new AtomicInteger();
        private final AtomicInteger aReads = new AtomicInteger();

        private boolean isB() {
            return Thread.currentThread() == b;
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInput> findWithFirstSkippedInput(String sagaId) {
            if (isB()) {
                Optional<StoredSagaWithFirstSkippedInput> found = super.findWithFirstSkippedInput(sagaId);
                if (bReads.incrementAndGet() == 1) {
                    bHasRead.countDown();
                }
                return found;
            }
            // The first read starts the release, the second is the replay's, after A has started it
            if (aReads.incrementAndGet() == 2) {
                aIsAboutToRead.countDown();
                await(bTriedToStart);
            }
            return super.findWithFirstSkippedInput(sagaId);
        }

        @Override
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            if (isB()) {
                await(aIsAboutToRead);
                boolean saved = super.compareAndSaveWithoutState(saga, expectedVersion);
                bStartWon.set(saved);
                bTriedToStart.countDown();
                await(aFinished);
                return saved;
            }
            return super.compareAndSaveWithoutState(saga, expectedVersion);
        }
    }

    // Call order: R starts the release and waits, P reads the releasing instance with its start record and waits before it
    // uses it, R replays the start event, deletes its record and reads the record after it and waits, P replays the start
    // record it read, loses, reads again, replays the record after it, deletes it, and the two finish
    private final class PollerRacingStore extends DelegatingStore {
        volatile @Nullable Thread r;
        volatile @Nullable Thread p;
        final CountDownLatch rHasStartedTheRelease = new CountDownLatch(1);
        final CountDownLatch pHasReadTheInstance = new CountDownLatch(1);
        final CountDownLatch rDeletedTheStartRecord = new CountDownLatch(1);
        final CountDownLatch pDeletedItsRecord = new CountDownLatch(1);
        private final AtomicInteger rWrites = new AtomicInteger();
        private final AtomicBoolean rPaused = new AtomicBoolean();
        private final AtomicInteger pReads = new AtomicInteger();

        private boolean isR() {
            return Thread.currentThread() == r;
        }

        private boolean isP() {
            return Thread.currentThread() == p;
        }

        @Override
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            boolean saved = super.compareAndSaveWithoutState(saga, expectedVersion);
            if (isR() && rWrites.incrementAndGet() == 1) {
                rHasStartedTheRelease.countDown();
                await(pHasReadTheInstance);
            }
            return saved;
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInput> findWithFirstSkippedInput(String sagaId) {
            Optional<StoredSagaWithFirstSkippedInput> found = super.findWithFirstSkippedInput(sagaId);
            // P's first read finds the instance releasing, its second is the replay's, and P keeps that one until R deleted the start record
            if (isP() && pReads.incrementAndGet() == 2) {
                pHasReadTheInstance.countDown();
                await(rDeletedTheStartRecord);
            }
            // R waits once, on its first read that finds a record after the start record, and keeps what it read
            if (isR() && !rPaused.get() && found.map(StoredSagaWithFirstSkippedInput::firstSkippedInput).filter(first -> first.seq() > 0).isPresent()
                && rPaused.compareAndSet(false, true)) {
                await(pDeletedItsRecord);
            }
            return found;
        }

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            boolean saved = super.compareAndSaveReplayed(saga, expectedVersion, quarantineId, replayed);
            if (isR() && replayed.seq() == 0) {
                rDeletedTheStartRecord.countDown();
            }
            if (isP() && replayed.seq() > 0) {
                pDeletedItsRecord.countDown();
            }
            return saved;
        }
    }

    // Call order: A replays the first record, reads the one with no stream and pauses, B applies that record and pauses
    // with a record left, A replays the record it read, loses to B's write, reads again, and the two finish
    private class StaleRecordStore extends DelegatingStore {
        volatile @Nullable Thread a;
        final CountDownLatch aHasReadTheRecordWithNoStream = new CountDownLatch(1);
        final CountDownLatch bAppliedThatRecord = new CountDownLatch(1);
        final CountDownLatch aFinished = new CountDownLatch(1);
        final AtomicBoolean staleReplayWon = new AtomicBoolean(false);
        final AtomicBoolean completingWriteWon = new AtomicBoolean(false);
        private final String staleEventId;
        private final AtomicBoolean aPaused = new AtomicBoolean();
        private final AtomicInteger bReplays = new AtomicInteger();

        StaleRecordStore(String staleEventId) {
            this.staleEventId = staleEventId;
        }

        boolean isA() {
            return Thread.currentThread() == a;
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInput> findWithFirstSkippedInput(String sagaId) {
            Optional<StoredSagaWithFirstSkippedInput> found = super.findWithFirstSkippedInput(sagaId);
            // A waits once, on its first read that returns the record with no stream first, and keeps what it read
            if (isA() && found.map(StoredSagaWithFirstSkippedInput::firstSkippedInput).filter(first -> first.event().getId().equals(staleEventId)).isPresent()
                && aPaused.compareAndSet(false, true)) {
                aHasReadTheRecordWithNoStream.countDown();
                await(bAppliedThatRecord);
            }
            return found;
        }

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId,
                                              SkippedInput replayed) {
            boolean saved = super.compareAndSaveReplayed(saga, expectedVersion, quarantineId, replayed);
            if (isA()) {
                noteCompletingWrite(saga, saved);
                if (replayed.event().getId().equals(staleEventId) && saved) {
                    staleReplayWon.set(true);
                }
            } else if (bReplays.incrementAndGet() == 1) {
                bAppliedThatRecord.countDown();
                await(aFinished);
            }
            return saved;
        }

        @Override
        public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
            boolean saved = super.compareAndSaveQuarantineEnded(saga, expectedVersion, quarantineId);
            if (isA()) {
                noteCompletingWrite(saga, saved);
            }
            return saved;
        }

        private void noteCompletingWrite(StoredSaga saga, boolean saved) {
            if (saved && SagaStatus.COMPLETED.name().equals(saga.index().storedStatus())) {
                completingWriteWon.set(true);
            }
        }
    }

    private final class NoLongerFittingStore extends StaleRecordStore {
        final AtomicBoolean aSavedWithoutState = new AtomicBoolean(false);
        private final SagaCodec<Order> codec = SagaCodec.of(inMemory.stateCodec(), null);

        NoLongerFittingStore(String staleEventId) {
            super(staleEventId);
        }

        // Refuses only a state that holds a twice, which is what replaying a again on B's instance produces
        @Override
        public boolean fits(StoredSaga candidate) {
            return !holdsATwice(codec.decode(candidate).state().items());
        }

        // The stored instance with the fields the write sets replaced, as the release asked before the store was asked to do it
        @Override
        public boolean fitsWithoutState(StoredSaga saga, long expectedVersion) {
            StoredSaga stored = inMemory.find(saga.sagaId()).orElse(null);
            if (stored == null || stored.version() != expectedVersion) {
                return true;
            }
            return fits(SagaEnvelopes.withFieldsExceptStateOf(stored, saga));
        }

        // A save by A of a quarantined instance that is not releasing is a requarantine, the start of a release is not
        @Override
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            boolean saved = super.compareAndSaveWithoutState(saga, expectedVersion);
            if (saved && isA() && saga.index().quarantineId() != null && !saga.index().releasing()) {
                aSavedWithoutState.set(true);
            }
            return saved;
        }
    }
}
