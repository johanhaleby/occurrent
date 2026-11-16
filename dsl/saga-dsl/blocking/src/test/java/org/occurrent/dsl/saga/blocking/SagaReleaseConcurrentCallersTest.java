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
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
                .build();
    }

    private SagaExecution<OrderEvent, Order, Object> execution(SagaStateStore<Order> store) {
        return new SagaExecution<>("orders", saga(), store, dispatched::add, converter, CONFIG, event -> true, now::get, eventStore);
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
    void a_second_release_call_writing_the_head_record_after_the_first_recorded_it_does_not_lose_the_input() throws Exception {
        HeadRacingStore store = new HeadRacingStore();
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
                () -> assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE)).as("no record is left. " + evidence).isEmpty()
        );
    }

    @Test
    void a_second_release_call_moving_the_head_record_while_the_first_has_started_and_not_read_the_records_applies_the_input_once_and_before_every_later_record() throws Exception {
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
        // The record an earlier release left for the input it was quarantined on, at a seq after the records of the inputs it skipped
        inMemory.recordSkippedInput(ORDER, quarantineId, new SkippedInput(1_000_000, poison.getExtension(OccurrentCloudEventExtension.STREAM_ID) + "@" + poison.getExtension(OccurrentCloudEventExtension.STREAM_VERSION)));

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
        assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE)).hasSize(1);
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
                () -> assertThat(after.state().items()).as("the input recorded after the start event is applied. " + evidence).containsExactly("r1"),
                () -> assertThat(dispatched).as("the commands it produced. " + evidence).containsExactly(new Pack(ORDER, "r1"))
        );
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
        public void recordSkippedInput(String sagaId, String quarantineId, SkippedInput input) {
            inMemory.recordSkippedInput(sagaId, quarantineId, input);
        }

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            return inMemory.findSkippedInputs(sagaId, quarantineId, limit);
        }

        @Override
        public void recordSkippedInputAtHead(String sagaId, String quarantineId, SkippedInput head) {
            inMemory.recordSkippedInputAtHead(sagaId, quarantineId, head);
        }

        @Override
        public void deleteSkippedInput(String sagaId, String quarantineId, SkippedInput input) {
            inMemory.deleteSkippedInput(sagaId, quarantineId, input);
        }

        @Override
        public void deleteSkippedInputs(String sagaId, String quarantineId) {
            inMemory.deleteSkippedInputs(sagaId, quarantineId);
        }

        @Override
        public void delete(String sagaId) {
            inMemory.delete(sagaId);
        }
    }

    // Call order: B reads, A reads, A writes the head record, B writes the head record over it and pauses, A wins the
    // start write and finishes the release, B resumes. The head write is the one recordSkippedInputAtHead makes
    private final class HeadRacingStore extends DelegatingStore {
        volatile @Nullable Thread b;
        final CountDownLatch bHasRead = new CountDownLatch(1);
        final CountDownLatch aRecorded = new CountDownLatch(1);
        final CountDownLatch bWroteTheHead = new CountDownLatch(1);
        final CountDownLatch aFinished = new CountDownLatch(1);
        private final AtomicInteger bReads = new AtomicInteger();

        private boolean isB() {
            return Thread.currentThread() == b;
        }

        @Override
        public Optional<StoredSaga> find(String sagaId) {
            Optional<StoredSaga> found = super.find(sagaId);
            if (isB() && bReads.incrementAndGet() == 1) {
                bHasRead.countDown();
            }
            return found;
        }

        @Override
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            if (!isB() && aRecorded.getCount() == 0 && bWroteTheHead.getCount() > 0) {
                await(bWroteTheHead);
            }
            return super.compareAndSaveWithoutState(saga, expectedVersion);
        }

        @Override
        public void recordSkippedInputAtHead(String sagaId, String quarantineId, SkippedInput head) {
            if (isB()) {
                await(aRecorded);
                super.recordSkippedInputAtHead(sagaId, quarantineId, head);
                bWroteTheHead.countDown();
                await(aFinished);
                return;
            }
            super.recordSkippedInputAtHead(sagaId, quarantineId, head);
            aRecorded.countDown();
        }
    }

    // Call order: B reads the instance, A writes the head record and starts the release and pauses before it reads the
    // records, B writes the head record and pauses, A reads the records and finishes the release, B resumes
    private final class HeldReadStore extends DelegatingStore {
        volatile @Nullable Thread b;
        final CountDownLatch bHasRead = new CountDownLatch(1);
        final CountDownLatch aIsAboutToReadTheRecords = new CountDownLatch(1);
        final CountDownLatch bWroteTheHead = new CountDownLatch(1);
        final CountDownLatch aFinished = new CountDownLatch(1);
        private final AtomicInteger bReads = new AtomicInteger();
        private final AtomicInteger aRecordReads = new AtomicInteger();

        private boolean isB() {
            return Thread.currentThread() == b;
        }

        @Override
        public Optional<SagaEnvelope<Order>> find(String sagaId) {
            Optional<SagaEnvelope<Order>> found = super.find(sagaId);
            if (isB() && bReads.incrementAndGet() == 1) {
                bHasRead.countDown();
            }
            return found;
        }

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            if (!isB() && aRecordReads.incrementAndGet() == 1) {
                aIsAboutToReadTheRecords.countDown();
                await(bWroteTheHead);
            }
            return super.findSkippedInputs(sagaId, quarantineId, limit);
        }

        @Override
        public void recordSkippedInputAtHead(String sagaId, String quarantineId, SkippedInput head) {
            if (isB()) {
                await(aIsAboutToReadTheRecords);
                super.recordSkippedInputAtHead(sagaId, quarantineId, head);
                bWroteTheHead.countDown();
                await(aFinished);
                return;
            }
            super.recordSkippedInputAtHead(sagaId, quarantineId, head);
        }
    }

    // Call order: R starts the release and waits, P reads the instance that has not started and waits before it reads the
    // records, R replays the start event, deletes its record and waits before it reads the records again, P reads the
    // record after it and replays it against what it read, deletes it, and the two finish
    private final class PollerRacingStore extends DelegatingStore {
        volatile @Nullable Thread r;
        volatile @Nullable Thread p;
        final CountDownLatch rHasStartedTheRelease = new CountDownLatch(1);
        final CountDownLatch pHasReadTheInstance = new CountDownLatch(1);
        final CountDownLatch rDeletedTheStartRecord = new CountDownLatch(1);
        final CountDownLatch pDeletedItsRecord = new CountDownLatch(1);
        private final AtomicInteger rWrites = new AtomicInteger();
        private final AtomicInteger rRecordReads = new AtomicInteger();
        private final AtomicInteger pRecordReads = new AtomicInteger();

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
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            if (isP() && pRecordReads.incrementAndGet() == 1) {
                pHasReadTheInstance.countDown();
                await(rDeletedTheStartRecord);
            }
            if (isR() && rRecordReads.incrementAndGet() == 2) {
                await(pDeletedItsRecord);
            }
            return super.findSkippedInputs(sagaId, quarantineId, limit);
        }

        @Override
        public void deleteSkippedInput(String sagaId, String quarantineId, SkippedInput input) {
            super.deleteSkippedInput(sagaId, quarantineId, input);
            if (isR() && input.seq() < 0) {
                rDeletedTheStartRecord.countDown();
            }
            if (isP() && input.seq() >= 0) {
                pDeletedItsRecord.countDown();
            }
        }
    }
}
