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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A release applies the first record under the quarantine id the instance was read with, and an instance whose read
 * returns no record of the input it stopped on is not turned active. The release refuses it at once, deletes no record
 * and dispatches nothing, and a later release applies every input once the store returns the records again.
 */
@DisplayName("A release whose read of the instance returns no record of the input it stopped on")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaReleaseStaleRecordsReadTest {

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

    private volatile boolean poisonFails = false;
    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    private final InMemoryEventStore eventStore = new InMemoryEventStore();
    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final List<Object> dispatched = new CopyOnWriteArrayList<>();
    private final AtomicInteger eventIds = new AtomicInteger();
    private final InMemorySagaStateStore<Order> inMemory = new InMemorySagaStateStore<>();
    private final SagaEnvelopes<Order> envelopes = SagaEnvelopes.of(inMemory, null);
    private final MissesTheRecords store = new MissesTheRecords();
    private SagaExecution<OrderEvent, Order, Object> execution;
    private String quarantineId;

    private Saga<OrderEvent, Order, Object> saga() {
        return Saga.<OrderEvent, Order, Object>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(e.orderId(), List.of()))
                .react(OrderPlaced.class, (state, e) -> List.of())
                .evolve(ItemAdded.class, (state, e) -> state.with(e.item()))
                .react(ItemAdded.class, (state, e) -> {
                    if (poisonFails && e.item().equals("poison")) {
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.orderId(), e.item())));
                })
                .build();
    }

    private CloudEvent append(OrderEvent event) {
        eventStore.write(ORDER, converter.toCloudEvents(List.of(event)));
        return eventStore.read(ORDER).eventList().getLast();
    }

    private String nextEventId() {
        return "e" + eventIds.incrementAndGet();
    }

    // An instance quarantined on "poison", with the records of "a" and "b" that it skipped afterwards
    @BeforeEach
    void aQuarantinedInstanceThatHasSkippedTwoInputs() {
        execution = new SagaExecution<>("orders", saga(), store, dispatched::add, converter, CONFIG, now::get);
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        CloudEvent poison = append(new ItemAdded(nextEventId(), ORDER, "poison"));
        catchThrowable(() -> execution.onCloudEvent(poison));
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> execution.onCloudEvent(poison));
        poisonFails = false;
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "a")));
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "b")));
        SagaEnvelope<Order> quarantined = envelopes.find(ORDER).orElseThrow();
        assertThat(quarantined.status()).isEqualTo(SagaStatus.QUARANTINED);
        quarantineId = quarantined.quarantineId();
        assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, 10)).as("the records the instance skipped").isNotEmpty();
        dispatched.clear();
    }

    @Test
    void refuses_at_once_and_does_not_turn_the_instance_active_or_delete_a_record_while_the_read_returns_no_record() {
        List<SkippedInput> recordsBefore = inMemory.findSkippedInputs(ORDER, quarantineId, 10);
        store.hidesRecords = true;
        store.pairReads.set(0);

        Throwable thrown = catchThrowable(() -> execution.release(ORDER));

        SagaEnvelope<Order> after = envelopes.find(ORDER).orElseThrow();
        String evidence = "thrown " + thrown + ", status " + after.status() + ", dispatched " + dispatched;
        assertAll(
                () -> assertThat(thrown).as("a release that reads no record of the input it stopped on is refused, naming the input. " + evidence)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("'o1@2'"),
                () -> assertThat(store.pairReads.get()).as("the refusal comes from the first read and not from a retry of it. " + evidence).isEqualTo(1),
                () -> assertThat(after.status()).as("the instance is not turned active. " + evidence).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, 10)).as("no record is deleted, and the release has put the input it replays first back as a record").containsAll(recordsBefore),
                () -> assertThat(dispatched).as("nothing is dispatched. " + evidence).isEmpty()
        );
    }

    @Test
    void applies_every_input_once_the_read_returns_the_records_again() {
        store.hidesRecords = true;
        catchThrowable(() -> execution.release(ORDER));
        store.hidesRecords = false;

        ReleaseResult result = execution.release(ORDER);

        SagaEnvelope<Order> after = envelopes.find(ORDER).orElseThrow();
        String evidence = "released " + result + ", status " + after.status() + ", items " + after.state().items() + ", dispatched " + dispatched;
        assertAll(
                () -> assertThat(result).as(evidence).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(after.status()).as(evidence).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(after.state().items()).as(evidence).containsExactly("poison", "a", "b"),
                () -> assertThat(dispatched).as(evidence).containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b")),
                () -> assertThat(inMemory.findSkippedInputs(ORDER, quarantineId, 10)).as(evidence).isEmpty()
        );
    }

    // A store that passes every call on, except that its read of the instance and its first record returns no record while it hides them
    private final class MissesTheRecords implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
        volatile boolean hidesRecords = false;
        final AtomicInteger pairReads = new AtomicInteger();

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
            pairReads.incrementAndGet();
            Optional<StoredSagaWithFirstSkippedInput> read = inMemory.findWithFirstSkippedInput(sagaId);
            if (hidesRecords) {
                return read.map(whole -> new StoredSagaWithFirstSkippedInput(whole.saga(), null));
            }
            return read;
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInputKey> findWithFirstSkippedInputKey(String sagaId) {
            Optional<StoredSagaWithFirstSkippedInputKey> read = inMemory.findWithFirstSkippedInputKey(sagaId);
            if (hidesRecords) {
                return read.map(whole -> new StoredSagaWithFirstSkippedInputKey(whole.saga(), null));
            }
            return read;
        }

        @Override
        public void delete(String sagaId) {
            inMemory.delete(sagaId);
        }
    }
}
