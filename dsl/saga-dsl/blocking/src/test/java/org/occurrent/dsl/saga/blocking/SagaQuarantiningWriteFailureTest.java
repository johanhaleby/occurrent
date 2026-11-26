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

import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInput;
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInputKey;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The write that quarantines an instance also records the input it was quarantined on, so a failure of it must leave
 * the delivery failed. Returning normally would acknowledge an input that is neither applied nor held.
 */
@DisplayName("A quarantining write that fails")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaQuarantiningWriteFailureTest {

    private static final String ORDER = "o1";
    private static final Duration BUDGET = Duration.ofMinutes(1);
    private static final SagaRunnerConfig CONFIG = SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET);

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

    private final AtomicBoolean poisonFails = new AtomicBoolean(true);
    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    private final InMemoryEventStore eventStore = new InMemoryEventStore();
    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final List<Object> dispatched = new CopyOnWriteArrayList<>();
    private final AtomicInteger eventIds = new AtomicInteger();
    private final InMemorySagaStateStore<Order> inMemory = new InMemorySagaStateStore<>();
    private final FailingStore store = new FailingStore();
    private final SagaExecution<OrderEvent, Order, Object> execution =
            new SagaExecution<>("orders", saga(), store, dispatched::add, converter, CONFIG, now::get);

    private Saga<OrderEvent, Order, Object> saga() {
        return Saga.<OrderEvent, Order, Object>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(e.orderId(), List.of()))
                .evolve(ItemAdded.class, (state, e) -> state.with(e.item()))
                .react(ItemAdded.class, (state, e) -> {
                    if (poisonFails.get() && e.item().equals("poison")) {
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

    private SagaEnvelope<Order> instance() {
        return SagaEnvelopes.of(inMemory, null).find(ORDER).orElseThrow();
    }

    private static List<Throwable> causesAndSuppressed(Throwable thrown) {
        List<Throwable> all = new ArrayList<>();
        all.add(thrown);
        for (Throwable suppressed : thrown.getSuppressed()) {
            all.addAll(causesAndSuppressed(suppressed));
        }
        return all;
    }

    @Test
    void rethrows_the_delivery_failure_so_nothing_is_acknowledged_and_leaves_the_instance_active_with_its_failure_record() {
        execution.onCloudEvent(append(new OrderPlaced("e" + eventIds.incrementAndGet(), ORDER)));
        CloudEvent poison = append(new ItemAdded("e" + eventIds.incrementAndGet(), ORDER, "poison"));
        assertThat(catchThrowable(() -> execution.onCloudEvent(poison))).as("control: the first failure").isInstanceOf(IllegalStateException.class);
        now.set(now.get().plus(BUDGET));
        store.failsTheNextQuarantiningWrite.set(true);

        Throwable thrown = catchThrowable(() -> execution.onCloudEvent(poison));

        SagaEnvelope<Order> stored = instance();
        assertAll(
                () -> assertThat(store.quarantiningWrites.get()).as("control: the quarantining write was attempted").isEqualTo(1),
                () -> assertThat(thrown).as("the subscription sees the failure and does not acknowledge the event").isInstanceOf(IllegalStateException.class).hasMessage("cannot pack poison"),
                () -> assertThat(causesAndSuppressed(thrown)).extracting(Throwable::getMessage).contains(FailingStore.WRITE_FAILURE),
                () -> assertThat(stored.status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(stored.failure()).as("the failure record of the first failure is kept").isNotNull(),
                () -> assertThat(stored.failure().input()).isEqualTo(ORDER + "@2"),
                () -> assertThat(dispatched).as("nothing is dispatched").isEmpty()
        );
    }

    @Test
    void records_the_input_it_was_quarantined_on_once_the_redelivery_succeeds() {
        execution.onCloudEvent(append(new OrderPlaced("e" + eventIds.incrementAndGet(), ORDER)));
        CloudEvent poison = append(new ItemAdded("e" + eventIds.incrementAndGet(), ORDER, "poison"));
        catchThrowable(() -> execution.onCloudEvent(poison));
        now.set(now.get().plus(BUDGET));
        store.failsTheNextQuarantiningWrite.set(true);
        assertThat(catchThrowable(() -> execution.onCloudEvent(poison))).as("control: the quarantining write failed").isNotNull();

        Throwable redelivery = catchThrowable(() -> execution.onCloudEvent(poison));

        SagaEnvelope<Order> stored = instance();
        List<SkippedInput> records = inMemory.findSkippedInputs(ORDER, stored.quarantineId(), Integer.MAX_VALUE);
        assertAll(
                () -> assertThat(redelivery).as("the redelivery is acknowledged").isNull(),
                () -> assertThat(stored.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(records).extracting(SkippedInput::key).containsExactly(ORDER + "@2"),
                () -> assertThat(records).extracting(SkippedInput::event).containsExactly(poison)
        );
    }

    private final class FailingStore implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
        static final String WRITE_FAILURE = "the state store is unavailable";
        final AtomicBoolean failsTheNextQuarantiningWrite = new AtomicBoolean();
        final AtomicInteger quarantiningWrites = new AtomicInteger();

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
            quarantiningWrites.incrementAndGet();
            if (failsTheNextQuarantiningWrite.getAndSet(false)) {
                throw new IllegalStateException(WRITE_FAILURE);
            }
            return inMemory.compareAndSaveSkipped(saga, expectedVersion, quarantineId, input);
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
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            return inMemory.compareAndSaveReplayed(saga, expectedVersion, quarantineId, replayed);
        }

        @Override
        public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
            return inMemory.compareAndSaveQuarantineEnded(saga, expectedVersion, quarantineId);
        }

        @Override
        public void delete(String sagaId) {
            inMemory.delete(sagaId);
        }
    }
}
