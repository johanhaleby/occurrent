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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A quarantined instance in a store that cannot write a skip and its record in one atomic write, such as
 * SpringMongoSagaStateStore on a standalone server holding data restored from a replica set. With quarantine off the
 * delivery of an input addressed to that instance must fail rather than return, since returning acknowledges an input
 * that was neither applied nor recorded for a release to replay.
 */
@DisplayName("A quarantined saga instance in a store that cannot write atomically, with quarantine off")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaRefusesAnInputItCannotRecordTest {

    private static final String ORDER = "o1";
    private static final Duration BUDGET = Duration.ofMinutes(1);
    private static final SagaRunnerConfig QUARANTINE_ON = SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET)
            .withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT);
    private static final SagaRunnerConfig QUARANTINE_OFF = SagaRunnerConfig.defaults().disableQuarantine()
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
    private final StoreThatCannotWriteAtomically store = new StoreThatCannotWriteAtomically();
    private final SagaEnvelopes<Order> envelopes = SagaEnvelopes.of(inMemory, null);

    private Saga<OrderEvent, Order, Object> saga() {
        return Saga.<OrderEvent, Order, Object>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(e.orderId(), List.of()))
                .evolve(ItemAdded.class, (state, e) -> state.with(e.item()))
                .react(ItemAdded.class, (state, e) -> {
                    if (poisonFails && e.item().equals("poison")) {
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.orderId(), e.item())));
                })
                .build();
    }

    private CloudEvent append(String orderId, OrderEvent event) {
        eventStore.write(orderId, converter.toCloudEvents(List.of(event)));
        return eventStore.read(orderId).eventList().getLast();
    }

    private String nextEventId() {
        return "e" + eventIds.incrementAndGet();
    }

    private SagaEnvelope<Order> instance(String orderId) {
        return envelopes.find(orderId).orElseThrow();
    }

    // Quarantined on "poison" by an execution that can record, which is what wrote the data a replica set restored here
    private SagaEnvelope<Order> quarantinedEarlier() {
        SagaExecution<OrderEvent, Order, Object> recording = new SagaExecution<>("orders", saga(), inMemory, dispatched::add, converter, QUARANTINE_ON, now::get);
        recording.onCloudEvent(append(ORDER, new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        CloudEvent poison = append(ORDER, new ItemAdded(nextEventId(), ORDER, "poison"));
        catchThrowable(() -> recording.onCloudEvent(poison));
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> recording.onCloudEvent(poison));
        poisonFails = false;
        SagaEnvelope<Order> quarantined = instance(ORDER);
        assertThat(quarantined.status()).as("control: quarantined").isEqualTo(SagaStatus.QUARANTINED);
        assertThat(quarantined.quarantineId()).as("control: with a quarantine id").isNotNull();
        dispatched.clear();
        return quarantined;
    }

    private SagaExecution<OrderEvent, Order, Object> executionWithQuarantineOff() {
        return new SagaExecution<>("orders", saga(), store, dispatched::add, converter, QUARANTINE_OFF, now::get);
    }

    @Test
    void refuses_an_input_addressed_to_it_naming_the_instance_the_input_and_the_ways_out_and_changes_nothing() {
        SagaEnvelope<Order> before = quarantinedEarlier();
        SagaExecution<OrderEvent, Order, Object> execution = executionWithQuarantineOff();
        List<SkippedInput> recordsBefore = inMemory.findSkippedInputs(ORDER, before.quarantineId(), Integer.MAX_VALUE);
        CloudEvent live = append(ORDER, new ItemAdded(nextEventId(), ORDER, "live"));

        Throwable thrown = catchThrowable(() -> execution.onCloudEvent(live));

        SagaEnvelope<Order> after = instance(ORDER);
        assertAll(
                () -> assertThat(thrown).as("the delivery fails, so the subscription does not move past the input")
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("orders")
                        .hasMessageContaining("'" + ORDER + "'")
                        .hasMessageContaining("quarantined")
                        .hasMessageContaining(inputKey(live))
                        .hasMessageContaining("replica set")
                        .hasMessageContaining("by hand"),
                () -> assertThat(store.skipWrites).as("no skip write is attempted").hasValue(0),
                () -> assertThat(after.version()).as("the instance's version").isEqualTo(before.version()),
                () -> assertThat(after.quarantineSkipCount()).as("the instance's skip count").isEqualTo(before.quarantineSkipCount()),
                () -> assertThat(after.status()).as("still quarantined").isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(inMemory.findSkippedInputs(ORDER, before.quarantineId(), Integer.MAX_VALUE)).as("no record is added").isEqualTo(recordsBefore),
                () -> assertThat(dispatched).as("the input is not applied").isEmpty()
        );
    }

    @Test
    void refuses_a_redelivery_of_the_same_input_again_since_the_first_delivery_was_not_acknowledged() {
        quarantinedEarlier();
        SagaExecution<OrderEvent, Order, Object> execution = executionWithQuarantineOff();
        CloudEvent live = append(ORDER, new ItemAdded(nextEventId(), ORDER, "live"));
        Throwable first = catchThrowable(() -> execution.onCloudEvent(live));

        Throwable redelivered = catchThrowable(() -> execution.onCloudEvent(live));

        assertAll(
                () -> assertThat(first).isInstanceOf(IllegalStateException.class),
                () -> assertThat(redelivered).as("not swallowed as an input already handled")
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining(inputKey(live)),
                () -> assertThat(store.skipWrites).hasValue(0),
                () -> assertThat(dispatched).isEmpty()
        );
    }

    @Test
    void records_the_redelivered_input_under_the_quarantine_id_once_the_data_is_on_a_store_that_records_skips() {
        SagaEnvelope<Order> before = quarantinedEarlier();
        CloudEvent live = append(ORDER, new ItemAdded(nextEventId(), ORDER, "live"));
        Throwable refused = catchThrowable(() -> executionWithQuarantineOff().onCloudEvent(live));
        SagaExecution<OrderEvent, Order, Object> recording = new SagaExecution<>("orders", saga(), inMemory, dispatched::add, converter, QUARANTINE_ON, now::get);

        Throwable redelivered = catchThrowable(() -> recording.onCloudEvent(live));

        List<SkippedInput> records = inMemory.findSkippedInputs(ORDER, before.quarantineId(), Integer.MAX_VALUE);
        assertAll(
                () -> assertThat(refused).as("control: refused first").isInstanceOf(IllegalStateException.class),
                () -> assertThat(redelivered).as("the redelivery is accepted").isNull(),
                () -> assertThat(records).as("recorded as a skipped input a release can replay")
                        .extracting(SkippedInput::key).contains(inputKey(live)),
                () -> assertThat(records).filteredOn(record -> record.key().equals(inputKey(live)))
                        .extracting(record -> record.event().getId()).containsExactly(live.getId()),
                () -> assertThat(instance(ORDER).status()).as("still quarantined").isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(dispatched).as("not applied").isEmpty()
        );
    }

    private static String inputKey(CloudEvent event) {
        return ORDER + "@" + event.getExtension("streamversion");
    }

    // Records skips like the in-memory store, but cannot write atomically, so any skip write it is asked for is a failure
    // as the refused transaction is
    private final class StoreThatCannotWriteAtomically implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
        final AtomicInteger skipWrites = new AtomicInteger();

        @Override
        public void requireAtomicTransitions() {
            throw new IllegalStateException("this store cannot write atomically");
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
            skipWrites.incrementAndGet();
            throw new IllegalStateException("the server refuses the transaction");
        }

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            return inMemory.findSkippedInputs(sagaId, quarantineId, limit);
        }

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            throw new IllegalStateException("the server refuses the transaction");
        }

        @Override
        public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
            throw new IllegalStateException("the server refuses the transaction");
        }

        @Override
        public void delete(String sagaId) {
            inMemory.delete(sagaId);
        }
    }
}
