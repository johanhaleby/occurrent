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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
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
import org.slf4j.LoggerFactory;

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
 * SpringMongoSagaStateStore on a standalone server holding data restored from a replica set. The runner refuses
 * quarantine on there, so with it off the instance skips what is addressed to it without recording it, rather than
 * failing on the refused write for every input and holding up the subscription.
 */
@DisplayName("A quarantined saga instance in a store that cannot write atomically, with quarantine off")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaSkipsUnrecordedTest {

    private static final String ORDER = "o1";
    private static final String OTHER_ORDER = "o2";
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
    void skips_an_input_addressed_to_it_without_recording_it_and_leaves_the_instance_as_it_was() {
        SagaEnvelope<Order> before = quarantinedEarlier();
        SagaExecution<OrderEvent, Order, Object> execution = executionWithQuarantineOff();
        List<SkippedInput> recordsBefore = inMemory.findSkippedInputs(ORDER, before.quarantineId(), Integer.MAX_VALUE);

        Throwable thrown = catchThrowable(() -> execution.onCloudEvent(append(ORDER, new ItemAdded(nextEventId(), ORDER, "live"))));

        SagaEnvelope<Order> after = instance(ORDER);
        assertAll(
                () -> assertThat(thrown).as("the delivery returns, so the subscription moves past the input").isNull(),
                () -> assertThat(store.skipWrites).as("no skip write is attempted").hasValue(0),
                () -> assertThat(after.version()).as("the instance's version").isEqualTo(before.version()),
                () -> assertThat(after.quarantineSkipCount()).as("the instance's skip count").isEqualTo(before.quarantineSkipCount()),
                () -> assertThat(after.status()).as("still quarantined").isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(inMemory.findSkippedInputs(ORDER, before.quarantineId(), Integer.MAX_VALUE)).as("no record is added").isEqualTo(recordsBefore),
                () -> assertThat(dispatched).as("the input is not applied").isEmpty()
        );
    }

    @Test
    void processes_the_next_input_to_another_instance_after_skipping_one() {
        quarantinedEarlier();
        SagaExecution<OrderEvent, Order, Object> execution = executionWithQuarantineOff();
        Throwable skipped = catchThrowable(() -> execution.onCloudEvent(append(ORDER, new ItemAdded(nextEventId(), ORDER, "live"))));

        Throwable placed = catchThrowable(() -> execution.onCloudEvent(append(OTHER_ORDER, new OrderPlaced(nextEventId(), OTHER_ORDER))));
        Throwable added = catchThrowable(() -> execution.onCloudEvent(append(OTHER_ORDER, new ItemAdded(nextEventId(), OTHER_ORDER, "book"))));

        assertAll(
                () -> assertThat(skipped).as("the skipped input does not hold up the subscription").isNull(),
                () -> assertThat(placed).as("the input that starts the other instance").isNull(),
                () -> assertThat(added).as("the input that updates the other instance").isNull(),
                () -> assertThat(instance(OTHER_ORDER).state().items()).as("the other instance applied its input").containsExactly("book"),
                () -> assertThat(dispatched).as("and issued its command").containsExactly(new Pack(OTHER_ORDER, "book"))
        );
    }

    @Test
    void says_in_a_warning_which_input_it_skipped_without_recording() {
        quarantinedEarlier();
        SagaExecution<OrderEvent, Order, Object> execution = executionWithQuarantineOff();
        CloudEvent live = append(ORDER, new ItemAdded(nextEventId(), ORDER, "live"));
        String key = ORDER + "@" + live.getExtension("streamversion");
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Logger executionLog = (Logger) LoggerFactory.getLogger(SagaExecution.class);
        executionLog.addAppender(appender);
        try {
            catchThrowable(() -> execution.onCloudEvent(live));

            List<ILoggingEvent> logged;
            synchronized (appender) {
                logged = new ArrayList<>(appender.list);
            }
            assertThat(logged)
                    .filteredOn(event -> event.getLevel() == Level.WARN)
                    .extracting(ILoggingEvent::getFormattedMessage)
                    .anySatisfy(message -> assertThat(message).contains("'" + key + "'").contains("without recording it"));
        } finally {
            executionLog.detachAppender(appender);
            appender.stop();
        }
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
