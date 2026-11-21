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
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A release whose replay of a record fails, after another call applied and deleted that record, must leave the instance
 * releasing. It never quarantines the instance again on a record that no longer exists, and a later release applies
 * every held input once. The store here serves the records of an older moment from its separate read of them, so the
 * outcome rests on the instance and its first record being read together.
 */
@DisplayName("A release whose replay fails on a record another call applied")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaReleaseFailureOnARecordAnotherCallAppliedTest {

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
    private volatile boolean aFails = false;
    private final AtomicReference<@Nullable Runnable> whileAFails = new AtomicReference<>();
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
                .react(OrderPlaced.class, (state, e) -> List.of())
                .evolve(ItemAdded.class, (state, e) -> state.with(e.item()))
                .react(ItemAdded.class, (state, e) -> {
                    if (poisonFails && e.item().equals("poison")) {
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    if (aFails && e.item().equals("a")) {
                        Runnable meanwhile = whileAFails.getAndSet(null);
                        if (meanwhile != null) {
                            aFails = false;
                            meanwhile.run();
                        }
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.orderId(), e.item())));
                })
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

    private static CloudEvent withoutStream(CloudEvent event) {
        return CloudEventBuilder.v1(event)
                .withoutExtension(OccurrentCloudEventExtension.STREAM_ID)
                .withoutExtension(OccurrentCloudEventExtension.STREAM_VERSION)
                .build();
    }

    private SagaEnvelope<Order> instance() {
        return envelopes.find(ORDER).orElseThrow();
    }

    @Test
    void leaves_the_instance_releasing_and_a_later_release_applies_every_input_once() {
        OlderRecordsStore older = new OlderRecordsStore();
        SagaExecution<OrderEvent, Order, Object> execution = execution(older);
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        CloudEvent poison = append(new ItemAdded(nextEventId(), ORDER, "poison"));
        catchThrowable(() -> execution.onCloudEvent(poison));
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> execution.onCloudEvent(poison));
        poisonFails = false;
        execution.onCloudEvent(withoutStream(append(new ItemAdded(nextEventId(), ORDER, "a"))));
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "b")));
        String quarantineId = Objects.requireNonNull(instance().quarantineId());
        // the input "a" has no stream, so only its record shows whether it was applied. It fails once in a release, so the instance holds a failure on it, and a second failure past its budget quarantines the instance again
        aFails = true;
        assertThat(execution.release(ORDER)).as("the first failure on a stays within its budget").isEqualTo(ReleaseResult.STILL_RELEASING);
        now.set(now.get().plus(BUDGET));
        SagaExecution<OrderEvent, Order, Object> otherRelease = execution(new StopsAfterTheFirstReplay());
        whileAFails.set(() -> {
            older.recordsAsTheyAreNow(quarantineId);
            catchThrowable(() -> otherRelease.release(ORDER));
        });

        ReleaseResult result = execution.release(ORDER);
        SagaEnvelope<Order> afterTheFailure = instance();
        AtomicReference<@Nullable ReleaseResult> later = new AtomicReference<>();
        Throwable laterFailure = catchThrowable(() -> later.set(execution.release(ORDER)));
        SagaEnvelope<Order> after = instance();

        String evidence = "result " + result + ", status after the failure " + afterTheFailure.status() + " releasing " + afterTheFailure.isReleasing()
                          + ", later release returned " + later.get() + " and threw " + laterFailure + ", status " + after.status() + ", items " + after.state().items() + ", dispatched " + dispatched;
        assertAll(
                () -> assertThat(older.otherCallRan).as("another call applied the record in between. " + evidence).isTrue(),
                () -> assertThat(result).as("the release does not quarantine the instance again on a record that is gone. " + evidence).isNotEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(afterTheFailure.isReleasing()).as("the instance is still releasing. " + evidence).isTrue(),
                () -> assertThat(laterFailure).as("a later release goes through. " + evidence).isNull(),
                () -> assertThat(later.get()).as(evidence).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(after.status()).as(evidence).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(after.state().items()).as("every held input is applied once. " + evidence).containsExactly("poison", "a", "b"),
                () -> assertThat(dispatched).as("and its commands dispatched once. " + evidence).containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b"))
        );
    }

    private class Delegating implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
        @Override
        public boolean fits(StoredSaga candidate) {
            return inMemory.fits(candidate);
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
        public void delete(String sagaId) {
            inMemory.delete(sagaId);
        }
    }

    // Its separate read of the records serves once the records as they were when recordsAsTheyAreNow was called, which is
    // older than the store after that. Its read of the instance and its first record is the store's own
    private final class OlderRecordsStore extends Delegating {
        private final AtomicReference<@Nullable List<SkippedInput>> older = new AtomicReference<>();
        volatile boolean otherCallRan = false;

        void recordsAsTheyAreNow(String quarantineId) {
            older.set(inMemory.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE));
            otherCallRan = true;
        }

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            List<SkippedInput> served = older.getAndSet(null);
            if (served == null) {
                return super.findSkippedInputs(sagaId, quarantineId, limit);
            }
            return served.stream().limit(limit).toList();
        }
    }

    // Stops the release it serves the way a crash does, once it has applied the first record and reads for the next one
    private final class StopsAfterTheFirstReplay extends Delegating {
        private final AtomicInteger reads = new AtomicInteger();

        @Override
        public Optional<StoredSagaWithFirstSkippedInput> findWithFirstSkippedInput(String sagaId) {
            if (reads.incrementAndGet() == 3) {
                throw new OutOfMemoryError("the other release stops here");
            }
            return super.findWithFirstSkippedInput(sagaId);
        }
    }
}
