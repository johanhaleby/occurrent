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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A state a replay saved that stops decoding once the release has begun makes the read of the instance and its first
 * record fail on every later call. The failure must count against the budget of that record, so the instance stays
 * releasing while it is within it and is quarantined again past it, rather than stay releasing for good with every
 * poll and every {@code release} throwing before anything is recorded.
 */
@DisplayName("A release whose read fails on a state that no longer decodes")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaReleaseReadFailureTest {

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
    private final FailsToDecodeAfterTheFirstReplay store = new FailsToDecodeAfterTheFirstReplay();
    private final SagaExecution<OrderEvent, Order, Object> execution = new SagaExecution<>("orders", saga(), store, dispatched::add, converter, CONFIG, now::get);
    private String keyOfA = "";

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

    private SagaEnvelope<Order> instance() {
        return envelopes.findWithoutState(ORDER).orElseThrow();
    }

    // Quarantined on poison, with a and b recorded after it. The state decodes until the first replay of a release has saved
    @BeforeEach
    void quarantinedOnPoisonWithAAndBRecorded() {
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        CloudEvent poison = append(new ItemAdded(nextEventId(), ORDER, "poison"));
        catchThrowable(() -> execution.onCloudEvent(poison));
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> execution.onCloudEvent(poison));
        poisonFails = false;
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "a")));
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "b")));
        List<SkippedInput> records = inMemory.findSkippedInputs(ORDER, Objects.requireNonNull(instance().quarantineId()), Integer.MAX_VALUE);
        assertThat(records).as("records of the quarantine").hasSize(3);
        keyOfA = records.get(1).key();
        store.undecodableAfterTheFirstReplay = true;
    }

    private void pollAfterTheInterval() {
        now.set(now.get().plus(CONFIG.timerPollInterval()).plusMillis(1));
        execution.pollTimers();
    }

    private String evidence(@Nullable ReleaseResult result, @Nullable Throwable thrown) {
        SagaEnvelope<Order> current = instance();
        return "result " + result + ", threw " + thrown + ", status " + current.status() + ", releasing " + current.isReleasing() + ", failure "
               + current.failure() + ", records " + inMemory.findSkippedInputs(ORDER, Objects.requireNonNullElse(current.quarantineId(), ""), Integer.MAX_VALUE).stream().map(SkippedInput::key).toList()
               + ", dispatched " + dispatched;
    }

    @Test
    void a_release_charges_the_failure_to_the_next_record_and_leaves_the_instance_releasing_inside_its_budget() {
        AtomicReference<@Nullable ReleaseResult> result = new AtomicReference<>();

        Throwable thrown = catchThrowable(() -> result.set(execution.release(ORDER)));

        SagaEnvelope<Order> after = instance();
        String evidence = evidence(result.get(), thrown);
        assertAll(
                () -> assertThat(store.stateStoppedDecoding).as("the state stopped decoding after the first replay. " + evidence).isTrue(),
                () -> assertThat(thrown).as("the failed read is recorded and not thrown. " + evidence).isNull(),
                () -> assertThat(result.get()).as(evidence).isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(after.isReleasing()).as("the instance is still releasing. " + evidence).isTrue(),
                () -> assertThat(after.failure()).as(evidence).isNotNull(),
                () -> assertThat(Objects.requireNonNull(after.failure()).input()).as("the failure is on the second record. " + evidence).isEqualTo(keyOfA),
                () -> assertThat(dispatched).as("only the first record was applied. " + evidence).containsExactly(new Pack(ORDER, "poison"))
        );
    }

    @Test
    void a_timer_poll_goes_on_charging_the_failure_to_the_record_and_quarantines_the_instance_again_past_its_budget() {
        catchThrowable(() -> execution.release(ORDER));
        pollAfterTheInterval();
        SagaEnvelope<Order> insideTheBudget = instance();
        String insideEvidence = evidence(null, null);
        now.set(now.get().plus(BUDGET));
        execution.pollTimers();

        SagaEnvelope<Order> after = instance();
        String evidence = evidence(null, null);
        assertAll(
                () -> assertThat(insideTheBudget.isReleasing()).as("inside the budget the instance stays releasing. " + insideEvidence).isTrue(),
                () -> assertThat(insideTheBudget.failure()).as(insideEvidence).isNotNull(),
                () -> assertThat(Objects.requireNonNull(insideTheBudget.failure()).input()).as("on the second record. " + insideEvidence).isEqualTo(keyOfA),
                () -> assertThat(after.isReleasing()).as("past the budget the release has ended. " + evidence).isFalse(),
                () -> assertThat(after.status()).as(evidence).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(Objects.requireNonNull(after.failure()).input()).as("quarantined again on the second record. " + evidence).isEqualTo(keyOfA),
                () -> assertThat(inMemory.findSkippedInputs(ORDER, Objects.requireNonNull(after.quarantineId()), Integer.MAX_VALUE)).as("the records after the first stay. " + evidence).hasSize(2)
        );
    }

    @Test
    void releasing_an_instance_already_releasing_goes_on_with_it_and_charges_the_failure_instead_of_refusing_the_decoding() {
        catchThrowable(() -> execution.release(ORDER));
        AtomicReference<@Nullable ReleaseResult> inside = new AtomicReference<>();
        Throwable insideThrown = catchThrowable(() -> inside.set(execution.release(ORDER)));
        String insideEvidence = evidence(inside.get(), insideThrown);
        now.set(now.get().plus(BUDGET));
        AtomicReference<@Nullable ReleaseResult> past = new AtomicReference<>();
        Throwable pastThrown = catchThrowable(() -> past.set(execution.release(ORDER)));

        SagaEnvelope<Order> after = instance();
        String evidence = evidence(past.get(), pastThrown);
        assertAll(
                () -> assertThat(insideThrown).as("inside the budget it does not throw. " + insideEvidence).isNull(),
                () -> assertThat(inside.get()).as(insideEvidence).isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(pastThrown).as("past the budget it does not throw. " + evidence).isNull(),
                () -> assertThat(past.get()).as(evidence).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(after.isReleasing()).as(evidence).isFalse(),
                () -> assertThat(Objects.requireNonNull(after.failure()).input()).as("quarantined again on the second record. " + evidence).isEqualTo(keyOfA)
        );
    }

    @Test
    void a_state_that_decodes_again_is_released_to_the_end() {
        catchThrowable(() -> execution.release(ORDER));
        store.undecodable = false;

        ReleaseResult result = execution.release(ORDER);

        SagaEnvelope<Order> after = envelopes.find(ORDER).orElseThrow();
        assertAll(
                () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(after.state().items()).containsExactly("poison", "a", "b"),
                () -> assertThat(dispatched).containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b"))
        );
    }

    private class Delegating implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
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

    // Reads the state through a codec that refuses once a replay of a release has been saved, so the read of the instance
    // whole fails while the read without its state works
    private final class FailsToDecodeAfterTheFirstReplay extends Delegating {
        volatile boolean undecodableAfterTheFirstReplay = false;
        volatile boolean undecodable = false;
        volatile boolean stateStoppedDecoding = false;

        @Override
        public SagaStateCodec<Order> stateCodec() {
            SagaStateCodec<Order> codec = inMemory.stateCodec();
            return new SagaStateCodec<>() {
                @Override
                public Object encode(Order state) {
                    return codec.encode(state);
                }

                @Override
                public Order decode(Object encoded) {
                    if (undecodable) {
                        throw new IllegalStateException("the state can no longer be decoded");
                    }
                    return codec.decode(encoded);
                }
            };
        }

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            boolean saved = super.compareAndSaveReplayed(saga, expectedVersion, quarantineId, replayed);
            if (saved && undecodableAfterTheFirstReplay) {
                undecodableAfterTheFirstReplay = false;
                undecodable = true;
                stateStoppedDecoding = true;
            }
            return saved;
        }
    }
}
