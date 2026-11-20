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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A delivery to a quarantined instance whose state no longer decodes is skipped only when the skip is recorded. When it
 * cannot be recorded, the instance stopped being quarantined, is gone, or the write kept losing, the load failure
 * propagates, so the event is not acknowledged unrecorded and not applied.
 */
@DisplayName("A delivery to a quarantined saga instance whose state cannot be decoded")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaRecordedTheSkipTest {

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

    private static final String DECODE_FAILURE = "the state can no longer be decoded";

    private volatile boolean poisonFails = false;
    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    private final InMemoryEventStore eventStore = new InMemoryEventStore();
    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final List<Object> dispatched = new CopyOnWriteArrayList<>();
    private final AtomicInteger eventIds = new AtomicInteger();
    private final InMemorySagaStateStore<Order> inMemory = new InMemorySagaStateStore<>();
    private final UndecodableStore store = new UndecodableStore();
    private final SagaEnvelopes<Order> envelopes = SagaEnvelopes.of(inMemory, null);
    private final SagaExecution<OrderEvent, Order, Object> execution =
            new SagaExecution<>("orders", saga(), store, dispatched::add, converter, CONFIG, event -> true, now::get, eventStore);

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

    // Quarantined on "poison", then its state stops decoding
    private void quarantinedWithAnUndecodableState() {
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        CloudEvent poison = append(new ItemAdded(nextEventId(), ORDER, "poison"));
        catchThrowable(() -> execution.onCloudEvent(poison));
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> execution.onCloudEvent(poison));
        poisonFails = false;
        assertThat(instance().status()).as("control: quarantined").isEqualTo(SagaStatus.QUARANTINED);
        store.undecodable = true;
    }

    private Throwable deliverANewInput() {
        return catchThrowable(() -> execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "live"))));
    }

    private boolean thePropagatedFailureIsTheLoadFailure(@Nullable Throwable thrown) {
        for (Throwable t = thrown; t != null; t = t.getCause()) {
            if (DECODE_FAILURE.equals(t.getMessage())) {
                return true;
            }
        }
        return false;
    }

    @Test
    void whose_instance_stopped_being_quarantined_when_the_write_lost_propagates_the_load_failure_and_does_not_skip_the_input() {
        quarantinedWithAnUndecodableState();
        store.beforeTheNextSkip(() -> {
            // Another node releases the instance between the skip's read and its first write
            store.undecodable = false;
            execution.release(ORDER);
            store.undecodable = true;
        });

        Throwable thrown = deliverANewInput();

        assertAll(
                () -> assertThat(instance().status()).as("control: the instance is active again").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(thrown).as("the delivery fails, so the subscription does not move past the input").isNotNull(),
                () -> assertThat(thePropagatedFailureIsTheLoadFailure(thrown)).as(String.valueOf(thrown)).isTrue(),
                () -> assertThat(dispatched).as("the input is not applied").doesNotContain(new Pack(ORDER, "live"))
        );
    }

    @Test
    void whose_instance_is_gone_when_the_write_lost_propagates_the_load_failure_and_does_not_skip_the_input() {
        quarantinedWithAnUndecodableState();
        AtomicBoolean wasGone = new AtomicBoolean();
        store.beforeTheNextSkip(() -> {
            inMemory.delete(ORDER);
            wasGone.set(inMemory.find(ORDER).isEmpty());
        });

        Throwable thrown = deliverANewInput();

        assertAll(
                () -> assertThat(wasGone).as("control: the instance was gone when the write lost").isTrue(),
                () -> assertThat(thrown).as("the delivery fails, so the subscription does not move past the input").isNotNull(),
                () -> assertThat(thePropagatedFailureIsTheLoadFailure(thrown)).as(String.valueOf(thrown)).isTrue()
        );
    }

    @Test
    void whose_write_keeps_losing_propagates_the_load_failure_after_every_attempt_and_does_not_skip_the_input() {
        quarantinedWithAnUndecodableState();
        store.losesEverySkip = true;
        store.skips.set(0);

        Throwable thrown = deliverANewInput();

        assertAll(
                () -> assertThat(store.skips.get()).as("control: the write was tried maxCasAttempts times").isEqualTo(CONFIG.maxCasAttempts()),
                () -> assertThat(thrown).as("the delivery fails, so the subscription does not move past the input").isNotNull(),
                () -> assertThat(thePropagatedFailureIsTheLoadFailure(thrown)).as(String.valueOf(thrown)).isTrue(),
                () -> assertThat(inMemory.findSkippedInputs(ORDER, instance().quarantineId(), Integer.MAX_VALUE)).as("nothing is recorded").isEmpty()
        );
    }

    private final class UndecodableStore implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
        volatile boolean undecodable;
        volatile boolean losesEverySkip;
        final AtomicInteger skips = new AtomicInteger();
        private final AtomicReference<@Nullable Runnable> beforeTheNextSkip = new AtomicReference<>();

        void beforeTheNextSkip(Runnable action) {
            beforeTheNextSkip.set(action);
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
            if (undecodable && inMemory.find(sagaId).isPresent()) {
                throw new IllegalStateException(DECODE_FAILURE);
            }
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
            skips.incrementAndGet();
            Runnable action = beforeTheNextSkip.getAndSet(null);
            if (action != null) {
                action.run();
            }
            if (losesEverySkip) {
                return false;
            }
            return inMemory.compareAndSaveSkipped(saga, expectedVersion, quarantineId, input);
        }

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            return inMemory.findSkippedInputs(sagaId, quarantineId, limit);
        }

        @Override
        public boolean compareAndSaveReleaseStarted(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput head) {
            return inMemory.compareAndSaveReleaseStarted(saga, expectedVersion, quarantineId, head);
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
