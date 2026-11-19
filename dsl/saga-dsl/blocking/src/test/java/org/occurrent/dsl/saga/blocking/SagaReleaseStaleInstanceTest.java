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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A release reads the instance, then the records. Another release can apply the first record in between, in a write
 * that changes the version and deletes the record, so the records read no longer holds the record the instance read was
 * taken before. The release must read the instance again before it replays what the records read gave it, or it computes
 * and dispatches the next record's commands from a state that lacks the first record's effect.
 */
@DisplayName("A release that another release overtakes between its instance read and its records read")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaReleaseStaleInstanceTest {

    private static final String ORDER = "o1";
    private static final Duration BUDGET = Duration.ofMinutes(1);
    private static final SagaRunnerConfig CONFIG = SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET)
            .withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT);
    private static final String STALE = "-from-a-state-without-poison";

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

    // The command of an input other than "poison" names the state it was computed from, so a command from a state that
    // lacks the effect of "poison" is told apart from one that does not
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
                    boolean computedFromAStateWithoutPoison = !e.item().equals("poison") && !state.items().contains("poison");
                    return List.of(SagaEffect.issue(new Pack(e.orderId(), computedFromAStateWithoutPoison ? e.item() + STALE : e.item())));
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

    @Test
    void does_not_dispatch_the_commands_of_the_next_record_from_the_state_it_read_before_the_first_record_was_applied() {
        OvertakenStore overtaken = new OvertakenStore();
        SagaExecution<OrderEvent, Order, Object> execution = execution(overtaken);
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        CloudEvent poison = append(new ItemAdded(nextEventId(), ORDER, "poison"));
        catchThrowable(() -> execution.onCloudEvent(poison));
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> execution.onCloudEvent(poison));
        poisonFails = false;
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "a")));
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "b")));
        assertThat(envelopes.find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED);
        SagaExecution<OrderEvent, Order, Object> otherRelease = execution(new StopsAfterTheFirstReplay());
        overtaken.otherRelease = () -> catchThrowable(() -> otherRelease.release(ORDER));

        ReleaseResult result = execution.release(ORDER);

        SagaEnvelope<Order> after = envelopes.find(ORDER).orElseThrow();
        String evidence = "released " + result + ", the other release ran " + overtaken.otherReleaseRan + ", items " + after.state().items() + ", dispatched " + dispatched;
        assertAll(
                () -> assertThat(overtaken.otherReleaseRan).as("the other release applied the first record between the two reads. " + evidence).isTrue(),
                () -> assertThat(dispatched).as("no command is dispatched from the state that lacks the first record's effect. " + evidence)
                        .noneMatch(command -> command instanceof Pack pack && pack.item().endsWith(STALE)),
                () -> assertThat(result).as(evidence).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(after.state().items()).as(evidence).containsExactly("poison", "a", "b")
        );
    }

    private class Delegating implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
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
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            return inMemory.findSkippedInputs(sagaId, quarantineId, limit);
        }

        @Override
        public void delete(String sagaId) {
            inMemory.delete(sagaId);
        }
    }

    // The first read of the records is the one that comes after the release read the instance, since starting the release
    // reads no records. Another release runs to its first replay write just before it, so the records it gives back have
    // lost their first record while the instance that was read has not
    private final class OvertakenStore extends Delegating {
        volatile Runnable otherRelease = () -> {
        };
        volatile boolean otherReleaseRan = false;
        private final AtomicBoolean overtaken = new AtomicBoolean();

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            if (overtaken.compareAndSet(false, true)) {
                otherRelease.run();
                otherReleaseRan = true;
            }
            return super.findSkippedInputs(sagaId, quarantineId, limit);
        }
    }

    // Stops the release it serves the way a crash does, before the second replay write, so the first record is applied and the next is not
    private final class StopsAfterTheFirstReplay extends Delegating {
        private final AtomicInteger replays = new AtomicInteger();

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            if (replays.incrementAndGet() == 2) {
                throw new OutOfMemoryError("the other release stops here");
            }
            return super.compareAndSaveReplayed(saga, expectedVersion, quarantineId, replayed);
        }
    }
}
