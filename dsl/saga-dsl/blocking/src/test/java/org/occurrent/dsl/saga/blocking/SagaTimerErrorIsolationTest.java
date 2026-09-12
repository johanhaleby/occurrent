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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The per-timer catch was {@code RuntimeException} while the catch around the whole poll was {@code Throwable}, so an
 * {@code Error} out of one instance's reaction unwound the loop over the rest of the batch. Every instance after the
 * failing one lost its turn, on every poll, for as long as the failing one kept failing. Instances the poll had
 * already reached had fired, which is why both tests below put the failing instance first.
 * <p>
 * A {@code StackOverflowError} from a recursive {@code evolve} and a {@code NoClassDefFoundError} from a reaction
 * touching a class that will not load are both reachable and both repeat per instance, so this needed one bad instance
 * rather than a batch's worth.
 * <p>
 * {@code OutOfMemoryError} is the exception and still abandons the poll, because it says nothing about whichever
 * instance held the thread when the heap ran out. That is {@code SagaExecutionSupport.isAttributableToTheInstance},
 * the same rule the event path applies.
 * <p>
 * This does not make a failing instance isolated from the saga's other instances. Nothing requires a store to give a
 * different instance a turn, so enough failing instances can stop the saga firing timers altogether. See
 * <a href="https://github.com/johanhaleby/occurrent/issues/1003">#1003</a>.
 */
@DisplayName("A timer reaction that throws an Error")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaTimerErrorIsolationTest {

    // Epoch millis far enough in the past that every seeded timer is due whatever the wall clock says.
    private static final long LONG_OVERDUE = 1_000;

    private static final String TIMER = "expiry";

    private final CloudEventConverter<OrderEvent> converter =
            new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:occurrent:saga-timer-error-isolation-test")).build();

    @Test
    void costs_only_its_own_instance_and_the_rest_of_the_batch_still_fires() {
        SagaStateStore<String> backing = SagaStateStore.inMemory();
        backing.compareAndSave("throws-an-error", envelope("throws-an-error"), 0);
        seedHealthy(backing, 3);
        SagaStateStore<String> store = failingInstanceFirst(backing, "throws-an-error");
        List<CancelOrder> dispatched = new ArrayList<>();

        execution(store, dispatched::add, sagaId -> {
            if (sagaId.equals("throws-an-error")) {
                throw new NoClassDefFoundError("com/example/AClassThatWillNotLoad");
            }
        }).pollTimers();

        assertThat(dispatched).as("the three healthy instances fire anyway")
                .containsExactlyInAnyOrderElementsOf(healthyCommands(3));
    }

    @Test
    void still_abandons_the_batch_when_the_JVM_ran_out_of_memory_rather_than_the_instance_failing() {
        // The throw happens inside the reaction, so it travels out through the compare-and-set retry loop on the way.
        // That loop catches Throwable, and nothing dispatching here is what says it neither retried the error nor
        // mapped it into something the guard would have read as the instance's.
        SagaStateStore<String> backing = SagaStateStore.inMemory();
        backing.compareAndSave("ran-out-of-memory", envelope("ran-out-of-memory"), 0);
        seedHealthy(backing, 3);
        SagaStateStore<String> store = failingInstanceFirst(backing, "ran-out-of-memory");
        List<CancelOrder> dispatched = new ArrayList<>();

        execution(store, dispatched::add, sagaId -> {
            if (sagaId.equals("ran-out-of-memory")) {
                throw new OutOfMemoryError("Java heap space");
            }
        }).pollTimers();

        assertThat(dispatched).as("the poll is abandoned rather than charged to whichever instance held the thread")
                .isEmpty();
    }

    private static void seedHealthy(SagaStateStore<String> store, int count) {
        IntStream.range(0, count).forEach(i -> store.compareAndSave("healthy-" + i, envelope("healthy-" + i), 0));
    }

    /**
     * The in-memory store iterates a {@code ConcurrentHashMap}, which neither sorts nor keeps insertion order, and
     * {@link SagaStateStore#findWithDueTimers} is free to answer in any order at all. Both tests here need the failing
     * instance to be reached before the healthy ones, because that is the only arrangement under which the old
     * {@code RuntimeException} catch abandons the rest of the batch and this one does not.
     * <p>
     * So the order is fixed here rather than assumed of the store. Without this, the {@code Error} test passes against
     * the old catch whenever the healthy instances happen to come first, and the {@code OutOfMemoryError} test fails
     * against correct code whenever the failing one comes last.
     */
    private static SagaStateStore<String> failingInstanceFirst(SagaStateStore<String> delegate, String failingSagaId) {
        return new SagaStateStore<>() {
            @Override
            public Optional<SagaEnvelope<String>> find(String sagaId) {
                return delegate.find(sagaId);
            }

            @Override
            public boolean compareAndSave(String sagaId, SagaEnvelope<String> envelope, long expectedVersion) {
                return delegate.compareAndSave(sagaId, envelope, expectedVersion);
            }

            @Override
            public List<SagaEnvelope<String>> findWithDueTimers(Instant now, int limit) {
                return delegate.findWithDueTimers(now, limit).stream()
                        .sorted(Comparator.comparing(envelope -> envelope.sagaId().equals(failingSagaId) ? 0 : 1))
                        .toList();
            }

            @Override
            public void delete(String sagaId) {
                delegate.delete(sagaId);
            }
        };
    }

    private static List<CancelOrder> healthyCommands(int count) {
        return IntStream.range(0, count).mapToObj(i -> new CancelOrder("healthy-" + i)).toList();
    }

    private static SagaEnvelope<String> envelope(String sagaId) {
        return new SagaEnvelope<>(sagaId, "waiting", SagaStatus.ACTIVE, 1, List.of(new TimerEntry(TIMER, LONG_OVERDUE)),
                Map.of(), null, Instant.ofEpochMilli(1), Instant.ofEpochMilli(1), null, null);
    }

    private SagaExecution<OrderEvent, String, CancelOrder> execution(SagaStateStore<String> store,
                                                                     CommandDispatcher<CancelOrder> dispatcher,
                                                                     ReactionFailure reactionFailure) {
        Saga<OrderEvent, String, CancelOrder> saga = Saga.<OrderEvent, String, CancelOrder>builder("waiting")
                .correlate(OrderPlaced.class, OrderPlaced::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, event) -> "waiting")
                .evolveOnTimeout(TIMER, (state, timeout) -> "cancelled")
                .reactOnTimeout(TIMER, (state, timeout) -> {
                    reactionFailure.failFor(timeout.sagaId());
                    return List.of(SagaEffect.issue(new CancelOrder(timeout.sagaId())));
                })
                .build();
        SagaRunnerConfig config = new SagaRunnerConfig(Duration.ofSeconds(15), 10, 50);
        return new SagaExecution<>("saga-timer-error-isolation", saga, store, dispatcher, converter, config, event -> true);
    }

    @FunctionalInterface
    private interface ReactionFailure {
        void failFor(String sagaId);
    }

    sealed interface OrderEvent permits OrderPlaced {
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record CancelOrder(String orderId) {
    }
}
