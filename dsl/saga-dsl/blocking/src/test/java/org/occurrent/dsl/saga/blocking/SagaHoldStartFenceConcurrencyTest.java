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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.TestReporter;
import org.occurrent.dsl.saga.HeldEvent;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaHeldEvents;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.Order;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OrderCommand;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OrderEvent;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.RecordingDispatcher;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.ORDER;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.STREAM;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.itemAt;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.placedAt;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.reserve;

/**
 * A hold and a start that run on two threads at once. The hold inserts a copy and then reads the instance, and the
 * start lists the held events after its own save, so whichever comes second sees the other and applies the copy. These
 * tests release the two deliveries together, round after round, and look at the reactions that ran and the copies left
 * held. A delivery that loses its save to the other retries the way a subscription redelivers it.
 */
@DisplayName("A held copy and the start that applies it, delivered from two threads at once")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SagaHoldStartFenceConcurrencyTest {

    private static final int ROUNDS = 3_000;
    private static final int MAX_REDELIVERIES = 50;
    private static final String ITEM = "book";

    private final ExecutorService threads = Executors.newFixedThreadPool(2);

    @AfterEach
    void stopThreads() {
        threads.shutdownNow();
    }

    /** What a round saw of the order in which the hold and the start met. */
    private record Orders(AtomicInteger startFirst, AtomicInteger holdBeforeTheStartSaved, AtomicInteger holdAfterTheStartSaved) {
        Orders() {
            this(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        }

        @Override
        public String toString() {
            return "item applied without a hold (start first) " + startFirst + ", hold inserted before the start saved " + holdBeforeTheStartSaved
                   + ", hold inserted after the start saved " + holdAfterTheStartSaved;
        }
    }

    /**
     * Delegates to the in-memory store, and spins for a random moment before a copy is inserted, so that the other
     * delivery has a chance to save and list in between. It records whether the instance was already started when the
     * copy went in.
     */
    private static final class SpinningBeforeTheHold<S> implements SagaStateStore<S>, SagaHeldEvents<S> {
        private final InMemorySagaStateStore<S> delegate = new InMemorySagaStateStore<>();
        private final AtomicInteger holds = new AtomicInteger();
        private final AtomicInteger holdsAfterTheStartSaved = new AtomicInteger();
        private final AtomicInteger lostSaves = new AtomicInteger();
        private final boolean startedWhenLowered;

        /** @param startedWhenLowered true when the start is told by its start mark being down at 3, false when told by the instance existing */
        SpinningBeforeTheHold(boolean startedWhenLowered) {
            this.startedWhenLowered = startedWhenLowered;
        }

        private static void spin() {
            int spins = ThreadLocalRandom.current().nextInt(0, 400);
            for (int i = 0; i < spins; i++) {
                Thread.onSpinWait();
            }
        }

        @Override
        public boolean hold(String sagaId, HeldEvent event) {
            spin();
            holds.incrementAndGet();
            Optional<SagaEnvelope<S>> instance = delegate.find(sagaId);
            boolean startSaved = startedWhenLowered
                    ? instance.map(found -> found.startMark() != null && found.startMark().version() == 3).orElse(false)
                    : instance.map(SagaEnvelope::started).orElse(false);
            if (startSaved) {
                holdsAfterTheStartSaved.incrementAndGet();
            }
            return delegate.hold(sagaId, event);
        }

        @Override
        public Optional<SagaEnvelope<S>> find(String sagaId) {
            spin();
            return delegate.find(sagaId);
        }

        @Override
        public Optional<SagaEnvelope<S>> findWithoutState(String sagaId) {
            return delegate.findWithoutState(sagaId);
        }

        @Override
        public boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
            return counted(delegate.compareAndSave(sagaId, envelope, expectedVersion));
        }

        @Override
        public boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
            return counted(delegate.compareAndSaveWithoutState(sagaId, envelope, expectedVersion));
        }

        private boolean counted(boolean saved) {
            if (!saved) {
                lostSaves.incrementAndGet();
            }
            return saved;
        }

        @Override
        public List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit) {
            return delegate.findWithDueTimers(now, limit);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }

        @Override
        public void failing(String sagaId, String key, Instant at) {
            delegate.failing(sagaId, key, at);
        }

        @Override
        public List<HeldEventRef> heldIndex(String sagaId) {
            return delegate.heldIndex(sagaId);
        }

        @Override
        public List<HeldEvent> held(String sagaId, Collection<String> keys) {
            return delegate.held(sagaId, keys);
        }

        @Override
        public void release(String sagaId, String key) {
            delegate.release(sagaId, key);
        }
    }

    /** One delivery, redelivered when it loses its save to the other one more often than the executor retries. */
    private static void deliverUntilAcknowledged(SagaExecution<OrderEvent, Order, OrderCommand> execution, CloudEvent event) {
        for (int attempt = 1; ; attempt++) {
            try {
                execution.onCloudEvent(event);
                return;
            } catch (SagaConcurrencyException e) {
                if (attempt == MAX_REDELIVERIES) {
                    throw e;
                }
            }
        }
    }

    private Callable<Void> afterTheBarrier(CyclicBarrier barrier, SagaExecution<OrderEvent, Order, OrderCommand> execution, CloudEvent event) {
        return () -> {
            barrier.await(10, TimeUnit.SECONDS);
            deliverUntilAcknowledged(execution, event);
            return null;
        };
    }

    private void deliverTogether(SagaExecution<OrderEvent, Order, OrderCommand> execution, CloudEvent first, CloudEvent second) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        Future<Void> one = threads.submit(afterTheBarrier(barrier, execution, first));
        Future<Void> other = threads.submit(afterTheBarrier(barrier, execution, second));
        one.get(30, TimeUnit.SECONDS);
        other.get(30, TimeUnit.SECONDS);
    }

    private static long reactionsTo(RecordingDispatcher dispatcher, String item) {
        return dispatcher.issued().stream().filter(command -> command.equals(reserve(item))).count();
    }

    // A save that loses to the other delivery runs the reactions of its input again, so a reaction is dispatched once
    // for the save that wins and once more for each save that was lost. What the instance applied is what it saved.
    private static void assertTheItemWasAppliedOnce(int round, SpinningBeforeTheHold<Order> store, RecordingDispatcher dispatcher) {
        assertThat(store.find(ORDER).orElseThrow().state().items()).as("round %d: the items the instance applied", round).containsExactly(ITEM);
        assertThat(reactionsTo(dispatcher, ITEM)).as("round %d: the item reaction, one dispatch for the winning save and one for each lost one", round)
                .isBetween(1L, 1L + store.lostSaves.get());
        assertThat(store.heldIndex(ORDER)).as("round %d: the held index", round).isEmpty();
    }

    private static SagaExecution<OrderEvent, Order, OrderCommand> executionOver(SagaStateStore<Order> store, RecordingDispatcher dispatcher) {
        return new SagaExecution<>("orders", HeldOrderSaga.saga(), store, dispatcher, HeldOrderSaga.converter(), SagaRunnerConfig.defaults(), event -> true, Instant::now, true);
    }

    @Test
    void an_item_below_the_start_and_a_start_below_it_delivered_together_run_the_item_once_and_leave_no_copy_held(TestReporter reporter) throws Exception {
        Orders orders = new Orders();
        for (int round = 0; round < ROUNDS; round++) {
            // Given
            SpinningBeforeTheHold<Order> store = new SpinningBeforeTheHold<>(true);
            RecordingDispatcher dispatcher = new RecordingDispatcher();
            SagaExecution<OrderEvent, Order, OrderCommand> execution = executionOver(store, dispatcher);
            execution.onCloudEvent(placedAt(5));

            // When
            deliverTogether(execution, itemAt(4, ITEM), placedAt(3));

            // Then
            int roundNumber = round;
            assertTheItemWasAppliedOnce(roundNumber, store, dispatcher);
            assertThat(store.find(ORDER).orElseThrow().startMark()).as("round %d: the start mark", roundNumber).isNotNull().satisfies(mark -> assertThat(mark.version()).isEqualTo(3));
            record(orders, store);
        }
        reporter.publishEntry("rounds", String.valueOf(ROUNDS));
        reporter.publishEntry("scenario one", orders.toString());
        System.out.println("fence scenario one, rounds " + ROUNDS + ": " + orders);
    }

    @Test
    void an_item_and_the_start_of_its_instance_delivered_together_run_the_item_once_after_the_start_and_leave_no_copy_held(TestReporter reporter) throws Exception {
        Orders orders = new Orders();
        for (int round = 0; round < ROUNDS; round++) {
            // Given
            SpinningBeforeTheHold<Order> store = new SpinningBeforeTheHold<>(false);
            RecordingDispatcher dispatcher = new RecordingDispatcher();
            SagaExecution<OrderEvent, Order, OrderCommand> execution = executionOver(store, dispatcher);

            // When
            deliverTogether(execution, itemAt(2, ITEM), placedAt(1));

            // Then
            int roundNumber = round;
            List<OrderCommand> issued = dispatcher.issued();
            assertTheItemWasAppliedOnce(roundNumber, store, dispatcher);
            assertThat(issued.indexOf(reserve(ITEM))).as("round %d: the item reaction runs after the start", roundNumber).isGreaterThan(issued.indexOf(new HeldOrderSaga.OpenLedger(ORDER)));
            record(orders, store);
        }
        reporter.publishEntry("rounds", String.valueOf(ROUNDS));
        reporter.publishEntry("scenario two", orders.toString());
        System.out.println("fence scenario two, rounds " + ROUNDS + ": " + orders);
    }

    private static void record(Orders orders, SpinningBeforeTheHold<?> store) {
        if (store.holds.get() == 0) {
            orders.startFirst().incrementAndGet();
        } else if (store.holdsAfterTheStartSaved.get() > 0) {
            orders.holdAfterTheStartSaved().incrementAndGet();
        } else {
            orders.holdBeforeTheStartSaved().incrementAndGet();
        }
    }
}
