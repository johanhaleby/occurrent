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
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.command.CommandDispatcher;
import org.occurrent.dsl.saga.HeldEvent;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.StartMark;
import org.occurrent.dsl.saga.SagaHeldEvents;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.mongodb.spring.SpringMongoSagaStateStore;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Docker-based. A hold and a start that run on two threads at once against MongoDB. The hold inserts a copy and then
 * reads the instance, and the start lists the held events after its own save, so whichever comes second sees the other
 * and applies the copy. The two deliveries are released together, round after round, and the reactions that ran and the
 * copies left held are looked at. A delivery that loses its save to the other retries the way a subscription redelivers
 * it. This is the in-memory {@code SagaHoldStartFenceConcurrencyTest} over the real store, where a read and a write are
 * a round trip and the interleavings are not the in-memory ones. It sits in the package of {@code SagaExecution}, which
 * is not public.
 */
@Testcontainers
@DisplayName("A held copy and the start that applies it, delivered from two threads at once against MongoDB")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(120)
class SpringMongoSagaHoldStartFenceConcurrencyTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String COLLECTION = "saga-hold-start-fence";
    private static final int ROUNDS = 300;
    private static final int MAX_REDELIVERIES = 50;
    private static final long MAX_DELAY_NANOS = 4_000_000;
    private static final String ITEM = "book";

    sealed interface OrderEvent {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record ItemAdded(String eventId, String orderId, String item) implements OrderEvent {
    }

    sealed interface OrderCommand {
    }

    record OpenLedger(String orderId) implements OrderCommand {
    }

    record Reserve(String orderId, String item) implements OrderCommand {
    }

    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(OrderEvent::eventId).build();
    private final ExecutorService threads = Executors.newFixedThreadPool(2);
    private SpringMongoSagaStateStore<String> mongoStore;

    @BeforeEach
    void createInstances() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-fence-" + UUID.randomUUID()));
        mongoStore = new SpringMongoSagaStateStore<>(new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase())), COLLECTION, String.class);
    }

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
     * Delegates to the MongoDB store, and waits for a random moment before a copy is inserted and before an instance is
     * saved, so that the two deliveries meet in every order. The read that follows the insert is not delayed, since a
     * delay there lets the other delivery's save in before the read and hides the interleaving where the read comes
     * first. It records whether the instance was already started when the copy went in.
     */
    private static final class WaitingBeforeTheHold implements SagaStateStore<String>, SagaHeldEvents<String> {
        private final SpringMongoSagaStateStore<String> delegate;
        private final AtomicInteger holds = new AtomicInteger();
        private final AtomicInteger holdsAfterTheStartSaved = new AtomicInteger();
        private final AtomicInteger lostSaves = new AtomicInteger();
        private final boolean startedWhenLowered;

        /** @param startedWhenLowered true when the start is told by its start mark being down at 3, false when told by the instance existing */
        WaitingBeforeTheHold(SpringMongoSagaStateStore<String> delegate, boolean startedWhenLowered) {
            this.delegate = delegate;
            this.startedWhenLowered = startedWhenLowered;
        }

        private static void pauseBriefly() {
            LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(0, MAX_DELAY_NANOS));
        }

        @Override
        public boolean hold(String sagaId, HeldEvent event) {
            pauseBriefly();
            holds.incrementAndGet();
            Optional<SagaEnvelope<String>> instance = delegate.find(sagaId);
            boolean startSaved = startedWhenLowered
                    ? instance.map(found -> found.startMark() != null && found.startMark().version() == 3).orElse(false)
                    : instance.map(SagaEnvelope::started).orElse(false);
            if (startSaved) {
                holdsAfterTheStartSaved.incrementAndGet();
            }
            return delegate.hold(sagaId, event);
        }

        @Override
        public Optional<SagaEnvelope<String>> find(String sagaId) {
            return delegate.find(sagaId);
        }

        @Override
        public Optional<SagaEnvelope<String>> findWithoutState(String sagaId) {
            return delegate.findWithoutState(sagaId);
        }

        @Override
        public boolean compareAndSave(String sagaId, SagaEnvelope<String> envelope, long expectedVersion) {
            pauseBriefly();
            return counted(delegate.compareAndSave(sagaId, envelope, expectedVersion));
        }

        @Override
        public boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<String> envelope, long expectedVersion) {
            return counted(delegate.compareAndSaveWithoutState(sagaId, envelope, expectedVersion));
        }

        private boolean counted(boolean saved) {
            if (!saved) {
                lostSaves.incrementAndGet();
            }
            return saved;
        }

        @Override
        public List<SagaEnvelope<String>> findWithDueTimers(Instant now, int limit) {
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
        public List<HeldEventRef> heldIndexWithoutWaiting(String sagaId, StartMark start) {
            return delegate.heldIndexWithoutWaiting(sagaId, start);
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

    private static Saga<OrderEvent, String, OrderCommand> saga() {
        return Saga.<OrderEvent, String, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> "placed")
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.orderId()))))
                .evolve(ItemAdded.class, (state, e) -> state + "," + e.item())
                .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                .build();
    }

    private CloudEvent placedAt(String orderId, long streamVersion) {
        return streamKeyed(orderId, streamVersion, new OrderPlaced("placed-" + streamVersion + "-" + orderId, orderId));
    }

    private CloudEvent itemAt(String orderId, long streamVersion) {
        return streamKeyed(orderId, streamVersion, new ItemAdded("item-" + streamVersion + "-" + orderId, orderId, ITEM));
    }

    // The stream key an event store writes, with a stream of its own for each round.
    private CloudEvent streamKeyed(String orderId, long streamVersion, OrderEvent event) {
        return CloudEventBuilder.v1(converter.toCloudEvent(event)).withExtension(OccurrentCloudEventExtension.occurrent("stream-" + orderId, streamVersion)).build();
    }

    private SagaExecution<OrderEvent, String, OrderCommand> executionOver(SagaStateStore<String> store, CommandDispatcher<OrderCommand> dispatcher) {
        return new SagaExecution<>("orders", saga(), store, dispatcher, converter, SagaRunnerConfig.defaults(), event -> true, Instant::now, true);
    }

    /** One delivery, redelivered when it loses its save to the other one more often than the executor retries. */
    private static void deliverUntilAcknowledged(SagaExecution<OrderEvent, String, OrderCommand> execution, CloudEvent event) {
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

    private Callable<Void> afterTheBarrier(CyclicBarrier barrier, SagaExecution<OrderEvent, String, OrderCommand> execution, CloudEvent event) {
        return () -> {
            barrier.await(10, TimeUnit.SECONDS);
            deliverUntilAcknowledged(execution, event);
            return null;
        };
    }

    private void deliverTogether(SagaExecution<OrderEvent, String, OrderCommand> execution, CloudEvent first, CloudEvent second) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        Future<Void> one = threads.submit(afterTheBarrier(barrier, execution, first));
        Future<Void> other = threads.submit(afterTheBarrier(barrier, execution, second));
        one.get(30, TimeUnit.SECONDS);
        other.get(30, TimeUnit.SECONDS);
    }

    private static long reactionsTo(List<OrderCommand> issued, String orderId) {
        return issued.stream().filter(command -> command.equals(new Reserve(orderId, ITEM))).count();
    }

    // A save that loses to the other delivery runs the reactions of its input again, so a reaction is dispatched once
    // for the save that wins and once more for each save that was lost. What the instance applied is what it saved.
    private static void assertTheItemWasAppliedOnce(int round, String orderId, WaitingBeforeTheHold store, List<OrderCommand> issued) {
        assertThat(store.find(orderId).orElseThrow().state()).as("round %d: the state the instance saved", round).isEqualTo("placed," + ITEM);
        assertThat(reactionsTo(issued, orderId)).as("round %d: the item reaction, one dispatch for the winning save and one for each lost one", round)
                .isBetween(1L, 1L + store.lostSaves.get());
        assertThat(store.heldIndex(orderId)).as("round %d: the held index", round).isEmpty();
    }

    private static void record(Orders orders, WaitingBeforeTheHold store) {
        if (store.holds.get() == 0) {
            orders.startFirst().incrementAndGet();
        } else if (store.holdsAfterTheStartSaved.get() > 0) {
            orders.holdAfterTheStartSaved().incrementAndGet();
        } else {
            orders.holdBeforeTheStartSaved().incrementAndGet();
        }
    }

    @Test
    void an_item_below_the_start_and_a_start_below_it_delivered_together_run_the_item_once_and_leave_no_copy_held(TestReporter reporter) throws Exception {
        Orders orders = new Orders();
        for (int round = 0; round < ROUNDS; round++) {
            // Given
            String orderId = "order-one-" + round;
            WaitingBeforeTheHold store = new WaitingBeforeTheHold(mongoStore, true);
            List<OrderCommand> issued = new CopyOnWriteArrayList<>();
            SagaExecution<OrderEvent, String, OrderCommand> execution = executionOver(store, issued::add);
            execution.onCloudEvent(placedAt(orderId, 5));

            // When
            deliverTogether(execution, itemAt(orderId, 4), placedAt(orderId, 3));

            // Then
            int roundNumber = round;
            assertTheItemWasAppliedOnce(roundNumber, orderId, store, issued);
            assertThat(store.find(orderId).orElseThrow().startMark()).as("round %d: the start mark", roundNumber).isNotNull().satisfies(mark -> assertThat(mark.version()).isEqualTo(3));
            record(orders, store);
        }
        reporter.publishEntry("rounds", String.valueOf(ROUNDS));
        reporter.publishEntry("scenario one", orders.toString());
        System.out.println("mongo fence scenario one, rounds " + ROUNDS + ": " + orders);
    }

    @Test
    void an_item_and_the_start_of_its_instance_delivered_together_run_the_item_once_after_the_start_and_leave_no_copy_held(TestReporter reporter) throws Exception {
        Orders orders = new Orders();
        for (int round = 0; round < ROUNDS; round++) {
            // Given
            String orderId = "order-two-" + round;
            WaitingBeforeTheHold store = new WaitingBeforeTheHold(mongoStore, false);
            List<OrderCommand> issued = new CopyOnWriteArrayList<>();
            SagaExecution<OrderEvent, String, OrderCommand> execution = executionOver(store, issued::add);

            // When
            deliverTogether(execution, itemAt(orderId, 2), placedAt(orderId, 1));

            // Then
            int roundNumber = round;
            assertTheItemWasAppliedOnce(roundNumber, orderId, store, issued);
            assertThat(issued.indexOf(new Reserve(orderId, ITEM))).as("round %d: the item reaction runs after the start", roundNumber).isGreaterThan(issued.indexOf(new OpenLedger(orderId)));
            record(orders, store);
        }
        reporter.publishEntry("rounds", String.valueOf(ROUNDS));
        reporter.publishEntry("scenario two", orders.toString());
        System.out.println("mongo fence scenario two, rounds " + ROUNDS + ": " + orders);
    }
}
