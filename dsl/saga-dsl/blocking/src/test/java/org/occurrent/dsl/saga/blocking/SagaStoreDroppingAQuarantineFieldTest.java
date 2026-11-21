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
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaInstance;
import org.occurrent.dsl.saga.SagaInstances;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreQueries;
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.codec.EncodedSaga;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.api.blocking.Subscription;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * What a saga does when its state store does not persist one field of the quarantine record, so the field is gone from
 * every {@link StoredSaga} read back. Each test drives an instance to quarantine on a store that drops one field and
 * then calls {@link SagaSubscription#release(String)}. They pin what happens today, so the documentation of the state
 * store contract can say it.
 */
@DisplayName("A state store that does not persist a field of the quarantine record")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaStoreDroppingAQuarantineFieldTest {

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
    }

    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    private final InMemoryEventStore eventStore = new InMemoryEventStore();
    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final List<Object> dispatched = new CopyOnWriteArrayList<>();
    private final java.util.concurrent.atomic.AtomicInteger eventIds = new AtomicInteger();
    private final FieldDroppingStore store = new FieldDroppingStore(new InMemorySagaStateStore<>());
    private final SagaExecution<OrderEvent, Order, Object> execution =
            new SagaExecution<>("orders", saga(), store, dispatched::add, converter, CONFIG, now::get);
    private final SagaSubscription subscription = new SagaSubscription(new NoSubscription(), Executors.newSingleThreadExecutor(), SagaInstances.of(store), null, null, null, execution::release);

    private ListAppender<ILoggingEvent> appender;
    private Logger executionLog;
    private @Nullable Level originalLevel;

    @BeforeEach
    void startCapturing() {
        appender = new ListAppender<>();
        appender.start();
        executionLog = (Logger) LoggerFactory.getLogger(SagaExecution.class);
        executionLog.addAppender(appender);
        originalLevel = executionLog.getLevel();
        executionLog.setLevel(Level.INFO);
    }

    @AfterEach
    void stopCapturing() {
        executionLog.setLevel(originalLevel);
        executionLog.detachAppender(appender);
        appender.stop();
        subscription.close();
    }

    private Saga<OrderEvent, Order, Object> saga() {
        return Saga.<OrderEvent, Order, Object>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(e.orderId(), List.of()))
                .evolve(ItemAdded.class, (state, e) -> new Order(e.orderId(), List.of(e.item())))
                .react(ItemAdded.class, (state, e) -> {
                    if (e.item().equals("poison")) {
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

    private CloudEvent item(String item) {
        return append(new ItemAdded("e" + eventIds.incrementAndGet(), ORDER, item));
    }

    private List<String> messages(Level level) {
        return appender.list.stream().filter(e -> e.getLevel() == level).map(ILoggingEvent::getFormattedMessage).toList();
    }

    /** What the delivery that ran the instance out of its budget did, null when it returned, which acknowledges the event. */
    private @Nullable Throwable quarantineOnPoison(String droppedField) {
        store.drop(droppedField);
        execution.onCloudEvent(append(new OrderPlaced("e" + eventIds.incrementAndGet(), ORDER)));
        CloudEvent poison = item("poison");
        assertThat(catchThrowable(() -> execution.onCloudEvent(poison))).as("control: the first failure is not acknowledged").isInstanceOf(IllegalStateException.class);
        now.set(now.get().plus(BUDGET));
        return catchThrowable(() -> execution.onCloudEvent(poison));
    }

    private static String refusalOf(String field) {
        return "Saga instance 'o1' is stored as QUARANTINED_RECORDING without " + field + ". The store has to persist every field of the StoredSaga it is given.";
    }

    private void assertEveryReadRefusesWith(String field) {
        Throwable later = catchThrowable(() -> execution.onCloudEvent(item("later")));
        Throwable page = catchThrowable(() -> subscription.instances().findByStatus(SagaStatus.QUARANTINED, now.get().plusSeconds(5), 10));
        Throwable release = catchThrowable(() -> subscription.release(ORDER));
        assertAll(
                () -> assertThat(later).as("a later delivery is not acknowledged, so the feed offers it again")
                        .isInstanceOf(IllegalStateException.class).hasMessage(refusalOf(field)),
                () -> assertThat(page).as("findByStatus for the page that holds the instance")
                        .isInstanceOf(IllegalStateException.class).hasMessage(refusalOf(field)),
                () -> assertThat(release).as("release")
                        .isInstanceOf(IllegalStateException.class).hasMessage(refusalOf(field)),
                () -> assertThat(dispatched).as("nothing is applied or replayed").isEmpty()
        );
    }

    @Test
    void that_drops_failureReleasedAt_acknowledges_everything_and_ends_a_release_QUARANTINED_AGAIN_without_replaying_anything() {
        Throwable quarantining = quarantineOnPoison(SagaCodec.FAILURE_RELEASED_AT);

        Throwable later = catchThrowable(() -> execution.onCloudEvent(item("later")));
        List<SagaInstance> page = subscription.instances().findByStatus(SagaStatus.QUARANTINED, now.get().plusSeconds(5), 10);
        String quarantineId = Objects.requireNonNull(page.getFirst().failure()).quarantineId();
        SagaSubscription.ReleaseResult released = subscription.release(ORDER);

        assertAll(
                () -> assertThat(quarantining).as("the quarantining delivery is acknowledged").isNull(),
                () -> assertThat(later).as("a later delivery is acknowledged, as a skip").isNull(),
                () -> assertThat(page).extracting(SagaInstance::sagaId).containsExactly(ORDER),
                () -> assertThat(page.getFirst().status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(released).isEqualTo(SagaSubscription.ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(dispatched).as("nothing is replayed").isEmpty(),
                () -> assertThat(store.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE)).as("the recorded inputs are still there").isNotEmpty(),
                () -> assertThat(subscription.instances().find(ORDER).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(messages(Level.INFO)).as("the one INFO line that names the cause")
                        .filteredOn(m -> m.contains("does not persist SagaFailure.releasedAt")).hasSize(1)
        );
    }

    @Test
    void that_drops_failureQuarantineId_acknowledges_the_quarantine_and_refuses_every_later_read() {
        Throwable quarantining = quarantineOnPoison(SagaCodec.FAILURE_QUARANTINE_ID);

        assertThat(quarantining).as("the quarantining delivery is acknowledged").isNull();
        assertEveryReadRefusesWith(SagaCodec.FAILURE_QUARANTINE_ID);
    }

    @Test
    void that_drops_quarantineSkipCount_acknowledges_the_quarantine_and_refuses_every_later_read() {
        Throwable quarantining = quarantineOnPoison(SagaCodec.QUARANTINE_SKIP_COUNT);

        assertThat(quarantining).as("the quarantining delivery is acknowledged").isNull();
        assertEveryReadRefusesWith(SagaCodec.QUARANTINE_SKIP_COUNT);
    }

    @Test
    void that_drops_failureInput_acknowledges_the_quarantine_and_refuses_every_later_read() {
        Throwable quarantining = quarantineOnPoison(SagaCodec.FAILURE_INPUT);

        assertThat(quarantining).as("the quarantining delivery is acknowledged").isNull();
        assertEveryReadRefusesWith(SagaCodec.FAILURE_INPUT);
    }

    private record NoSubscription() implements Subscription {
        @Override
        public String id() {
            return "orders";
        }

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    /** Persists everything it is given except the one field it was told to drop, from every instance it saves. */
    private static final class FieldDroppingStore implements SagaStateStore<Order>, SagaStateStoreQueries<Order>, SagaStateStoreSkippedInputs {
        final InMemorySagaStateStore<Order> delegate;
        private volatile @Nullable String dropped;

        FieldDroppingStore(InMemorySagaStateStore<Order> delegate) {
            this.delegate = delegate;
        }

        void drop(String field) {
            dropped = field;
        }

        private StoredSaga withoutTheField(StoredSaga saga) {
            Map<String, Object> fields = new LinkedHashMap<>(saga.encoded().fields());
            fields.remove(dropped);
            return new StoredSaga(saga.sagaId(), saga.version(), new EncodedSaga(fields), saga.index());
        }

        @Override
        public boolean fits(StoredSaga candidate) {
            return delegate.fits(candidate);
        }

        @Override
        public void requireAtomicTransitions() {
            delegate.requireAtomicTransitions();
        }

        @Override
        public SagaStateCodec<Order> stateCodec() {
            return delegate.stateCodec();
        }

        @Override
        public Optional<StoredSaga> find(String sagaId) {
            return delegate.find(sagaId);
        }

        @Override
        public Optional<StoredSaga> findWithoutState(String sagaId) {
            return delegate.findWithoutState(sagaId);
        }

        @Override
        public boolean compareAndSave(StoredSaga saga, long expectedVersion) {
            return delegate.compareAndSave(withoutTheField(saga), expectedVersion);
        }

        @Override
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            return delegate.compareAndSaveWithoutState(withoutTheField(saga), expectedVersion);
        }

        @Override
        public List<StoredSaga> findWithDueTimers(Instant now, int limit) {
            return delegate.findWithDueTimers(now, limit);
        }

        @Override
        public List<StoredSaga> findByStatus(SagaStatus status, Instant updatedBefore, int limit) {
            return delegate.findByStatus(status, updatedBefore, limit);
        }

        @Override
        public List<String> findReleasingSagaIds(@Nullable String after, int limit) {
            return delegate.findReleasingSagaIds(after, limit);
        }

        @Override
        public boolean compareAndSaveSkipped(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput input) {
            return delegate.compareAndSaveSkipped(withoutTheField(saga), expectedVersion, quarantineId, input);
        }

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            return delegate.compareAndSaveReplayed(withoutTheField(saga), expectedVersion, quarantineId, replayed);
        }

        @Override
        public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
            return delegate.compareAndSaveQuarantineEnded(withoutTheField(saga), expectedVersion, quarantineId);
        }

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            return delegate.findSkippedInputs(sagaId, quarantineId, limit);
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInput> findWithFirstSkippedInput(String sagaId) {
            return delegate.findWithFirstSkippedInput(sagaId);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }
    }
}
