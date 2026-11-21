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

import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInput;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * What the runner says when it switches quarantine off, which is the only place an operator learns that this saga has
 * no instance isolation. The message has to be actionable, so what it must not say matters as much as what it says. An
 * earlier version told every feed to move to a catch-up model over MongoDB, which named the configuration a catching-up
 * push saga was already running as its own remedy.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaQuarantineWarningTest {

    sealed interface OrderEvent permits OrderPlaced {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record ShipOrder(String orderId) {
    }

    private ListAppender<ILoggingEvent> appender;
    private Logger runnerLog;
    private @Nullable Level originalLevel;
    private CloudEventConverter<OrderEvent> converter;

    @BeforeEach
    void startCapturing() {
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).build();
        appender = new ListAppender<>();
        appender.start();
        runnerLog = (Logger) LoggerFactory.getLogger(SagaRunner.class);
        runnerLog.addAppender(appender);
        // Restored in teardown. The level is process-global, so leaving it set would silence lower-level logging in
        // whichever test happens to run next.
        originalLevel = runnerLog.getLevel();
        runnerLog.setLevel(Level.WARN);
    }

    @AfterEach
    void stopCapturing() {
        runnerLog.setLevel(originalLevel);
        runnerLog.detachAppender(appender);
        appender.stop();
    }


    @Test
    void starts_with_quarantine_on_and_warns_about_nothing() {
        RecordingModel model = new RecordingModel();

        SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .run("orders", saga(), SagaStateStore.inMemory(), c -> {
                }, null, SagaRunnerConfig.defaults())
                .close();

        assertAll(
                () -> assertThat(model.subscribed).isEqualTo(1),
                () -> assertThat(warnings()).isEmpty()
        );
    }

    @Test
    void starts_with_quarantine_switched_off_and_warns_about_nothing() {
        RecordingModel model = new RecordingModel();

        SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .run("orders", saga(), SagaStateStore.inMemory(), c -> {
                }, null, SagaRunnerConfig.defaults().disableQuarantine())
                .close();

        assertAll(
                () -> assertThat(model.subscribed).isEqualTo(1),
                () -> assertThat(warnings()).isEmpty()
        );
    }

    @Test
    void refuses_to_start_when_quarantine_is_on_and_the_store_cannot_record_skipped_inputs() {
        RecordingModel model = new RecordingModel();

        Throwable thrown = catchThrowable(() -> SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .run("orders", saga(), new StoreWithoutSkippedInputs<>(), c -> {
                }, null, SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMinutes(5))));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class),
                () -> assertThat(thrown).hasMessageContaining("SagaStateStoreSkippedInputs"),
                () -> assertThat(thrown).hasMessageContaining("SagaRunnerConfig.disableQuarantine()"),
                () -> assertThat(model.subscribed).isZero()
        );
    }

    @Test
    void starts_with_a_store_that_cannot_record_skipped_inputs_when_quarantine_is_switched_off() {
        RecordingModel model = new RecordingModel();

        SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .run("orders", saga(), new StoreWithoutSkippedInputs<>(), c -> {
                }, null, SagaRunnerConfig.defaults().disableQuarantine())
                .close();

        assertAll(
                () -> assertThat(model.subscribed).isEqualTo(1),
                () -> assertThat(warnings()).isEmpty()
        );
    }

    @Test
    void refuses_to_start_when_quarantine_is_on_and_the_store_cannot_write_atomically() {
        RecordingModel model = new RecordingModel();

        Throwable thrown = catchThrowable(() -> SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .run("orders", saga(), new StoreWithoutAtomicTransitions<>(), c -> {
                }, null, SagaRunnerConfig.defaults()));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("no atomic transitions here"),
                () -> assertThat(model.subscribed).isZero()
        );
    }

    @Test
    void does_not_ask_the_store_for_atomic_transitions_when_quarantine_is_switched_off() {
        RecordingModel model = new RecordingModel();

        SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .run("orders", saga(), new StoreWithoutAtomicTransitions<>(), c -> {
                }, null, SagaRunnerConfig.defaults().disableQuarantine())
                .close();

        assertThat(model.subscribed).isEqualTo(1);
    }

    // Cannot write atomically, and has no use for the rest of the skipped input methods, since with quarantine on the
    // runner refuses it at start
    private static final class StoreWithoutAtomicTransitions<S> extends StoreWithoutSkippedInputs<S> implements SagaStateStoreSkippedInputs {
        @Override
        public boolean fits(StoredSaga candidate) {
            return true;
        }

        @Override
        public void requireAtomicTransitions() {
            throw new IllegalStateException("no atomic transitions here");
        }

        @Override
        public List<String> findReleasingSagaIds(@Nullable String after, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInput> findWithFirstSkippedInput(String sagaId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean compareAndSaveSkipped(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput input) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
            throw new UnsupportedOperationException();
        }
    }

    private Saga<OrderEvent, String, ShipOrder> saga() {
        return Saga.<OrderEvent, String, ShipOrder>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> e.orderId())
                .build();
    }

    private List<String> warnings() {
        return new ArrayList<>(appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList());
    }

    private String warning() {
        return warnings().getFirst();
    }

    // Delegates to the in-memory store and, unlike it, does not implement SagaStateStoreSkippedInputs
    private static class StoreWithoutSkippedInputs<S> implements SagaStateStore<S> {
        private final SagaStateStore<S> delegate = SagaStateStore.inMemory();

        @Override
        public SagaStateCodec<S> stateCodec() {
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
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            return delegate.compareAndSaveWithoutState(saga, expectedVersion);
        }

        @Override
        public boolean compareAndSave(StoredSaga saga, long expectedVersion) {
            return delegate.compareAndSave(saga, expectedVersion);
        }

        @Override
        public List<StoredSaga> findWithDueTimers(Instant now, int limit) {
            return delegate.findWithDueTimers(now, limit);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }
    }

    private static class RecordingModel implements Subscribable {
        int subscribed;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            subscribed++;
            return new NoopSubscription(subscriptionId);
        }
    }

    private static final class NeverLeader implements CompetingConsumerStrategy {
        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return false;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return false;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }

    private record NoopSubscription(String id) implements Subscription {
        @Override
        public void waitUntilStarted() {
        }

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
