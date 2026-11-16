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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.HistoryRetainingSubscriptions;
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
import java.util.stream.Stream;

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
    void names_the_model_that_retains_nothing_and_says_isolation_is_gone() {
        run(new RetainsNothing());

        assertAll(
                () -> assertThat(warnings()).hasSize(1),
                () -> assertThat(warning()).contains("cannot say whether it still holds an event it delivered"),
                () -> assertThat(warning()).contains(RetainsNothing.class.getName()),
                () -> assertThat(warning()).contains("What its failing event holds up is decided by whatever feeds this subscription")
        );
    }

    /**
     * The remedy an operator is given has to be one they do not already have. An earlier message sent every feed to a
     * catch-up model over MongoDB, which a push saga catching up from the event store was already running, and which
     * does not make its events retained anyway.
     */
    @Test
    void does_not_offer_a_catch_up_model_as_the_remedy_for_a_feed_that_cannot_have_one() {
        run(new RetainsNothing());

        assertAll(
                () -> assertThat(warning()).doesNotContain("Run the saga on one of the MongoDB subscription models"),
                () -> assertThat(warning()).contains("A push feed on its own does not"),
                () -> assertThat(warning()).contains("issues/918")
        );
    }

    @Test
    void says_nothing_at_all_when_the_model_holds_everything_it_delivers() {
        run(new Retains());

        assertThat(warnings()).isEmpty();
    }

    /**
     * Answering for one event is not enough to quarantine on, because a quarantined instance skips everything
     * addressed to it afterwards and skipping acknowledges. A model holding only some of what it delivers could have
     * one of those later events as its only copy, so quarantine stays off for it and it is told why.
     */
    @Test
    void switches_quarantine_off_when_the_model_cannot_guarantee_it_holds_everything() {
        run(new RetainsSomeEvents());

        assertAll(
                () -> assertThat(warnings()).hasSize(1),
                () -> assertThat(warning()).contains("cannot guarantee it holds every event it delivers"),
                () -> assertThat(warning()).contains("quarantine is switched off"),
                () -> assertThat(warning()).contains("skips everything addressed to it afterwards")
        );
    }

    /**
     * A saga that switched the budget off asked for the pre-0.34.0 behaviour, so it has given nothing up and is not
     * warned. Without this the message would fire for every saga that deliberately opted out.
     */
    @Test
    void says_nothing_when_quarantine_was_switched_off_deliberately() {
        SagaRunner.<OrderEvent, ShipOrder>agnostic(new RetainsNothing(), converter)
                .run("orders", saga(), SagaStateStore.inMemory(), c -> {
                }, null, SagaRunnerConfig.defaults().disableQuarantine())
                .close();

        assertThat(warnings()).isEmpty();
    }

    /**
     * The default configuration quarantines after five minutes, so a saga on it gives quarantine up on such a model. It
     * never asked for quarantine, so this is a debug line and not a warning.
     */
    @ParameterizedTest
    @MethodSource("modelsThatCannotSupportQuarantine")
    @DisplayName("says that quarantine is switched off under the default configuration at debug and not at warn")
    void says_that_quarantine_is_switched_off_under_the_default_configuration_at_debug_and_not_at_warn(Subscribable model) {
        runnerLog.setLevel(Level.DEBUG);

        SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .run("orders", saga(), SagaStateStore.inMemory(), c -> {
                }, null, SagaRunnerConfig.defaults())
                .close();

        assertAll(
                () -> assertThat(switchedOffLines(Level.WARN)).as("the lines at WARN").isEmpty(),
                () -> assertThat(switchedOffLines(Level.DEBUG)).as("the lines at DEBUG").hasSize(1)
        );
    }

    /**
     * Setting the budget to what the default is still asks for quarantine, so it is told at warn.
     */
    @ParameterizedTest
    @MethodSource("modelsThatCannotSupportQuarantine")
    @DisplayName("warns that quarantine is switched off when a budget equal to the default was set explicitly")
    void warns_that_quarantine_is_switched_off_when_a_budget_equal_to_the_default_was_set_explicitly(Subscribable model) {
        runnerLog.setLevel(Level.DEBUG);

        SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .run("orders", saga(), SagaStateStore.inMemory(), c -> {
                }, null, SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMinutes(5)))
                .close();

        assertAll(
                () -> assertThat(switchedOffLines(Level.WARN)).as("the lines at WARN").hasSize(1),
                () -> assertThat(switchedOffLines(Level.DEBUG)).as("the lines at DEBUG").isEmpty()
        );
    }

    private List<String> switchedOffLines(Level level) {
        return new ArrayList<>(appender.list).stream()
                .filter(event -> event.getLevel() == level)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains("quarantine is switched off for this saga"))
                .toList();
    }

    private static Stream<Subscribable> modelsThatCannotSupportQuarantine() {
        return Stream.of(new RetainsNothing(), new RetainsSomeEvents());
    }

    /**
     * A model that holds every event it delivers lets a quarantined saga skip them, and the release then asks the event
     * store for what it skipped. A runner with nothing to ask would quarantine and never be able to release, so it
     * refuses to start and names both ways out.
     */
    @Test
    void refuses_to_start_when_quarantine_is_on_the_model_holds_every_event_and_there_is_no_event_store_queries() {
        Retains model = new Retains();

        Throwable thrown = catchThrowable(() -> SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .run("orders", saga(), SagaStateStore.inMemory(), c -> {
                }, null, SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMinutes(5))));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class),
                () -> assertThat(thrown).hasMessageContaining("SagaRunner.eventStoreQueries(..)"),
                () -> assertThat(thrown).hasMessageContaining("SagaRunnerConfig.disableQuarantine()"),
                () -> assertThat(model.subscribed).isZero()
        );
    }

    /**
     * Quarantine records what the instance skips and a release reads it back, both through the optional
     * SagaStateStoreSkippedInputs capability. A store without it would quarantine and then have nowhere to put the
     * skipped inputs, so the runner refuses to start instead of finding out on the first quarantine.
     */
    @Test
    void refuses_to_start_when_quarantine_is_on_the_model_holds_every_event_and_the_store_cannot_record_skipped_inputs() {
        Retains model = new Retains();

        Throwable thrown = catchThrowable(() -> SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .eventStoreQueries(new InMemoryEventStore())
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
        Retains model = new Retains();

        SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .eventStoreQueries(new InMemoryEventStore())
                .run("orders", saga(), new StoreWithoutSkippedInputs<>(), c -> {
                }, null, SagaRunnerConfig.defaults().disableQuarantine())
                .close();

        assertAll(
                () -> assertThat(model.subscribed).isEqualTo(1),
                () -> assertThat(warnings()).isEmpty()
        );
    }

    @Test
    void starts_without_event_store_queries_when_quarantine_is_off_on_a_model_that_holds_every_event() {
        Retains model = new Retains();

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
    void starts_without_event_store_queries_on_a_model_that_cannot_hold_every_event_with_quarantine_switched_off() {
        RetainsSomeEvents model = new RetainsSomeEvents();

        SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .run("orders", saga(), SagaStateStore.inMemory(), c -> {
                }, null, SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMinutes(5)))
                .close();

        assertAll(
                () -> assertThat(model.subscribed).isEqualTo(1),
                () -> assertThat(warnings()).hasSize(1),
                () -> assertThat(warning()).contains("quarantine is switched off")
        );
    }

    @Test
    void starts_on_a_model_that_holds_every_event_when_there_are_event_store_queries() {
        Retains model = new Retains();

        run(model);

        assertAll(
                () -> assertThat(model.subscribed).isEqualTo(1),
                () -> assertThat(warnings()).isEmpty()
        );
    }

    private void run(Subscribable model) {
        SagaRunner.<OrderEvent, ShipOrder>agnostic(model, converter)
                .eventStoreQueries(new InMemoryEventStore())
                .run("orders", saga(), SagaStateStore.inMemory(), c -> {
                }, null, SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMinutes(5)))
                .close();
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
    private static final class StoreWithoutSkippedInputs<S> implements SagaStateStore<S> {
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

    private static class RetainsNothing implements Subscribable {
        int subscribed;

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            subscribed++;
            return new NoopSubscription(subscriptionId);
        }
    }

    private static final class Retains extends RetainsNothing implements HistoryRetainingSubscriptions {
        @Override
        public boolean retains(CloudEvent event) {
            return true;
        }

        @Override
        public boolean retainsEveryEvent() {
            return true;
        }
    }

    // Can answer for an event and cannot promise to hold everything, which is what earns the startup line. Its
    // per-event answer is never asked, because the missing promise switches quarantine off before that.
    private static final class RetainsSomeEvents extends RetainsNothing implements HistoryRetainingSubscriptions {
        @Override
        public boolean retains(CloudEvent event) {
            return false;
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
