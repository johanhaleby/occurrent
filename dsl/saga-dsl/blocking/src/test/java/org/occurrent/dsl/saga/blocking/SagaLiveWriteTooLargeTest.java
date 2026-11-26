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
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInput;
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInputKey;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A live input asks its store whether it can write the instance with the input applied, before it dispatches a
 * command, and whenever the failure of an input is written the store is asked about that write too. When the store
 * cannot take a write, the input dispatches nothing and the failure takes the smaller write that records it, down to no
 * write at all. The store is driven here with a predicate over the candidate instance, and the saga is driven through
 * {@link SagaExecution} with a test clock.
 */
@DisplayName("A live saga input whose instance the store cannot write")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaLiveWriteTooLargeTest {

    private static final String ORDER = "o1";
    private static final Duration BUDGET = Duration.ofMinutes(1);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(15);
    private static final SagaRunnerConfig CONFIG = SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET)
            .withTimerPollInterval(POLL_INTERVAL)
            .withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT);
    private static final SagaRunnerConfig CONFIG_WITHOUT_QUARANTINE = SagaRunnerConfig.defaults().disableQuarantine()
            .withTimerPollInterval(POLL_INTERVAL)
            .withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT);
    // The input of the second event of the stream, the one these tests fail on
    private static final String FAILING_INPUT = ORDER + "@2";
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

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

    private volatile boolean aFails = false;
    private volatile Predicate<SagaEnvelope<Order>> fits = candidate -> true;
    private final AtomicReference<Instant> now = new AtomicReference<>(START);
    private final InMemoryEventStore eventStore = new InMemoryEventStore();
    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final List<Object> dispatched = new CopyOnWriteArrayList<>();
    private final AtomicInteger eventIds = new AtomicInteger();
    private final InMemorySagaStateStore<Order> inMemory = new InMemorySagaStateStore<>();
    private final SagaEnvelopes<Order> envelopes = SagaEnvelopes.of(inMemory, null);
    private final SagaCodec<Order> codec = SagaCodec.of(inMemory.stateCodec(), null);
    private final PredicateStore predicateStore = new PredicateStore();
    private ListAppender<ILoggingEvent> appender;
    private Logger executionLog;

    @BeforeEach
    void captureErrors() {
        appender = new ListAppender<>();
        appender.start();
        executionLog = (Logger) LoggerFactory.getLogger(SagaExecution.class);
        executionLog.addAppender(appender);
    }

    @AfterEach
    void stopCapturing() {
        executionLog.detachAppender(appender);
        appender.stop();
    }

    // The ERRORs other than the one that announces a quarantine, which every quarantine logs
    private List<String> errors() {
        return appender.list.stream().filter(e -> e.getLevel() == Level.ERROR).map(ILoggingEvent::getFormattedMessage)
                .filter(message -> !message.contains("is now QUARANTINED")).toList();
    }

    private Saga<OrderEvent, Order, Object> saga() {
        return Saga.<OrderEvent, Order, Object>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(e.orderId(), List.of()))
                .evolve(ItemAdded.class, (state, e) -> state.with(e.item()))
                .react(ItemAdded.class, (state, e) -> {
                    if (aFails && e.item().equals("a")) {
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.orderId(), e.item())));
                })
                .build();
    }

    private SagaExecution<OrderEvent, Order, Object> execution(SagaRunnerConfig config) {
        return new SagaExecution<>("orders", saga(), predicateStore, dispatched::add, converter, config, now::get);
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

    private SagaExecution<OrderEvent, Order, Object> placed(SagaRunnerConfig config) {
        SagaExecution<OrderEvent, Order, Object> execution = execution(config);
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        assertThat(instance().status()).as("control: placed").isEqualTo(SagaStatus.ACTIVE);
        return execution;
    }

    private void advanceThroughTheBudget() {
        now.set(now.get().plus(BUDGET));
    }

    private static boolean appliedItem(SagaEnvelope<Order> envelope, String item) {
        return envelope.state() != null && envelope.state().items().contains(item);
    }

    private static boolean hasFailureMessage(SagaEnvelope<Order> envelope) {
        return envelope.failure() != null && envelope.failure().failureMessage() != null;
    }

    private static boolean hasPending(SagaEnvelope<Order> envelope) {
        return envelope.streamMarks().values().stream().anyMatch(marks -> marks.pending().values().stream().anyMatch(versions -> !versions.isEmpty()));
    }

    private static boolean hasFailure(SagaEnvelope<Order> envelope) {
        return envelope.failure() != null;
    }

    @Test
    void an_input_whose_instance_the_store_cannot_write_dispatches_nothing_records_the_failure_on_the_event_and_quarantines_the_instance_on_it_once_the_budget_runs_out() {
        SagaExecution<OrderEvent, Order, Object> execution = placed(CONFIG);
        fits = candidate -> !appliedItem(candidate, "a");
        CloudEvent a = append(new ItemAdded(nextEventId(), ORDER, "a"));

        Throwable first = catchThrowable(() -> execution.onCloudEvent(a));
        SagaEnvelope<Order> afterTheFirst = instance();
        advanceThroughTheBudget();
        Throwable second = catchThrowable(() -> execution.onCloudEvent(a));
        SagaEnvelope<Order> quarantined = instance();

        assertAll(
                () -> assertThat(first).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + ORDER + "'")
                        .hasMessageContaining("cannot apply the event").hasMessageContaining("No command of it was dispatched."),
                () -> assertThat(second).as("the quarantining delivery is acknowledged").isNull(),
                () -> assertThat(dispatched).as("nothing is dispatched on either delivery").isEmpty(),
                () -> assertThat(afterTheFirst.status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(afterTheFirst.failure()).isNotNull(),
                () -> assertThat(afterTheFirst.failure().input()).as("the failure names the event").isEqualTo(FAILING_INPUT),
                () -> assertThat(afterTheFirst.failure().failureMessage()).contains("cannot apply the event"),
                () -> assertThat(afterTheFirst.state().items()).as("the event is not applied").isEmpty(),
                () -> assertThat(quarantined.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(quarantined.failure().input()).isEqualTo(FAILING_INPUT),
                () -> assertThat(quarantined.failure().firstFailedAt()).as("the budget ran from the first failure").isEqualTo(START),
                () -> assertThat(quarantined.state().items()).isEmpty(),
                () -> assertThat(errors()).as("every write fit, so no ERROR").isEmpty()
        );
    }

    @Test
    void an_instance_quarantined_on_an_input_the_store_could_not_write_is_released_once_the_store_can_and_the_input_is_then_applied_and_dispatched_once() {
        SagaExecution<OrderEvent, Order, Object> execution = placed(CONFIG);
        fits = candidate -> !appliedItem(candidate, "a");
        CloudEvent a = append(new ItemAdded(nextEventId(), ORDER, "a"));
        catchThrowable(() -> execution.onCloudEvent(a));
        advanceThroughTheBudget();
        catchThrowable(() -> execution.onCloudEvent(a));
        assertThat(instance().status()).as("control: quarantined").isEqualTo(SagaStatus.QUARANTINED);
        fits = candidate -> true;

        ReleaseResult result = execution.release(ORDER);

        assertAll(
                () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(dispatched).containsExactly(new Pack(ORDER, "a")),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(instance().state().items()).containsExactly("a")
        );
    }

    @Test
    void with_quarantine_off_an_input_whose_instance_the_store_cannot_write_dispatches_nothing_fails_the_delivery_and_is_listed_as_pending() {
        SagaExecution<OrderEvent, Order, Object> execution = placed(CONFIG_WITHOUT_QUARANTINE);
        fits = candidate -> !appliedItem(candidate, "a");
        CloudEvent a = append(new ItemAdded(nextEventId(), ORDER, "a"));

        Throwable thrown = catchThrowable(() -> execution.onCloudEvent(a));

        SagaEnvelope<Order> after = instance();
        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("No command of it was dispatched."),
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(after.state().items()).as("the event is not applied").isEmpty(),
                () -> assertThat(hasPending(after)).as("the event is listed as pending").isTrue(),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(errors()).isEmpty()
        );
    }

    @Test
    void a_failure_record_the_store_cannot_write_in_full_is_written_without_its_message_and_without_listing_the_input_and_the_budget_still_runs_to_a_quarantine_dispatching_nothing() {
        SagaExecution<OrderEvent, Order, Object> execution = placed(CONFIG);
        aFails = true;
        fits = candidate -> !hasFailureMessage(candidate) && !hasPending(candidate);
        CloudEvent a = append(new ItemAdded(nextEventId(), ORDER, "a"));
        long versionBefore = instance().version();

        Throwable first = catchThrowable(() -> execution.onCloudEvent(a));
        SagaEnvelope<Order> afterTheFirst = instance();
        Throwable sameInstant = catchThrowable(() -> execution.onCloudEvent(a));
        SagaEnvelope<Order> afterTheRedelivery = instance();
        advanceThroughTheBudget();
        Throwable late = catchThrowable(() -> execution.onCloudEvent(a));
        SagaEnvelope<Order> quarantined = instance();

        assertAll(
                () -> assertThat(first).isInstanceOf(IllegalStateException.class).hasMessage("cannot pack a"),
                () -> assertThat(afterTheFirst.failure()).isNotNull(),
                () -> assertThat(afterTheFirst.failure().input()).isEqualTo(FAILING_INPUT),
                () -> assertThat(afterTheFirst.failure().failureMessage()).as("no message").isNull(),
                () -> assertThat(afterTheFirst.failure().failureType()).isEqualTo(IllegalStateException.class.getName()),
                () -> assertThat(hasPending(afterTheFirst)).as("the input is not listed as pending").isFalse(),
                () -> assertThat(afterTheFirst.version()).isGreaterThan(versionBefore),
                () -> assertThat(afterTheFirst.status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(sameInstant).isInstanceOf(IllegalStateException.class),
                () -> assertThat(afterTheRedelivery.version()).as("the record names the event already, so nothing is written").isEqualTo(afterTheFirst.version()),
                () -> assertThat(late).as("the quarantining delivery is acknowledged").isNull(),
                () -> assertThat(quarantined.status()).as("the budget still runs").isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(quarantined.failure().input()).isEqualTo(FAILING_INPUT),
                () -> assertThat(quarantined.failure().firstFailedAt()).as("the clock of the first failure is kept").isEqualTo(START),
                () -> assertThat(quarantined.failure().failureMessage()).isNull(),
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(errors()).as("a bare record fit, so no ERROR").isEmpty()
        );
    }

    @Test
    void a_failure_record_the_store_cannot_write_even_without_its_message_is_not_written_logs_an_error_naming_the_instance_and_the_delivery_fails_on_every_redelivery_without_ever_quarantining() {
        SagaExecution<OrderEvent, Order, Object> execution = placed(CONFIG);
        aFails = true;
        fits = candidate -> !hasFailure(candidate);
        CloudEvent a = append(new ItemAdded(nextEventId(), ORDER, "a"));
        SagaEnvelope<Order> before = instance();

        Throwable first = catchThrowable(() -> execution.onCloudEvent(a));
        List<String> errorsAfterTheFirst = errors();
        advanceThroughTheBudget();
        Throwable second = catchThrowable(() -> execution.onCloudEvent(a));
        advanceThroughTheBudget();
        Throwable third = catchThrowable(() -> execution.onCloudEvent(a));

        assertAll(
                () -> assertThat(first).isInstanceOf(IllegalStateException.class).hasMessage("cannot pack a"),
                () -> assertThat(second).isInstanceOf(IllegalStateException.class).hasMessage("cannot pack a"),
                () -> assertThat(third).isInstanceOf(IllegalStateException.class).hasMessage("cannot pack a"),
                () -> assertThat(instance()).as("nothing is written, the version included").isEqualTo(before),
                () -> assertThat(instance().status()).as("never quarantined, however long it fails").isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(errorsAfterTheFirst).singleElement().asString().contains("'" + ORDER + "'").contains("could not record the failure")
                        .contains("SagaStateStore.delete(\"" + ORDER + "\")"),
                () -> assertThat(errors()).as("one ERROR for every delivery").hasSize(3),
                () -> assertThat(dispatched).isEmpty()
        );
    }

    @Test
    void a_write_that_quarantines_the_instance_and_does_not_fit_in_full_quarantines_it_with_a_bare_record_that_keeps_the_input_and_its_first_failure_and_the_instance_is_released_afterwards() {
        SagaExecution<OrderEvent, Order, Object> execution = placed(CONFIG);
        aFails = true;
        fits = candidate -> !(candidate.isQuarantined() && hasFailureMessage(candidate));
        CloudEvent a = append(new ItemAdded(nextEventId(), ORDER, "a"));

        catchThrowable(() -> execution.onCloudEvent(a));
        SagaEnvelope<Order> active = instance();
        advanceThroughTheBudget();
        Throwable thrown = catchThrowable(() -> execution.onCloudEvent(a));
        SagaEnvelope<Order> quarantined = instance();
        aFails = false;
        fits = candidate -> true;
        ReleaseResult released = execution.release(ORDER);

        assertAll(
                () -> assertThat(active.failure().failureMessage()).as("control: the first failure is recorded in full").isEqualTo("cannot pack a"),
                () -> assertThat(hasPending(active)).as("control: and the input is listed as pending").isTrue(),
                () -> assertThat(thrown).as("the quarantining delivery is acknowledged").isNull(),
                () -> assertThat(quarantined.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(quarantined.quarantineId()).isNotNull(),
                () -> assertThat(quarantined.failure().input()).isEqualTo(FAILING_INPUT),
                () -> assertThat(quarantined.failure().failureMessage()).as("the bare record has no message").isNull(),
                () -> assertThat(quarantined.failure().firstFailedAt()).isEqualTo(START),
                () -> assertThat(errors()).isEmpty(),
                () -> assertThat(released).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(dispatched).containsExactly(new Pack(ORDER, "a")),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE)
        );
    }

    @Test
    void with_quarantine_off_a_pending_record_the_store_cannot_write_is_not_written_logs_an_error_and_the_delivery_fails_dispatching_nothing() {
        SagaExecution<OrderEvent, Order, Object> execution = placed(CONFIG_WITHOUT_QUARANTINE);
        aFails = true;
        fits = candidate -> !hasPending(candidate);
        CloudEvent a = append(new ItemAdded(nextEventId(), ORDER, "a"));
        SagaEnvelope<Order> before = instance();

        Throwable thrown = catchThrowable(() -> execution.onCloudEvent(a));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("cannot pack a"),
                () -> assertThat(instance()).as("nothing is written, the version included").isEqualTo(before),
                () -> assertThat(errors()).singleElement().asString().contains("'" + ORDER + "'").contains("could not list it as pending"),
                () -> assertThat(dispatched).isEmpty()
        );
    }

    @Test
    void an_input_the_store_can_write_is_dispatched_and_saved_as_before() {
        SagaExecution<OrderEvent, Order, Object> execution = placed(CONFIG);
        fits = candidate -> true;

        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "a")));

        assertAll(
                () -> assertThat(dispatched).containsExactly(new Pack(ORDER, "a")),
                () -> assertThat(instance().state().items()).containsExactly("a"),
                () -> assertThat(errors()).isEmpty()
        );
    }

    // Answers fits(..) with the test's predicate over the decoded candidate
    private class PredicateStore implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
        @Override
        public boolean fits(StoredSaga candidate) {
            return fits.test(codec.decode(candidate));
        }

        // The stored instance with the fields the write sets replaced
        @Override
        public boolean fitsWithoutState(StoredSaga saga, long expectedVersion) {
            StoredSaga stored = inMemory.find(saga.sagaId()).orElse(null);
            if (stored == null || stored.version() != expectedVersion) {
                return true;
            }
            return fits(SagaEnvelopes.withFieldsExceptStateOf(stored, saga));
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
}
