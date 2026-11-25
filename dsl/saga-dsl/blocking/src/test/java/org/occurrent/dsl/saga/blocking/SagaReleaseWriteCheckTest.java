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
import org.occurrent.dsl.saga.StoredSagaWithFirstSkippedInputKey;
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
import org.occurrent.dsl.saga.SagaEnvelope.StreamMarks;
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStateStoreSkippedInputs;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.SkippedInput;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A release asks its store before every write it makes, see {@link SagaStateStoreSkippedInputs#fits}. The store is a
 * predicate over the whole candidate instance here, so each test names the one write the store refuses. Driven through
 * {@link SagaExecution} with a test clock.
 */
@DisplayName("Releasing a quarantined saga instance checks every write it makes with the store")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaReleaseWriteCheckTest {

    private static final String ORDER = "o1";
    private static final Duration BUDGET = Duration.ofMinutes(1);
    private static final SagaRunnerConfig CONFIG = SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET)
            .withTimerPollInterval(Duration.ofSeconds(15))
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
    private volatile boolean aFails = false;
    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    private final InMemoryEventStore eventStore = new InMemoryEventStore();
    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final List<Object> dispatched = new CopyOnWriteArrayList<>();
    private final AtomicInteger eventIds = new AtomicInteger();
    private final InMemorySagaStateStore<Order> inMemory = new InMemorySagaStateStore<>();
    private final SagaEnvelopes<Order> envelopes = SagaEnvelopes.of(inMemory, null);
    private final SagaCodec<Order> codec = SagaCodec.of(inMemory.stateCodec(), null);
    private final List<StoredSaga> asked = new CopyOnWriteArrayList<>();
    private volatile Predicate<SagaEnvelope<Order>> fits = candidate -> true;
    private volatile @Nullable Runnable beforeTheNextReplayedWrite;
    private final PredicateStore store = new PredicateStore();
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

    private List<String> errors() {
        return appender.list.stream().filter(e -> e.getLevel() == Level.ERROR).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private Saga<OrderEvent, Order, Object> saga() {
        return Saga.<OrderEvent, Order, Object>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(e.orderId(), List.of()))
                .evolve(ItemAdded.class, (state, e) -> state.with(e.item()))
                .react(ItemAdded.class, (state, e) -> {
                    if (poisonFails && e.item().equals("poison") || aFails && e.item().equals("a")) {
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

    private List<SkippedInput> records() {
        return inMemory.findSkippedInputs(ORDER, instance().quarantineId(), Integer.MAX_VALUE);
    }

    // Quarantined on "poison", with "a" and "b" skipped after it, so a release has three records to replay
    private SagaExecution<OrderEvent, Order, Object> quarantinedOnPoisonWithTwoSkipped() {
        SagaExecution<OrderEvent, Order, Object> execution = new SagaExecution<>("orders", saga(), store, dispatched::add, converter, CONFIG, now::get);
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        CloudEvent poison = append(new ItemAdded(nextEventId(), ORDER, "poison"));
        catchThrowable(() -> execution.onCloudEvent(poison));
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> execution.onCloudEvent(poison));
        assertThat(instance().status()).as("control: quarantined").isEqualTo(SagaStatus.QUARANTINED);
        poisonFails = false;
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "a")));
        execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "b")));
        assertThat(records()).as("control: three records").hasSize(3);
        appender.list.clear();
        return execution;
    }

    private static boolean appliedItem(SagaEnvelope<Order> envelope, String item) {
        return envelope.state() != null && envelope.state().items().contains(item);
    }

    private static boolean carriesMessage(SagaEnvelope<Order> envelope, String prefix) {
        return envelope.failure() != null && envelope.failure().failureMessage() != null && envelope.failure().failureMessage().startsWith(prefix);
    }

    @Test
    void a_store_that_cannot_write_the_release_started_makes_release_throw_naming_the_instance_and_the_delete_and_leaves_the_instance_as_it_was() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped();
        SagaEnvelope<Order> before = instance();
        fits = candidate -> !candidate.isReleasing();

        Throwable thrown = catchThrowable(() -> execution.release(ORDER));

        SagaEnvelope<Order> after = instance();
        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining(ORDER).hasMessageContaining("SagaStateStore.delete"),
                () -> assertThat(after.version()).as("nothing is written").isEqualTo(before.version()),
                () -> assertThat(after.isReleasing()).isFalse(),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(records()).hasSize(3),
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(errors()).singleElement().asString().contains(ORDER)
        );
    }

    @Test
    void a_failure_record_the_store_cannot_write_quarantines_the_instance_again_on_the_failing_record_without_applying_or_dispatching_it() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped();
        aFails = true;
        fits = candidate -> !carriesMessage(candidate, "cannot pack a");

        ReleaseResult result = execution.release(ORDER);

        SagaEnvelope<Order> after = instance();
        List<SkippedInput> remaining = records();
        assertAll(
                () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.isReleasing()).isFalse(),
                () -> assertThat(remaining).extracting(SkippedInput::key).as("a and b stay recorded, poison was applied").hasSize(2).first().isEqualTo(after.failure().input()),
                () -> assertThat(after.failure().failureMessage()).contains("failure record"),
                () -> assertThat(after.state().items()).containsExactly("poison"),
                () -> assertThat(dispatched).as("only the record applied before the failure").containsExactly(new Pack(ORDER, "poison")),
                () -> assertThat(errors()).singleElement().asString().contains("without applying it").contains("cannot write the failure record")
        );
    }

    @Test
    void a_full_requarantine_the_store_cannot_write_falls_back_to_the_bare_stop_with_no_failure_message_and_dispatches_nothing() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped();
        String firstKey = records().getFirst().key();
        fits = candidate -> !appliedItem(candidate, "poison") && !carriesMessage(candidate, "The release of saga instance");

        ReleaseResult result = execution.release(ORDER);

        SagaEnvelope<Order> after = instance();
        assertAll(
                () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.isReleasing()).isFalse(),
                () -> assertThat(after.failure().input()).isEqualTo(firstKey),
                () -> assertThat(after.failure().failureMessage()).isNull(),
                () -> assertThat(records()).hasSize(3),
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(errors()).singleElement().asString().contains("QUARANTINED again")
        );
    }

    @Test
    void when_the_store_cannot_write_the_applied_instance_or_either_requarantine_release_writes_nothing_returns_STILL_RELEASING_and_says_to_delete_the_instance() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped();
        fits = candidate -> candidate.isReleasing() && !appliedItem(candidate, "poison");

        ReleaseResult first = execution.release(ORDER);
        SagaEnvelope<Order> afterTheFirst = instance();
        ReleaseResult second = execution.release(ORDER);

        assertAll(
                () -> assertThat(first).isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(second).isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(afterTheFirst.isReleasing()).isTrue(),
                () -> assertThat(afterTheFirst.state().items()).isEmpty(),
                () -> assertThat(instance().version()).as("the second release writes nothing either").isEqualTo(afterTheFirst.version()),
                () -> assertThat(records()).hasSize(3),
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(errors()).hasSize(2).allSatisfy(message -> assertThat(message).contains(ORDER).contains("SagaStateStore.delete"))
        );
    }

    @Test
    void a_store_that_cannot_write_the_instance_released_returns_STILL_RELEASING_with_every_record_applied_and_the_instance_still_releasing() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped();
        fits = candidate -> candidate.status() != SagaStatus.ACTIVE;

        ReleaseResult first = execution.release(ORDER);
        SagaEnvelope<Order> afterTheFirst = instance();
        ReleaseResult second = execution.release(ORDER);

        assertAll(
                () -> assertThat(first).isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(second).isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(afterTheFirst.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(afterTheFirst.isReleasing()).isTrue(),
                () -> assertThat(afterTheFirst.state().items()).containsExactly("poison", "a", "b"),
                () -> assertThat(instance().version()).isEqualTo(afterTheFirst.version()),
                () -> assertThat(records()).isEmpty(),
                () -> assertThat(dispatched).as("each record once").containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b")),
                () -> assertThat(errors()).hasSize(2).allSatisfy(message -> assertThat(message).contains(ORDER).contains("SagaStateStore.delete"))
        );
    }

    @Test
    void a_store_that_cannot_write_the_deletion_of_a_record_whose_input_the_instance_applied_already_returns_STILL_RELEASING_and_writes_nothing() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped();
        markPoisonApplied();
        long versionBeforeTheRelease = instance().version();
        fits = candidate -> candidate.version() < versionBeforeTheRelease + 2;

        ReleaseResult result = execution.release(ORDER);

        SagaEnvelope<Order> after = instance();
        assertAll(
                () -> assertThat(result).isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(after.version()).as("only the write that started the release").isEqualTo(versionBeforeTheRelease + 1),
                () -> assertThat(after.isReleasing()).isTrue(),
                () -> assertThat(records()).hasSize(3),
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(errors()).singleElement().asString().contains(ORDER).contains("SagaStateStore.delete")
        );
    }

    // A skip recorded while the release writes a record loses its compare-and-set, and the write is rebased onto it
    @Test
    void a_write_that_loses_to_a_recorded_skip_is_asked_again_rebased_and_when_the_store_refuses_that_it_is_not_saved_and_the_record_is_not_dispatched_again() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped();
        long skipCountBefore = instance().quarantineSkipCount();
        List<Boolean> verdictsOnTheEventApplied = new CopyOnWriteArrayList<>();
        fits = candidate -> {
            boolean fitsIt = candidate.quarantineSkipCount() <= skipCountBefore;
            if (appliedItem(candidate, "poison")) {
                verdictsOnTheEventApplied.add(fitsIt);
            }
            return fitsIt;
        };
        beforeTheNextReplayedWrite = () -> execution.onCloudEvent(append(new ItemAdded(nextEventId(), ORDER, "c")));

        ReleaseResult result = execution.release(ORDER);

        SagaEnvelope<Order> after = instance();
        assertAll(
                () -> assertThat(verdictsOnTheEventApplied).as("control: the first candidate fits and the rebased one does not").startsWith(true, false),
                () -> assertThat(after.quarantineSkipCount()).as("control: the skip was recorded").isGreaterThan(skipCountBefore),
                () -> assertThat(result).isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(after.state().items()).as("the rebased write was not saved").isEmpty(),
                () -> assertThat(after.isReleasing()).isTrue(),
                () -> assertThat(records()).as("the three records and the skip stay recorded").hasSize(4),
                () -> assertThat(dispatched).as("the record is dispatched by the first write only").containsExactly(new Pack(ORDER, "poison")),
                () -> assertThat(errors()).singleElement().asString().contains(ORDER).contains("SagaStateStore.delete")
        );
    }

    @Test
    void a_candidate_for_a_write_without_the_state_that_is_passed_to_fits_carries_the_state_the_store_holds() {
        quarantinedOnPoisonWithTwoSkipped();
        SagaEnvelope<Order> stored = instance();
        SagaEnvelope<Order> withoutState = envelopes.findWithoutState(ORDER).orElseThrow();
        asked.clear();

        boolean fitsIt = envelopes.fitsWithoutState(store, withoutState, stored.version());

        assertAll(
                () -> assertThat(fitsIt).isTrue(),
                () -> assertThat(asked).hasSize(1),
                () -> assertThat(codec.decode(asked.getFirst()).state()).isNotNull().isEqualTo(stored.state())
        );
    }

    @Test
    void a_write_the_instance_has_moved_past_counts_as_fitting_since_the_compare_and_set_refuses_it() {
        quarantinedOnPoisonWithTwoSkipped();
        SagaEnvelope<Order> withoutState = envelopes.findWithoutState(ORDER).orElseThrow();
        fits = candidate -> false;
        asked.clear();

        boolean fitsIt = envelopes.fitsWithoutState(store, withoutState, instance().version() - 1);

        assertAll(
                () -> assertThat(fitsIt).isTrue(),
                () -> assertThat(asked).as("the store is not asked").isEmpty()
        );
    }

    // The instance has applied the input of the first record already, so the release only deletes that record
    private void markPoisonApplied() {
        SagaEnvelope<Order> current = instance();
        SkippedInput poison = records().getFirst();
        String type = poison.event().getType();
        long version = streamVersionOf(poison.event());
        StreamMarks existing = current.streamMarks().get(ORDER);
        Map<String, TypeMark> byType = new HashMap<>(existing.byType());
        byType.put(type, new TypeMark(version, List.of(version), 0));
        Map<String, StreamMarks> marks = new HashMap<>(current.streamMarks());
        marks.put(ORDER, new StreamMarks(existing.floor(), byType, Map.of(), existing.legacyFailed(), Map.of()));
        SagaEnvelope<Order> seeded = new SagaEnvelope<>(current.sagaId(), current.state(), current.status(), current.version(), current.timers(),
                current.streamWatermarks(), current.positionWatermark(), current.createdAt(), current.updatedAt(), current.completedAt(),
                current.currentStep(), current.started(), current.failure(), marks, current.quarantineSkipCount());
        assertThat(envelopes.compareAndSave(ORDER, seeded, current.version())).isTrue();
    }

    private static long streamVersionOf(CloudEvent event) {
        Object version = event.getExtension("streamversion");
        return Long.parseLong(String.valueOf(version));
    }

    // Delegates to the in-memory store, and answers fits with the predicate over the decoded candidate
    private final class PredicateStore implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
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
        public void delete(String sagaId) {
            inMemory.delete(sagaId);
        }

        @Override
        public void requireAtomicTransitions() {
            inMemory.requireAtomicTransitions();
        }

        @Override
        public List<String> findReleasingSagaIds(@Nullable String after, int limit) {
            return inMemory.findReleasingSagaIds(after, limit);
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
        public boolean compareAndSaveSkipped(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput input) {
            return inMemory.compareAndSaveSkipped(saga, expectedVersion, quarantineId, input);
        }

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            Runnable hook = beforeTheNextReplayedWrite;
            if (hook != null) {
                beforeTheNextReplayedWrite = null;
                hook.run();
            }
            return inMemory.compareAndSaveReplayed(saga, expectedVersion, quarantineId, replayed);
        }

        @Override
        public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
            return inMemory.compareAndSaveQuarantineEnded(saga, expectedVersion, quarantineId);
        }

        @Override
        public boolean fits(StoredSaga candidate) {
            asked.add(candidate);
            return fits.test(codec.decode(candidate));
        }
    }
}
