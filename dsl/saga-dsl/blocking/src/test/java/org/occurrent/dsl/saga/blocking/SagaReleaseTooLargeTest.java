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
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A release asks its store whether it can write the instance with a record applied, before it dispatches the record's
 * commands. When the store cannot, the release quarantines the instance again on the record with its stream marks
 * trimmed, and dispatches nothing. The store is driven here with a predicate over the candidate instance, and the
 * instance is made large by seeding applied versions into its stream marks. Driven through {@link SagaExecution} with a
 * test clock.
 */
@DisplayName("Releasing a quarantined saga instance whose next write the store cannot take")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaReleaseTooLargeTest {

    private static final String ORDER = "o1";
    private static final String SEEDED_TYPE = "seeded";
    private static final int SEEDED_VERSIONS = 5_000;
    private static final int LIMIT = 1_000;
    private static final Duration BUDGET = Duration.ofMinutes(1);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(15);
    private static final SagaRunnerConfig CONFIG = SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET)
            .withTimerPollInterval(POLL_INTERVAL)
            .withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT);
    private static final SagaRunnerConfig CONFIG_WITHOUT_QUARANTINE = SagaRunnerConfig.defaults().disableQuarantine()
            .withTimerPollInterval(POLL_INTERVAL)
            .withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT);
    private static final String RELEASE_AGAIN = "so release the instance again to go on from that event";
    private static final String STOPS_AGAIN = "even with the stream versions the release applied trimmed. A further release stops on this event too.";

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
    private volatile boolean poisonFails = false;
    private volatile Predicate<SagaEnvelope<Order>> fits = candidate -> true;
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

    private SagaExecution<OrderEvent, Order, Object> execution(SagaStateStore<Order> store, SagaRunnerConfig config) {
        return new SagaExecution<>("orders", saga(), store, dispatched::add, converter, config, now::get);
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

    private static long appliedVersions(SagaEnvelope<Order> envelope) {
        long count = 0;
        for (StreamMarks marks : envelope.streamMarks().values()) {
            for (TypeMark typeMark : marks.byType().values()) {
                count += typeMark.applied().size();
            }
        }
        return count;
    }

    private static Map<String, Object> withoutVersion(StoredSaga stored) {
        Map<String, Object> fields = new HashMap<>(stored.encoded().fields());
        fields.remove("version");
        return fields;
    }

    private static boolean appliedItems(SagaEnvelope<Order> envelope, String item) {
        return envelope.state() != null && envelope.state().items().contains(item);
    }

    // Quarantined on "poison", with "a" and "b" skipped after it, so a release has three records to replay
    private SagaExecution<OrderEvent, Order, Object> quarantinedOnPoisonWithTwoSkipped(SagaStateStore<Order> store, SagaRunnerConfig config) {
        SagaExecution<OrderEvent, Order, Object> execution = execution(store, CONFIG);
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
        assertThat(inMemory.findSkippedInputs(ORDER, instance().quarantineId(), Integer.MAX_VALUE)).as("control: three records").hasSize(3);
        appender.list.clear();
        return config == CONFIG ? execution : execution(store, config);
    }

    // Adds applied versions of an event type no saga event has, so the instance keeps many versions behind the reorder
    // window. A release trims them when it quarantines the instance again
    private void keepSeededVersions() {
        SagaEnvelope<Order> current = instance();
        List<Long> applied = new ArrayList<>();
        for (long version = 0; version < SEEDED_VERSIONS; version++) {
            applied.add(1_000_000L + version);
        }
        StreamMarks existing = current.streamMarks().get(ORDER);
        Map<String, TypeMark> byType = new HashMap<>(existing.byType());
        byType.put(SEEDED_TYPE, new TypeMark(1_000_000L + SEEDED_VERSIONS, applied, 0));
        Map<String, StreamMarks> marks = new HashMap<>(current.streamMarks());
        marks.put(ORDER, new StreamMarks(existing.floor(), byType, existing.pending(), existing.legacyFailed(), existing.pendingSince()));
        SagaEnvelope<Order> seeded = new SagaEnvelope<>(current.sagaId(), current.state(), current.status(), current.version(), current.timers(),
                current.streamWatermarks(), current.positionWatermark(), current.createdAt(), current.updatedAt(), current.completedAt(),
                current.currentStep(), current.started(), current.failure(), marks, current.quarantineSkipCount());
        assertThat(envelopes.compareAndSave(ORDER, seeded, current.version())).isTrue();
        assertThat(appliedVersions(instance())).as("control: the instance keeps the seeded versions").isGreaterThanOrEqualTo(SEEDED_VERSIONS);
    }

    // Applied versions and state items, because applying a record here adds an item and no applied version
    private static long size(SagaEnvelope<Order> envelope) {
        return appliedVersions(envelope) + (envelope.state() == null ? 0 : envelope.state().items().size());
    }

    // Seeds the versions and lets the store take the size the instance holds and no more. The release start has that
    // size and fits, applying a record adds an item and does not, and the quarantine again trims the marks and fits
    private long seedVersionsAndLimitStoreToThem() {
        keepSeededVersions();
        long held = size(instance());
        fits = candidate -> size(candidate) <= held;
        return held;
    }

    @Test
    void a_release_whose_start_does_not_fit_with_every_mark_kept_is_written_trimmed_and_releases_in_one_call_dispatching_every_record_once() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped(predicateStore, CONFIG);
        keepSeededVersions();
        fits = candidate -> appliedVersions(candidate) <= LIMIT;
        SagaEnvelope<Order> held = instance();
        SagaEnvelope<Order> untrimmedStart = Objects.requireNonNull(SagaExecutionSupport.startRelease(held, now.get()));
        SagaEnvelope<Order> trimmedStart = Objects.requireNonNull(SagaExecutionSupport.startReleaseTrimmed(held, now.get(), CONFIG.reorderWindow()));
        assertThat(fits.test(untrimmedStart)).as("control: the untrimmed start does not fit").isFalse();
        assertThat(fits.test(trimmedStart)).as("control: the trimmed start fits").isTrue();

        AtomicReference<ReleaseResult> result = new AtomicReference<>();
        Throwable thrown = catchThrowable(() -> result.set(execution.release(ORDER)));

        assertAll(
                () -> assertThat(thrown).as("the release does not throw").isNull(),
                () -> assertThat(result.get()).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(dispatched).as("each record once").containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b")),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(errors()).as("no ERROR").isEmpty()
        );
    }

    @Test
    void a_release_whose_start_does_not_fit_even_trimmed_throws_naming_the_instance_dispatches_nothing_and_leaves_the_instance_as_it_was() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped(predicateStore, CONFIG);
        fits = candidate -> !candidate.isReleasing();
        SagaEnvelope<Order> before = instance();

        Throwable thrown = catchThrowable(() -> execution.release(ORDER));

        assertAll(
                () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + ORDER + "'").hasMessageContaining("cannot be released"),
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(instance()).as("the stored instance is unchanged").isEqualTo(before)
        );
    }

    @Test
    void a_release_whose_next_write_does_not_fit_stops_before_dispatching_logs_that_a_further_release_goes_on_and_the_further_release_applies_the_record_once() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped(predicateStore, CONFIG);
        long held = seedVersionsAndLimitStoreToThem();

        ReleaseResult stopped = execution.release(ORDER);

        SagaEnvelope<Order> afterTheStop = instance();
        List<String> errorsAfterTheStop = errors();
        List<Object> dispatchedAtTheStop = List.copyOf(dispatched);
        List<SkippedInput> recordsAtTheStop = inMemory.findSkippedInputs(ORDER, afterTheStop.quarantineId(), Integer.MAX_VALUE);
        ReleaseResult further = execution.release(ORDER);

        assertAll(
                () -> assertThat(stopped).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(dispatchedAtTheStop).as("nothing is dispatched for the record").isEmpty(),
                () -> assertThat(afterTheStop.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(afterTheStop.isReleasing()).isFalse(),
                () -> assertThat(appliedVersions(afterTheStop)).as("the marks are trimmed").isLessThan(held),
                () -> assertThat(size(afterTheStop)).as("the stored instance fits").isLessThanOrEqualTo(held),
                () -> assertThat(recordsAtTheStop).as("every record stays recorded").hasSize(3),
                () -> assertThat(errorsAfterTheStop).as("one ERROR, that a further release goes on").singleElement().asString().contains(RELEASE_AGAIN).doesNotContain(STOPS_AGAIN),
                () -> assertThat(further).as("the further release gets past the record").isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(dispatched).as("each record once, overall").containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b")),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(instance().state().items()).containsExactly("poison", "a", "b")
        );
    }

    @Test
    void an_input_the_store_cannot_write_even_with_the_marks_trimmed_logs_that_a_further_release_stops_too_stays_quarantined_and_dispatches_nothing_on_every_release() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped(predicateStore, CONFIG);
        fits = candidate -> !appliedItems(candidate, "poison");

        ReleaseResult first = execution.release(ORDER);
        List<String> errorsAfterTheFirst = errors();
        ReleaseResult second = execution.release(ORDER);

        SagaEnvelope<Order> after = instance();
        assertAll(
                () -> assertThat(first).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(second).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(errorsAfterTheFirst).singleElement().asString().contains(STOPS_AGAIN).doesNotContain(RELEASE_AGAIN),
                () -> assertThat(errors()).as("the further release logs it again").hasSize(2).allSatisfy(message -> assertThat(message).contains(STOPS_AGAIN)),
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.isReleasing()).isFalse(),
                () -> assertThat(inMemory.findSkippedInputs(ORDER, after.quarantineId(), Integer.MAX_VALUE)).hasSize(3)
        );
    }

    @Test
    void the_candidate_the_stop_expects_a_further_release_to_write_is_the_candidate_the_further_release_asks_the_store_about() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped(predicateStore, CONFIG);
        seedVersionsAndLimitStoreToThem();
        asked.clear();
        execution.release(ORDER);
        List<StoredSaga> askedByTheStop = List.copyOf(asked);
        asked.clear();

        execution.release(ORDER);

        StoredSaga expected = askedByTheStop.getLast();
        StoredSaga actual = asked.get(1);
        assertAll(
                () -> assertThat(askedByTheStop).as("the stop asks four times, for the start, the record, the quarantine again and the further release").hasSize(4),
                () -> assertThat(withoutVersion(actual)).isEqualTo(withoutVersion(expected))
        );
    }

    @Test
    void a_store_that_keeps_the_default_answer_gets_no_guard_so_a_release_applies_and_dispatches_every_record_whatever_the_size() {
        InMemorySagaStateStore<Order> keepsTheDefault = inMemory;
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped(new DelegatingStore(keepsTheDefault), CONFIG);
        keepSeededVersions();

        ReleaseResult result = execution.release(ORDER);

        assertAll(
                () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(errors()).isEmpty(),
                () -> assertThat(dispatched).containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b")),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE)
        );
    }

    @Test
    void a_release_with_quarantine_turned_off_still_stops_on_a_write_the_store_cannot_take_and_dispatches_nothing() {
        quarantinedOnPoisonWithTwoSkipped(predicateStore, CONFIG);
        seedVersionsAndLimitStoreToThem();
        SagaExecution<OrderEvent, Order, Object> withoutQuarantine = execution(predicateStore, CONFIG_WITHOUT_QUARANTINE);

        ReleaseResult stopped = withoutQuarantine.release(ORDER);
        ReleaseResult further = withoutQuarantine.release(ORDER);

        assertAll(
                () -> assertThat(stopped).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(further).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(dispatched).containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b")),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE)
        );
    }

    @Test
    void the_timer_poller_that_goes_on_with_a_release_stops_it_on_a_write_the_store_cannot_take_and_a_later_release_goes_on_from_that_record() {
        SagaExecution<OrderEvent, Order, Object> execution = quarantinedOnPoisonWithTwoSkipped(predicateStore, CONFIG);
        keepSeededVersions();
        aFails = true;
        ReleaseResult stoppedOnA = execution.release(ORDER);
        aFails = false;
        fits = candidate -> appliedVersions(candidate) <= LIMIT;
        now.set(instance().updatedAt().plus(POLL_INTERVAL));
        dispatched.clear();

        execution.pollTimers();

        SagaEnvelope<Order> afterThePoll = instance();
        List<Object> dispatchedByThePoll = List.copyOf(dispatched);
        ReleaseResult further = execution.release(ORDER);

        assertAll(
                () -> assertThat(stoppedOnA).as("control: the first release stopped on 'a'").isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(dispatchedByThePoll).as("the poll dispatched nothing").isEmpty(),
                () -> assertThat(afterThePoll.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(afterThePoll.isReleasing()).isFalse(),
                () -> assertThat(errors()).singleElement().asString().contains(RELEASE_AGAIN),
                () -> assertThat(further).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(dispatched).containsExactly(new Pack(ORDER, "a"), new Pack(ORDER, "b")),
                () -> assertThat(instance().state().items()).containsExactly("poison", "a", "b")
        );
    }

    // Answers fits(..) as its delegate does, which says true to every instance
    private class DelegatingStore implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
        private final InMemorySagaStateStore<Order> delegate;

        DelegatingStore(InMemorySagaStateStore<Order> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean fits(StoredSaga candidate) {
            return delegate.fits(candidate);
        }

        @Override
        public boolean fitsWithoutState(StoredSaga saga, long expectedVersion) {
            return delegate.fitsWithoutState(saga, expectedVersion);
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
            return delegate.compareAndSave(saga, expectedVersion);
        }

        @Override
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            return delegate.compareAndSaveWithoutState(saga, expectedVersion);
        }

        @Override
        public List<StoredSaga> findWithDueTimers(Instant now, int limit) {
            return delegate.findWithDueTimers(now, limit);
        }

        @Override
        public List<String> findReleasingSagaIds(@Nullable String after, int limit) {
            return delegate.findReleasingSagaIds(after, limit);
        }

        @Override
        public boolean compareAndSaveSkipped(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput input) {
            return delegate.compareAndSaveSkipped(saga, expectedVersion, quarantineId, input);
        }

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            return delegate.compareAndSaveReplayed(saga, expectedVersion, quarantineId, replayed);
        }

        @Override
        public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
            return delegate.compareAndSaveQuarantineEnded(saga, expectedVersion, quarantineId);
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
        public Optional<StoredSagaWithFirstSkippedInputKey> findWithFirstSkippedInputKey(String sagaId) {
            return delegate.findWithFirstSkippedInputKey(sagaId);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }
    }

    // Answers fits(..) with the test's predicate over the decoded candidate, and remembers what it was asked
    private class PredicateStore extends DelegatingStore {
        PredicateStore() {
            super(inMemory);
        }

        @Override
        public boolean fits(StoredSaga candidate) {
            asked.add(candidate);
            return fits.test(codec.decode(candidate));
        }

        // The stored instance with the fields the write sets replaced, as the release asked before the store was asked to do it
        @Override
        public boolean fitsWithoutState(StoredSaga saga, long expectedVersion) {
            StoredSaga stored = inMemory.find(saga.sagaId()).orElse(null);
            if (stored == null || stored.version() != expectedVersion) {
                return true;
            }
            return fits(SagaEnvelopes.withFieldsExceptStateOf(stored, saga));
        }
    }
}
