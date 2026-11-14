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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaEnvelope.TimerEntry;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import io.cloudevents.CloudEvent;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * What the executor writes to an instance when a fire of one of its timers throws, so the next poll ranks the instance
 * behind the ones that have been due for longer. Every test drives {@code pollTimers} over a real in-memory store.
 */
@DisplayName("The failed-attempt marker on a saga timer")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaTimerFailedAttemptMarkerTest {

    private static final String TIMER = "expiry";
    private static final String OTHER_TIMER = "reminder";
    private static final long FIRES_AT = 1_000;
    private static final long OTHER_FIRES_AT = 10_000_000;
    private static final Instant POLL = Instant.ofEpochMilli(5_000);
    private static final long MARKED_UNTIL = POLL.plusMillis(1).toEpochMilli();
    private static final Instant CREATED_AND_UPDATED = Instant.ofEpochMilli(1);
    private static final int MAX_CAS_ATTEMPTS = 3;

    private final CloudEventConverter<OrderEvent> converter =
            new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:occurrent:saga-timer-marker-test")).build();

    private final SagaStateStore<String> backing = SagaStateStore.inMemory();
    private final List<CancelOrder> dispatched = new ArrayList<>();
    private Consumer<String> onReaction = sagaId -> {
        throw new IllegalStateException("this instance always fails");
    };

    private final AtomicLong clockMillis = new AtomicLong(POLL.toEpochMilli());
    private Runnable onDispatch = () -> {
    };

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
        executionLog.setLevel(Level.WARN);
    }

    @AfterEach
    void stopCapturing() {
        executionLog.setLevel(originalLevel);
        executionLog.detachAppender(appender);
        appender.stop();
    }

    @Test
    void is_written_to_the_failed_timer_only_and_leaves_the_instances_other_timers_untouched() {
        seed("s1", List.of(new TimerEntry(TIMER, FIRES_AT), new TimerEntry(OTHER_TIMER, OTHER_FIRES_AT)));

        poll(new CountingStore(backing));

        assertThat(backing.find("s1").orElseThrow().timers())
                .containsExactly(new TimerEntry(TIMER, FIRES_AT, MARKED_UNTIL), new TimerEntry(OTHER_TIMER, OTHER_FIRES_AT));
    }

    @Test
    void keeps_the_version_and_leaves_updatedAt_and_the_state_alone() {
        seed("s1", List.of(new TimerEntry(TIMER, FIRES_AT)));
        SagaEnvelope<String> before = backing.find("s1").orElseThrow();

        poll(new CountingStore(backing));

        SagaEnvelope<String> after = backing.find("s1").orElseThrow();
        assertAll(
                () -> assertThat(after.version()).isEqualTo(before.version()),
                () -> assertThat(after.updatedAt()).isEqualTo(before.updatedAt()),
                () -> assertThat(after.state()).isEqualTo(before.state()),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.ACTIVE)
        );
    }

    @Test
    void makes_the_instance_due_again_just_after_the_poll_instant() {
        seed("s1", List.of(new TimerEntry(TIMER, FIRES_AT)));

        poll(new CountingStore(backing));

        assertAll(
                () -> assertThat(backing.findWithDueTimers(POLL, 10)).isEmpty(),
                () -> assertThat(backing.findWithDueTimers(POLL.plusMillis(1), 10)).extracting(SagaEnvelope::sagaId).containsExactly("s1")
        );
    }

    @Test
    void is_retried_against_a_re_read_when_the_first_writes_lose_to_a_concurrent_write() {
        seed("s1", List.of(new TimerEntry(TIMER, FIRES_AT)));
        CountingStore store = new CountingStore(backing).losingTheFirst(2);

        poll(store);

        SagaEnvelope<String> after = backing.find("s1").orElseThrow();
        assertAll(
                () -> assertThat(store.markerWrites.get()).as("write attempts").isEqualTo(3),
                () -> assertThat(after.timers()).containsExactly(new TimerEntry(TIMER, FIRES_AT, MARKED_UNTIL)),
                () -> assertThat(after.version()).as("initial 1, two concurrent bumps, the marker keeps the version it read").isEqualTo(3),
                () -> assertThat(markerWarnings()).isEmpty()
        );
    }

    @Test
    void is_attempted_exactly_maxCasAttempts_times_and_warned_about_when_every_write_loses() {
        seed("s1", List.of(new TimerEntry(TIMER, FIRES_AT)));
        CountingStore store = new CountingStore(backing).losingTheFirst(Integer.MAX_VALUE);

        poll(store);

        assertAll(
                () -> assertThat(store.markerWrites.get()).as("write attempts").isEqualTo(MAX_CAS_ATTEMPTS),
                () -> assertThat(backing.find("s1").orElseThrow().timers()).as("no marker was written").containsExactly(new TimerEntry(TIMER, FIRES_AT)),
                () -> assertThat(markerWarnings()).hasSize(1),
                () -> assertThat(markerWarnings().get(0)).contains("s1", "'expiry'", "3 attempts", "kept changing").doesNotContain("refuses")
        );
    }

    @Test
    void is_not_written_when_the_timer_was_cancelled_while_it_was_firing() {
        seed("s1", List.of(new TimerEntry(TIMER, FIRES_AT)));
        onReaction = sagaId -> {
            replaceTimers(sagaId, List.of());
            throw new IllegalStateException("fails after a concurrent cancel");
        };
        CountingStore store = new CountingStore(backing);

        poll(store);

        assertAll(
                () -> assertThat(store.markerWrites.get()).isZero(),
                () -> assertThat(backing.find("s1").orElseThrow().timers()).isEmpty(),
                () -> assertThat(markerWarnings()).isEmpty()
        );
    }

    @Test
    void is_not_written_when_the_timer_was_re_armed_while_it_was_firing() {
        seed("s1", List.of(new TimerEntry(TIMER, FIRES_AT)));
        TimerEntry reArmed = new TimerEntry(TIMER, 9_000);
        onReaction = sagaId -> {
            replaceTimers(sagaId, List.of(reArmed));
            throw new IllegalStateException("fails after a concurrent re-arm");
        };
        CountingStore store = new CountingStore(backing);

        poll(store);

        assertAll(
                () -> assertThat(store.markerWrites.get()).isZero(),
                () -> assertThat(backing.find("s1").orElseThrow().timers()).containsExactly(reArmed)
        );
    }

    @Test
    void is_not_written_when_the_instance_completed_while_the_timer_was_firing() {
        seed("s1", List.of(new TimerEntry(TIMER, FIRES_AT)));
        onReaction = sagaId -> {
            SagaEnvelope<String> current = backing.find(sagaId).orElseThrow();
            backing.compareAndSave(sagaId, new SagaEnvelope<>(sagaId, "cancelled", SagaStatus.COMPLETED, current.version() + 1,
                    current.timers(), Map.of(), null, CREATED_AND_UPDATED, CREATED_AND_UPDATED, CREATED_AND_UPDATED, null), current.version());
            throw new IllegalStateException("fails after a concurrent completion");
        };
        CountingStore store = new CountingStore(backing);

        poll(store);

        assertAll(
                () -> assertThat(store.markerWrites.get()).isZero(),
                () -> assertThat(backing.find("s1").orElseThrow().timers()).containsExactly(new TimerEntry(TIMER, FIRES_AT))
        );
    }

    @Test
    void does_not_stop_the_poll_for_the_other_instances_when_the_store_fails_during_the_write() {
        seed("a-unlucky", List.of(new TimerEntry(TIMER, FIRES_AT)));
        seed("b-failing", List.of(new TimerEntry(TIMER, FIRES_AT)));
        seed("c-healthy", List.of(new TimerEntry(TIMER, FIRES_AT)));
        onReaction = sagaId -> {
            if (!sagaId.equals("c-healthy")) {
                throw new IllegalStateException("fails");
            }
        };
        CountingStore store = new CountingStore(backing).throwingOnWriteFor("a-unlucky");

        poll(store);

        assertAll(
                () -> assertThat(dispatched).as("the healthy instance behind it still fired").containsExactly(new CancelOrder("c-healthy")),
                () -> assertThat(backing.find("b-failing").orElseThrow().timers())
                        .as("the next failing instance was still marked").containsExactly(new TimerEntry(TIMER, FIRES_AT, MARKED_UNTIL)),
                () -> assertThat(backing.find("a-unlucky").orElseThrow().timers()).containsExactly(new TimerEntry(TIMER, FIRES_AT)),
                () -> assertThat(markerWarnings()).hasSize(1),
                () -> assertThat(markerWarnings().get(0)).contains("a-unlucky", "state store failed")
        );
    }

    @Test
    void a_timer_that_failed_once_fires_on_a_later_poll_once_its_reaction_succeeds_and_is_then_consumed() {
        seed("s1", List.of(new TimerEntry(TIMER, FIRES_AT)));
        AtomicInteger calls = new AtomicInteger();
        onReaction = sagaId -> {
            if (calls.getAndIncrement() == 0) {
                throw new IllegalStateException("fails once");
            }
        };
        CountingStore store = new CountingStore(backing);

        execution(store, () -> POLL).pollTimers();
        execution(store, () -> POLL.plusMillis(1)).pollTimers();

        SagaEnvelope<String> after = backing.find("s1").orElseThrow();
        assertAll(
                () -> assertThat(dispatched).containsExactly(new CancelOrder("s1")),
                () -> assertThat(after.timers()).isEmpty()
        );
    }

    @Test
    void a_failed_timer_still_waiting_out_its_marker_does_not_fire_when_another_timer_on_the_instance_brings_it_into_the_batch() {
        TimerEntry waitingOutItsMarker = new TimerEntry(TIMER, FIRES_AT, MARKED_UNTIL);
        seed("s1", List.of(waitingOutItsMarker, new TimerEntry(OTHER_TIMER, POLL.toEpochMilli())));
        onReaction = sagaId -> {
        };

        poll(new CountingStore(backing));

        assertAll(
                () -> assertThat(dispatched).as("only the timer that is due fired").containsExactly(new CancelOrder("s1/reminder")),
                () -> assertThat(backing.find("s1").orElseThrow().timers()).containsExactly(waitingOutItsMarker)
        );
    }

    @Test
    void a_timer_that_was_marked_by_a_concurrent_poll_after_this_poll_read_the_batch_is_not_fired() {
        seed("s1", List.of(new TimerEntry(TIMER, FIRES_AT)));
        TimerEntry markedMeanwhile = new TimerEntry(TIMER, FIRES_AT, MARKED_UNTIL);
        onReaction = sagaId -> {
        };
        CountingStore store = new CountingStore(backing).afterEachDueTimerRead(() -> replaceTimers("s1", List.of(markedMeanwhile)));

        poll(store);

        assertAll(
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(backing.find("s1").orElseThrow().timers()).containsExactly(markedMeanwhile)
        );
    }

    @Test
    void does_not_make_a_slow_event_delivery_lose_its_save_however_many_times_a_poll_marks_the_instance_while_it_runs() {
        seed("s1", List.of(new TimerEntry(TIMER, FIRES_AT)));
        SagaExecution<OrderEvent, String, CancelOrder> execution = execution(new CountingStore(backing), () -> Instant.ofEpochMilli(clockMillis.get()));
        // The dispatch sits between the event's read and its save, and each poll in it writes a fresh marker
        onDispatch = () -> {
            clockMillis.addAndGet(10);
            execution.pollTimers();
        };

        assertThatCode(() -> execution.onCloudEvent(streamKeyed(1, new OrderNoted("s1")))).doesNotThrowAnyException();

        SagaEnvelope<String> after = backing.find("s1").orElseThrow();
        assertAll(
                () -> assertThat(dispatched).as("the event's command, once").containsExactly(new CancelOrder("noted-s1")),
                () -> assertThat(after.state()).isEqualTo("noted"),
                () -> assertThat(after.version()).as("the event's save, on top of the version it read").isEqualTo(2),
                () -> assertThat(after.timers()).as("saved with the timers it read, which drops the marker").containsExactly(new TimerEntry(TIMER, FIRES_AT))
        );
    }

    @Test
    void leaves_the_poll_working_and_the_failing_timer_unmarked_when_the_store_refuses_a_save_at_the_same_version() {
        seed("a-failing", List.of(new TimerEntry(TIMER, FIRES_AT)));
        seed("b-healthy", List.of(new TimerEntry(TIMER, FIRES_AT)));
        AtomicInteger failures = new AtomicInteger();
        onReaction = sagaId -> {
            if (sagaId.equals("a-failing")) {
                failures.incrementAndGet();
                throw new IllegalStateException("always fails");
            }
        };
        CountingStore store = new CountingStore(backing).refusingASaveAtTheSameVersion();

        assertThatCode(() -> {
            execution(store, () -> POLL).pollTimers();
            execution(store, () -> POLL.plusMillis(1)).pollTimers();
        }).doesNotThrowAnyException();

        assertAll(
                () -> assertThat(dispatched).as("the healthy instance fired").containsExactly(new CancelOrder("b-healthy")),
                () -> assertThat(backing.find("a-failing").orElseThrow().timers()).as("never marked").containsExactly(new TimerEntry(TIMER, FIRES_AT)),
                () -> assertThat(failures.get()).as("tried again by the later poll").isEqualTo(2),
                () -> assertThat(markerWarnings()).as("one per poll, none fatal").hasSize(2),
                () -> assertThat(store.markerWrites.get()).as("one refused write per failed fire, not maxCasAttempts").isEqualTo(2),
                () -> assertThat(markerWarnings()).as("names the refusing store").allSatisfy(message -> assertThat(message)
                        .contains("a-failing", "refuses a save that keeps the version").doesNotContain("kept changing"))
        );
    }

    private void poll(CountingStore store) {
        execution(store, () -> POLL).pollTimers();
    }

    private void seed(String sagaId, List<TimerEntry> timers) {
        backing.compareAndSave(sagaId, new SagaEnvelope<>(sagaId, "waiting", SagaStatus.ACTIVE, 1, timers,
                Map.of(), null, CREATED_AND_UPDATED, CREATED_AND_UPDATED, null, null), 0);
    }

    private void replaceTimers(String sagaId, List<TimerEntry> timers) {
        SagaEnvelope<String> current = backing.find(sagaId).orElseThrow();
        backing.compareAndSave(sagaId, new SagaEnvelope<>(sagaId, current.state(), current.status(), current.version() + 1, timers,
                Map.of(), null, current.createdAt(), current.updatedAt(), null, null), current.version());
    }

    private CloudEvent streamKeyed(long streamVersion, OrderEvent event) {
        return CloudEventBuilder.v1(converter.toCloudEvent(event)).withExtension(OccurrentCloudEventExtension.occurrent("stream-" + event.orderId(), streamVersion)).build();
    }

    private List<String> warnings() {
        return appender.list.stream().filter(event -> event.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage).toList();
    }

    private List<String> markerWarnings() {
        return warnings().stream().filter(message -> message.contains("could not record that its timer")).toList();
    }

    private SagaExecution<OrderEvent, String, CancelOrder> execution(SagaStateStore<String> store, Supplier<Instant> clock) {
        Saga<OrderEvent, String, CancelOrder> saga = Saga.<OrderEvent, String, CancelOrder>builder("waiting")
                .correlate(OrderPlaced.class, OrderPlaced::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, event) -> "waiting")
                .correlate(OrderNoted.class, OrderNoted::orderId)
                .evolve(OrderNoted.class, (state, event) -> "noted")
                .react(OrderNoted.class, (state, event) -> List.of(SagaEffect.issue(new CancelOrder("noted-" + event.orderId()))))
                .evolveOnTimeout(TIMER, (state, timeout) -> "cancelled")
                .reactOnTimeout(TIMER, (state, timeout) -> {
                    onReaction.accept(timeout.sagaId());
                    return List.of(SagaEffect.issue(new CancelOrder(timeout.sagaId())));
                })
                .evolveOnTimeout(OTHER_TIMER, (state, timeout) -> "reminded")
                .reactOnTimeout(OTHER_TIMER, (state, timeout) -> List.of(SagaEffect.issue(new CancelOrder(timeout.sagaId() + "/reminder"))))
                .build();
        SagaRunnerConfig config = new SagaRunnerConfig(Duration.ofSeconds(15), 10, MAX_CAS_ATTEMPTS);
        return new SagaExecution<>("saga-timer-marker", saga, store, command -> {
            onDispatch.run();
            dispatched.add(command);
        }, converter, config, event -> true, clock);
    }

    /** Delegates to a real store and counts the without-state writes, which only the marker write uses. Can lose or fail them on demand. */
    private static final class CountingStore implements SagaStateStore<String> {
        private final SagaStateStore<String> delegate;
        final AtomicInteger markerWrites = new AtomicInteger();
        private int toLose;
        private @Nullable String failingFor;
        private @Nullable Runnable afterDueTimerRead;
        private boolean refusingSameVersion;

        CountingStore(SagaStateStore<String> delegate) {
            this.delegate = delegate;
        }

        CountingStore losingTheFirst(int writes) {
            this.toLose = writes;
            return this;
        }

        CountingStore throwingOnWriteFor(String sagaId) {
            this.failingFor = sagaId;
            return this;
        }

        CountingStore refusingASaveAtTheSameVersion() {
            this.refusingSameVersion = true;
            return this;
        }

        CountingStore afterEachDueTimerRead(Runnable action) {
            this.afterDueTimerRead = action;
            return this;
        }

        @Override
        public Optional<SagaEnvelope<String>> find(String sagaId) {
            return delegate.find(sagaId);
        }

        @Override
        public boolean compareAndSave(String sagaId, SagaEnvelope<String> envelope, long expectedVersion) {
            return delegate.compareAndSave(sagaId, envelope, expectedVersion);
        }

        @Override
        public boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<String> envelope, long expectedVersion) {
            markerWrites.incrementAndGet();
            if (sagaId.equals(failingFor)) {
                throw new IllegalStateException("the store is down");
            }
            if (refusingSameVersion && envelope.version() == expectedVersion) {
                return false;
            }
            if (toLose > 0) {
                toLose--;
                SagaEnvelope<String> current = delegate.find(sagaId).orElseThrow();
                SagaEnvelope<String> bumped = new SagaEnvelope<>(sagaId, current.state(), current.status(), current.version() + 1, current.timers(),
                        current.streamWatermarks(), null, current.createdAt(), current.updatedAt(), current.completedAt(), current.currentStep());
                delegate.compareAndSave(sagaId, bumped, current.version());
                return false;
            }
            return delegate.compareAndSave(sagaId, envelope, expectedVersion);
        }

        @Override
        public List<SagaEnvelope<String>> findWithDueTimers(Instant now, int limit) {
            List<SagaEnvelope<String>> due = delegate.findWithDueTimers(now, limit);
            if (afterDueTimerRead != null) {
                afterDueTimerRead.run();
            }
            return due;
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }
    }

    sealed interface OrderEvent {
        String orderId();
    }

    record OrderPlaced(String orderId) implements OrderEvent {
    }

    record OrderNoted(String orderId) implements OrderEvent {
    }

    record CancelOrder(String orderId) {
    }
}
