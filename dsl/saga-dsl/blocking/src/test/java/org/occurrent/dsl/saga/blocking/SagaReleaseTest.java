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

import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
import org.occurrent.dsl.saga.codec.SagaCodec;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.*;
import org.occurrent.dsl.saga.SagaEnvelope.TypeMark;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
import org.occurrent.dsl.saga.internal.SkippedInputKey;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Releasing a quarantined saga instance, <a href="https://github.com/johanhaleby/occurrent/issues/1259">#1259</a>
 * and ADR 178. A quarantined instance records the key and the event of the inputs it skips, and a release replays the
 * input it was quarantined on and then those, from the events the records hold. Driven through {@link SagaExecution}
 * with a clock and a state store the test controls, so every interleaving is deterministic.
 */
@DisplayName("Releasing a quarantined saga instance")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaReleaseTest {

    private static final String ORDER = "o1";
    private static final String REMINDER = "reminder";
    private static final Duration BUDGET = Duration.ofMinutes(1);
    // Best effort, so an event with no stream key reaches the saga and is recorded by its CloudEvent id and source
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

    sealed interface OrderCommand permits Pack, Remind {
    }

    record Pack(String orderId, String item) implements OrderCommand {
    }

    record Remind(String orderId) implements OrderCommand {
    }

    record Order(String orderId, List<String> items) {
        Order with(String item) {
            List<String> added = new ArrayList<>(items);
            added.add(item);
            return new Order(orderId, List.copyOf(added));
        }
    }

    // The items, and "placed" for the start event, whose reaction throws for as long as they are in here
    private final Set<String> failing = ConcurrentHashMap.newKeySet();
    // Run once, just before a reaction throws for one of them, so another call can move the instance on in between
    private final AtomicReference<@Nullable Runnable> beforeTheNextFailure = new AtomicReference<>();

    private Saga<OrderEvent, Order, OrderCommand> saga() {
        return Saga.<OrderEvent, Order, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(e.orderId(), List.of()))
                .react(OrderPlaced.class, (state, e) -> {
                    if (failing.contains("placed")) {
                        throw new IllegalStateException("cannot place");
                    }
                    return List.of(SagaEffect.startTimeout(REMINDER, Duration.ofMinutes(10)));
                })
                .evolve(ItemAdded.class, (state, e) -> state.with(e.item()))
                .react(ItemAdded.class, (state, e) -> {
                    if (failing.contains(e.item())) {
                        Runnable hook = beforeTheNextFailure.getAndSet(null);
                        if (hook != null) {
                            hook.run();
                        }
                        throw new IllegalStateException("cannot pack " + e.item());
                    }
                    return List.of(SagaEffect.issue(new Pack(e.orderId(), e.item())));
                })
                .reactOnTimeout(REMINDER, (state, t) -> List.of(SagaEffect.issue(new Remind(t.sagaId()))))
                .isTerminal(state -> state.items().contains("done"))
                .build();
    }

    private final Instant start = Instant.parse("2026-01-01T00:00:00Z");
    private final AtomicReference<Instant> now = new AtomicReference<>(start);
    private final InMemoryEventStore eventStore = new InMemoryEventStore();
    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final InterceptingStore store = new InterceptingStore();
    private final List<OrderCommand> dispatched = new CopyOnWriteArrayList<>();
    private final AtomicInteger eventIds = new AtomicInteger();
    private SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(CONFIG);

    private SagaExecution<OrderEvent, Order, OrderCommand> execution(SagaRunnerConfig config) {
        return new SagaExecution<>("orders", saga(), store, dispatched::add, converter, config, now::get);
    }

    private CloudEvent append(OrderEvent event) {
        eventStore.write(ORDER, converter.toCloudEvents(List.of(event)));
        return eventStore.read(ORDER).eventList().getLast();
    }

    private CloudEvent appendOn(String streamId, OrderEvent event) {
        eventStore.write(streamId, converter.toCloudEvents(List.of(event)));
        return eventStore.read(streamId).eventList().getLast();
    }

    private CloudEvent placed() {
        return append(new OrderPlaced(nextEventId(), ORDER));
    }

    private CloudEvent added(String item) {
        return append(new ItemAdded(nextEventId(), ORDER, item));
    }

    private String nextEventId() {
        return "e" + eventIds.incrementAndGet();
    }

    // What a feed hands over for an event that has no stream extensions
    private static CloudEvent withoutStream(CloudEvent event) {
        return CloudEventBuilder.v1(event)
                .withoutExtension(OccurrentCloudEventExtension.STREAM_ID)
                .withoutExtension(OccurrentCloudEventExtension.STREAM_VERSION)
                .build();
    }

    private static String cloudEventKey(CloudEvent event) {
        return SkippedInputKey.cloudEventKey(event.getId(), event.getSource().toString());
    }

    private static String streamKey(CloudEvent event) {
        return event.getExtension(OccurrentCloudEventExtension.STREAM_ID) + "@" + event.getExtension(OccurrentCloudEventExtension.STREAM_VERSION);
    }

    // The poller leaves a releasing instance written within the last timerPollInterval to the release that wrote it, so a
    // poll that goes on with a release comes a poll interval after that release's last write
    private void pollAfter(SagaRunnerConfig config) {
        now.set(now.get().plus(config.timerPollInterval()).plusMillis(1));
        execution.pollTimers();
    }

    private void pollAfterTheInterval() {
        pollAfter(CONFIG);
    }

    private void deliver(CloudEvent event) {
        execution.onCloudEvent(event);
    }

    // Fails once, and once more past the budget, which quarantines
    private void quarantineOn(CloudEvent event) {
        assertThatThrownBy(() -> execution.onCloudEvent(event)).isInstanceOf(IllegalStateException.class);
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> execution.onCloudEvent(event));
        assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED);
    }

    private SagaEnvelope<Order> instance() {
        return store.envelopes.find(ORDER).orElseThrow();
    }

    // Records an input under a quarantine id in a write that moves the instance one version on, the only way a store takes a record
    private void recordUnder(String sagaId, String quarantineId, SkippedInput input) {
        SagaEnvelope<Order> current = instance();
        assertThat(store.envelopes.compareAndSaveSkipped(store.delegate, SagaExecutionSupport.withSkipCounted(current), current.version(), quarantineId, input)).isTrue();
    }

    private List<String> items() {
        return instance().state().items();
    }

    private List<SkippedInput> records() {
        return store.delegate.findSkippedInputs(ORDER, instance().quarantineId(), Integer.MAX_VALUE);
    }

    private List<String> recordedKeys() {
        return records().stream().map(SkippedInput::key).toList();
    }

    private List<String> recordedKeysOf(String quarantineId) {
        return store.delegate.findSkippedInputs(ORDER, quarantineId, Integer.MAX_VALUE).stream().map(SkippedInput::key).toList();
    }

    private List<Pack> packed() {
        return dispatched.stream().filter(Pack.class::isInstance).map(Pack.class::cast).toList();
    }

    /** Placed, quarantined on adding "poison", then "a" skipped as a CloudEvent-keyed input and "b" as a stream-keyed one. */
    private record Quarantined(CloudEvent k, CloudEvent a, CloudEvent b) {
    }

    private Quarantined quarantinedOnPoisonWithTwoSkipped() {
        deliver(placed());
        failing.add("poison");
        CloudEvent k = added("poison");
        quarantineOn(k);
        CloudEvent a = added("a");
        deliver(withoutStream(a));
        CloudEvent b = added("b");
        deliver(b);
        return new Quarantined(k, a, b);
    }

    /** As {@link #quarantinedOnPoisonWithTwoSkipped()}, with "a" skipped stream-keyed too. */
    private Quarantined quarantinedOnPoisonWithTwoStreamKeyedSkipped() {
        deliver(placed());
        failing.add("poison");
        CloudEvent k = added("poison");
        quarantineOn(k);
        CloudEvent a = added("a");
        deliver(a);
        CloudEvent b = added("b");
        deliver(b);
        return new Quarantined(k, a, b);
    }

    @Nested
    class WhenEveryInputCanBeApplied {

        @Test
        void the_quarantined_instance_records_the_input_it_was_quarantined_on_and_each_skipped_input_by_its_key_and_moves_no_watermark() {
            deliver(placed());
            failing.add("poison");
            CloudEvent k = added("poison");
            quarantineOn(k);
            Long watermarkWhenQuarantined = instance().streamWatermarks().get(ORDER);

            CloudEvent a = added("a");
            deliver(withoutStream(a));
            CloudEvent b = added("b");
            deliver(b);
            // Redeliveries of a recorded input and of the input it was quarantined on add nothing
            deliver(b);
            deliver(withoutStream(a));
            catchThrowable(() -> deliver(k));

            assertAll(
                    () -> assertThat(recordedKeys()).containsExactly(streamKey(k), cloudEventKey(a), streamKey(b)),
                    () -> assertThat(instance().streamWatermarks().get(ORDER)).isEqualTo(watermarkWhenQuarantined),
                    () -> assertThat(instance().failure().input()).isEqualTo(streamKey(k))
            );
        }

        @Test
        void applies_the_input_it_was_quarantined_on_and_then_every_skipped_input_in_order_and_makes_the_instance_active() {
            quarantinedOnPoisonWithTwoSkipped();
            failing.clear();

            ReleaseResult result = execution.release(ORDER);

            SagaEnvelope<Order> released = instance();
            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(released.status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(released.state().items()).containsExactly("poison", "a", "b"),
                    () -> assertThat(packed()).extracting(Pack::item).containsExactly("poison", "a", "b"),
                    () -> assertThat(released.failure()).isNull(),
                    () -> assertThat(released.quarantineSkipCount()).isNull(),
                    () -> assertThat(released.quarantineId()).isNull(),
                    () -> assertThat(released.isReleasing()).isFalse()
            );
        }

        @Test
        void an_instance_quarantined_on_its_start_event_records_it_and_what_it_skips_and_starts_when_it_is_released() {
            failing.add("placed");
            CloudEvent start = placed();
            quarantineOn(start);
            CloudEvent a = added("a");
            deliver(a);
            assertThat(recordedKeys()).containsExactly(streamKey(start), streamKey(a));
            failing.clear();

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(instance().started()).isTrue(),
                    () -> assertThat(items()).containsExactly("a")
            );
        }

        @Test
        void a_timer_that_came_due_while_quarantined_fires_only_once_the_instance_is_active() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            now.set(start.plus(Duration.ofHours(1)));
            // "a" keeps the release going without finishing it
            failing.clear();
            failing.add("a");
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);

            pollAfterTheInterval();
            boolean remindedWhileReleasing = dispatched.contains(new Remind(ORDER));
            failing.clear();
            // This poll goes on with the release, and the next fires what came due
            pollAfterTheInterval();
            pollAfterTheInterval();

            assertAll(
                    () -> assertThat(remindedWhileReleasing).isFalse(),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).containsExactly("poison", "a", "b"),
                    () -> assertThat(dispatched).containsOnlyOnce(new Remind(ORDER))
            );
        }
    }

    @Nested
    class WhenAnInputFailsAgainWhileReleasing {

        @Test
        void stays_releasing_inside_the_budget_of_the_input_it_was_quarantined_on_and_is_quarantined_on_it_again_once_the_budget_is_used_up() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoSkipped();
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);

            ReleaseResult first = execution.release(ORDER);
            boolean releasingAfterFirst = instance().isReleasing();
            now.set(releasedAt.plus(BUDGET).minusMillis(1));
            ReleaseResult insideTheBudget = execution.release(ORDER);
            now.set(releasedAt.plus(BUDGET));
            ReleaseResult pastTheBudget = execution.release(ORDER);

            SagaEnvelope<Order> requarantined = instance();
            assertAll(
                    () -> assertThat(first).isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(releasingAfterFirst).isTrue(),
                    () -> assertThat(insideTheBudget).isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(pastTheBudget).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(requarantined.status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(requarantined.failure().input()).isEqualTo(streamKey(quarantined.k())),
                    () -> assertThat(requarantined.failure().releasedAt()).isNull(),
                    () -> assertThat(requarantined.isReleasing()).isFalse(),
                    () -> assertThat(recordedKeys()).containsExactly(streamKey(quarantined.k()), cloudEventKey(quarantined.a()), streamKey(quarantined.b())),
                    () -> assertThat(requarantined.state().items()).isEmpty()
            );
        }

        @Test
        void writes_nothing_when_the_same_input_fails_again_inside_its_budget() {
            quarantinedOnPoisonWithTwoSkipped();
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
            SagaEnvelope<Order> afterTheFirstFailure = instance();

            now.set(releasedAt.plus(BUDGET.dividedBy(3)));
            ReleaseResult second = execution.release(ORDER);
            now.set(releasedAt.plus(BUDGET.dividedBy(3).multipliedBy(2)));
            ReleaseResult third = execution.release(ORDER);

            SagaEnvelope<Order> afterwards = instance();
            assertAll(
                    () -> assertThat(second).isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(third).isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(afterwards.version()).isEqualTo(afterTheFirstFailure.version()),
                    () -> assertThat(afterwards.updatedAt()).isEqualTo(afterTheFirstFailure.updatedAt())
            );
        }

        @Test
        void a_recorded_input_whose_budget_runs_out_becomes_the_failure_input_with_its_record_and_the_later_records_kept() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);
            failing.clear();
            failing.add("a");

            ReleaseResult first = execution.release(ORDER);
            now.set(releasedAt.plus(BUDGET));
            ReleaseResult pastTheBudget = execution.release(ORDER);

            SagaEnvelope<Order> requarantined = instance();
            assertAll(
                    () -> assertThat(first).isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(pastTheBudget).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(requarantined.status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(requarantined.failure().input()).isEqualTo(streamKey(quarantined.a())),
                    () -> assertThat(requarantined.failure().failureMessage()).isEqualTo("cannot pack a"),
                    () -> assertThat(requarantined.failure().releasedAt()).isNull(),
                    () -> assertThat(recordedKeys()).containsExactly(streamKey(quarantined.a()), streamKey(quarantined.b())),
                    () -> assertThat(requarantined.state().items()).containsExactly("poison")
            );
        }

        @Test
        void the_timer_poller_goes_on_with_the_release_once_the_input_gets_through() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            failing.add("a");
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
            failing.clear();

            pollAfterTheInterval();

            assertAll(
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).containsExactly("poison", "a", "b")
            );
        }
    }

    @Nested
    class WhenAnotherWriteLandsWhileReleasing {

        @Test
        void a_live_input_skipped_during_the_release_is_recorded_and_applied_and_only_the_write_that_finds_no_record_left_makes_the_instance_active() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            CloudEvent live = added("live");
            // Happens between the release reading the instance and writing the input it applies
            store.beforeNextStateWrite(() -> deliver(live));
            store.accepted.clear();

            ReleaseResult result = execution.release(ORDER);

            List<SagaEnvelope<Order>> activeWrites = store.accepted.stream().filter(e -> e.status() == SagaStatus.ACTIVE).toList();
            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(packed()).extracting(Pack::item).containsExactly("poison", "a", "b", "live"),
                    () -> assertThat(items()).containsExactly("poison", "a", "b", "live"),
                    () -> assertThat(activeWrites).hasSize(1),
                    () -> assertThat(activeWrites.getFirst().quarantineSkipCount()).isNull(),
                    () -> assertThat(store.accepted.getLast().status()).isEqualTo(SagaStatus.ACTIVE)
            );
        }

        @Test
        void two_releases_racing_on_the_same_instance_apply_each_entry_once() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            SagaExecution<OrderEvent, Order, OrderCommand> otherNode = execution(CONFIG);
            AtomicReference<ReleaseResult> otherResult = new AtomicReference<>();
            store.beforeNextStateWrite(() -> otherResult.set(otherNode.release(ORDER)));
            store.accepted.clear();

            ReleaseResult result = execution.release(ORDER);

            List<List<String>> appliedStates = store.accepted.stream().filter(e -> e.state() != null).map(e -> e.state().items()).toList();
            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(otherResult.get()).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).containsExactly("poison", "a", "b"),
                    () -> assertThat(appliedStates).as("one write per entry, and no entry applied twice").containsExactly(List.of("poison"), List.of("poison", "a"), List.of("poison", "a", "b")),
                    () -> assertThat(store.accepted.getLast().state()).as("the write that ends the quarantine comes last and carries no state").isNull(),
                    () -> assertThat(store.accepted.getLast().status()).isEqualTo(SagaStatus.ACTIVE)
            );
        }
    }

    @Nested
    class WhenTheRecordsToReplayCannotBeRead {

        @Test
        void a_read_of_the_records_that_throws_makes_the_release_throw_before_it_starts_and_a_second_release_goes_through() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoSkipped();
            failing.clear();
            List<String> recordsBefore = recordedKeys();
            IllegalStateException storeDown = new IllegalStateException("state store is down");
            store.findSkippedInputsFailure = storeDown;

            Throwable thrown = catchThrowable(() -> execution.release(ORDER));
            SagaEnvelope<Order> afterTheFailedRead = instance();
            List<String> recordsAfterTheFailedRead = recordedKeys();
            store.findSkippedInputsFailure = null;
            ReleaseResult second = execution.release(ORDER);

            assertAll(
                    () -> assertThat(thrown).isSameAs(storeDown),
                    () -> assertThat(afterTheFailedRead.status()).as("the instance is still quarantined").isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(afterTheFailedRead.isReleasing()).as("the release did not start, so the poller has nothing to go on with").isFalse(),
                    () -> assertThat(recordsAfterTheFailedRead).as("no record is touched").isEqualTo(recordsBefore),
                    () -> assertThat(recordsAfterTheFailedRead).first().isEqualTo(streamKey(quarantined.k())),
                    () -> assertThat(second).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).containsExactly("poison", "a", "b")
            );
        }
    }

    @Nested
    class WhenTheReleaseIsRefused {

        @Test
        void an_instance_quarantined_by_0_34_0_with_no_quarantine_id_is_refused_naming_delete_as_the_only_way_out() {
            SagaFailure failure = new SagaFailure("o1@2", 7L, start, IllegalStateException.class.getName(), "boom", null, null);
            store.envelopes.compareAndSave(ORDER, new SagaEnvelope<>(ORDER, new Order(ORDER, List.of()), SagaStatus.QUARANTINED, 1, List.of(),
                    Map.of(ORDER, 1L), null, start, start, null, null, true, failure, Map.of(), null), 0);

            Throwable thrown = catchThrowable(() -> execution.release(ORDER));

            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("quarantined by 0.34.0")
                            .hasMessageContaining("SagaStateStore.delete(\"" + ORDER + "\")")
                            .hasMessageNotContaining("restart"),
                    () -> assertThat(instance().isReleasing()).isFalse()
            );
        }

        @Test
        void an_instance_quarantined_by_0_35_0_whose_records_are_gone_is_refused_naming_the_input_it_stopped_on_and_is_left_as_it_was() {
            CloudEvent stopped = quarantinedWithoutItsRecords();
            SagaEnvelope<Order> before = instance();
            int writesBefore = store.writesWithoutState.get();

            Throwable thrown = catchThrowable(() -> execution.release(ORDER));

            SagaEnvelope<Order> after = instance();
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("'" + streamKey(stopped) + "'")
                            .hasMessageContaining("returned no record of the input")
                            .hasMessageContaining("findWithFirstSkippedInput"),
                    () -> assertThat(after.status()).as("still quarantined").isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(after.isReleasing()).as("the release did not start").isFalse(),
                    () -> assertThat(after.version()).as("nothing was written").isEqualTo(before.version()),
                    () -> assertThat(store.writesWithoutState.get()).as("no write that starts a release was attempted").isEqualTo(writesBefore),
                    () -> assertThat(packed()).as("nothing is applied").isEmpty()
            );
        }

        @Test
        void an_instance_quarantined_by_0_35_0_whose_first_record_is_of_another_input_is_refused_naming_the_input_it_stopped_on() {
            CloudEvent stopped = quarantinedWithoutItsRecords();
            recordUnder(ORDER, instance().quarantineId(), new SkippedInput(0, "o1@99", added("other")));
            SagaEnvelope<Order> before = instance();

            Throwable thrown = catchThrowable(() -> execution.release(ORDER));

            SagaEnvelope<Order> after = instance();
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessageContaining("'" + streamKey(stopped) + "'"),
                    () -> assertThat(after.isReleasing()).as("the release did not start").isFalse(),
                    () -> assertThat(after.version()).as("nothing was written").isEqualTo(before.version()),
                    () -> assertThat(recordedKeys()).as("the record is left").containsExactly("o1@99")
            );
        }

        // As an instance restored from a backup that has the instance and not the records of its quarantine. Returns the event it stopped on
        private CloudEvent quarantinedWithoutItsRecords() {
            deliver(placed());
            failing.add("poison");
            CloudEvent stopped = added("poison");
            quarantineOn(stopped);
            SagaEnvelope<Order> quarantined = instance();
            SkippedInput first = records().getFirst();
            assertThat(store.envelopes.compareAndSaveReplayed(store.delegate, SagaExecutionSupport.oneVersionUp(quarantined), quarantined.version(), quarantined.quarantineId(), first))
                    .as("control: the record is removed").isTrue();
            assertThat(records()).as("control: it has no record").isEmpty();
            return stopped;
        }
    }

    @Nested
    class WhenThereIsNothingToRelease {

        @Test
        void an_unknown_instance_is_not_found() {
            assertThat(execution.release("unknown")).isEqualTo(ReleaseResult.NOT_FOUND);
        }

        @Test
        void an_active_instance_is_not_quarantined_and_is_left_as_it_was() {
            deliver(placed());
            SagaEnvelope<Order> before = instance();

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.NOT_QUARANTINED),
                    () -> assertThat(instance()).isEqualTo(before)
            );
        }
    }

    @Nested
    class TheTimerPoller {

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
        void logs_that_it_could_not_list_the_instances_when_the_listing_fails_and_not_that_it_could_not_go_on_with_one() {
            store.listFailure = new IllegalStateException("the store is down");

            execution.pollTimers();

            List<String> warnings = warnings();
            assertAll(
                    () -> assertThat(warnings).anyMatch(m -> m.contains("could not list the instances it is releasing")),
                    () -> assertThat(warnings).noneMatch(m -> m.contains("could not go on with the release of instance"))
            );
        }

        @Test
        void logs_the_id_of_an_instance_it_could_not_go_on_with_and_goes_on_with_the_next_instance() {
            releasingOn("o1", "a1");
            releasingOn("o2", "b1");
            failing.clear();
            store.findFailsFor = "o1";

            pollAfterTheInterval();

            assertAll(
                    () -> assertThat(warnings()).anyMatch(m -> m.contains("could not go on with the release of instance 'o1'")),
                    () -> assertThat(warnings()).noneMatch(m -> m.contains("could not list the instances it is releasing")),
                    () -> assertThat(store.envelopes.find("o1").orElseThrow().state().items()).isEmpty(),
                    () -> assertThat(store.envelopes.find("o2").orElseThrow().state().items()).containsExactly("poison", "b1"),
                    () -> assertThat(store.envelopes.find("o2").orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE)
            );
        }

        @Test
        void leaves_the_instances_after_one_that_runs_out_of_memory_to_the_next_poll() {
            releasingOn("o1", "a1");
            releasingOn("o2", "b1");
            failing.clear();
            store.findRunsOutOfMemoryFor = "o1";

            pollAfterTheInterval();

            assertAll(
                    () -> assertThat(warnings()).anyMatch(m -> m.contains("could not go on with the release of instance 'o1'")),
                    () -> assertThat(store.envelopes.find("o2").orElseThrow().isReleasing()).isTrue(),
                    () -> assertThat(store.envelopes.find("o2").orElseThrow().state().items()).isEmpty()
            );
        }

        private List<String> warnings() {
            return appender.list.stream().filter(e -> e.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage).toList();
        }

        // Quarantined on "poison" with the items skipped after it, and releasing, because the release stopped on "poison" again
        private void releasingOn(String orderId, String... skipped) {
            deliver(appendOn(orderId, new OrderPlaced(nextEventId(), orderId)));
            failing.add("poison");
            CloudEvent poison = appendOn(orderId, new ItemAdded(nextEventId(), orderId, "poison"));
            assertThatThrownBy(() -> deliver(poison)).isInstanceOf(IllegalStateException.class);
            now.set(now.get().plus(BUDGET));
            catchThrowable(() -> deliver(poison));
            for (String item : skipped) {
                deliver(appendOn(orderId, new ItemAdded(nextEventId(), orderId, item)));
            }
            assertThat(execution.release(orderId)).isEqualTo(ReleaseResult.STILL_RELEASING);
        }
    }

    @Nested
    class WhenAReplayFailsAfterAnotherCallMovedTheReleaseOn {

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
        }

        private List<String> messages(Level level) {
            return appender.list.stream().filter(e -> e.getLevel() == level).map(ILoggingEvent::getFormattedMessage).toList();
        }

        @Test
        void the_release_is_released_when_the_other_call_finished_it() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            now.set(start.plus(Duration.ofHours(1)));
            SagaExecution<OrderEvent, Order, OrderCommand> otherNode = execution(CONFIG);
            AtomicReference<@Nullable ReleaseResult> otherResult = new AtomicReference<>();
            beforeTheNextFailure.set(() -> {
                failing.clear();
                otherResult.set(otherNode.release(ORDER));
            });

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(otherResult).hasValue(ReleaseResult.RELEASED),
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).containsExactly("poison", "a", "b"),
                    () -> assertThat(messages(Level.INFO)).anyMatch(m -> m.contains("moved the release on and it is RELEASED now")),
                    () -> assertThat(messages(Level.WARN)).noneMatch(m -> m.contains("It stays releasing"))
            );
        }

        @Test
        void the_release_is_quarantined_again_when_the_other_call_quarantined_it_again() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
            now.set(releasedAt.plus(BUDGET));
            SagaExecution<OrderEvent, Order, OrderCommand> otherNode = execution(CONFIG);
            AtomicReference<@Nullable ReleaseResult> otherResult = new AtomicReference<>();
            beforeTheNextFailure.set(() -> otherResult.set(otherNode.release(ORDER)));

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(otherResult).hasValue(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(instance().isReleasing()).isFalse(),
                    () -> assertThat(messages(Level.INFO)).anyMatch(m -> m.contains("moved the release on and it is QUARANTINED_AGAIN now"))
            );
        }

        @Test
        void the_release_is_still_releasing_when_no_other_call_moved_it_on() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            now.set(start.plus(Duration.ofHours(1)));

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(instance().isReleasing()).isTrue(),
                    () -> assertThat(messages(Level.INFO)).noneMatch(m -> m.contains("moved the release on"))
            );
        }
    }

    @Nested
    class WhenAnotherCallWritesBetweenTheStartOfTheReleaseAndItsReadBack {

        @Test
        void a_release_started_and_quarantined_again_by_another_call_is_a_result_and_not_a_store_defect() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);
            SagaExecution<OrderEvent, Order, OrderCommand> otherNode = execution(CONFIG);
            store.beforeNextFindWithoutState(() -> {
                now.set(releasedAt.plus(BUDGET));
                assertThat(otherNode.release(ORDER)).isEqualTo(ReleaseResult.QUARANTINED_AGAIN);
            });

            AtomicReference<@Nullable ReleaseResult> result = new AtomicReference<>();
            Throwable thrown = catchThrowable(() -> result.set(execution.release(ORDER)));

            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(result).hasValue(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(instance().isReleasing()).isFalse()
            );
        }
    }

    @Nested
    class AnInstanceThatFailedOnAnInputBefore035BelowTheWatermarkOfItsStream {

        // As 0.34.0 left it: "poison" failed and its failure record stays while the next input of the stream applied, so
        // the watermark is past it and the instance holds no stream marks
        private CloudEvent seedTheInstanceAs034LeftIt() {
            placed();
            CloudEvent poison = added("poison");
            added("next");
            SagaFailure legacyFailure = new SagaFailure(streamKey(poison), 2L, start, IllegalStateException.class.getName(), "cannot pack poison", null, null);
            SagaEnvelope<Order> legacy = new SagaEnvelope<>(ORDER, new Order(ORDER, List.of("next")), SagaStatus.ACTIVE, 1, List.of(),
                    Map.of(ORDER, 3L), null, start, start, null, null, true, legacyFailure, Map.of(), null);
            assertThat(store.envelopes.compareAndSave(ORDER, legacy, 0)).isTrue();
            return poison;
        }

        @Test
        void is_applied_once_on_the_input_it_failed_on_when_the_release_crashes_before_the_write_that_applies_it_and_a_second_release_runs() {
            CloudEvent poison = seedTheInstanceAs034LeftIt();
            failing.add("poison");
            quarantineOn(poison);
            failing.clear();
            store.beforeNextDeleteOfARecord(() -> {
                throw new OutOfMemoryError("crash before the write that applies the record");
            });
            assertThat(catchThrowable(() -> execution.release(ORDER))).isInstanceOf(OutOfMemoryError.class).hasMessageContaining("crash before the write");

            ReleaseResult second = execution.release(ORDER);

            assertAll(
                    () -> assertThat(second).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).as("the items of the instance").containsExactlyInAnyOrder("next", "poison"),
                    () -> assertThat(packed()).as("commands are dispatched before the write, so the release that crashed before it dispatched them and the second release dispatches them again").containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "poison"))
            );
        }
    }

    @Nested
    class AnInputBehindTheReorderWindowOfItsTypeMark {

        @Test
        void is_applied_once_when_the_release_crashes_before_the_write_that_applies_it_and_a_second_release_runs() {
            execution = execution(CONFIG.withReorderWindow(2));
            deliver(placed());
            failing.add("poison");
            CloudEvent poison = added("poison");
            assertThat(catchThrowable(() -> execution.onCloudEvent(poison))).isInstanceOf(IllegalStateException.class);
            for (String item : List.of("i3", "i4", "i5", "i6")) {
                deliver(added(item));
            }
            now.set(now.get().plus(BUDGET));
            catchThrowable(() -> execution.onCloudEvent(poison));
            assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED);
            failing.clear();
            store.beforeNextDeleteOfARecord(() -> {
                throw new OutOfMemoryError("crash before the write that applies the record");
            });
            assertThat(catchThrowable(() -> execution.release(ORDER))).isInstanceOf(OutOfMemoryError.class).hasMessageContaining("crash before the write");

            ReleaseResult second = execution.release(ORDER);

            assertAll(
                    () -> assertThat(second).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).as("the items of the instance").containsExactlyInAnyOrder("i3", "i4", "i5", "i6", "poison"),
                    () -> assertThat(packed()).as("the commands dispatched for the input it failed on").as("commands are dispatched before the write, so the release that crashed before it dispatched the input and the second release dispatches it again").extracting(Pack::item).filteredOn("poison"::equals).hasSize(2)
            );
        }

        @Test
        void leaves_the_released_instance_with_no_more_applied_versions_of_a_type_than_the_window() {
            execution = execution(CONFIG.withReorderWindow(1));
            deliver(placed());
            failing.add("poison");
            quarantineOn(added("poison"));
            for (String item : List.of("i3", "i4", "i5", "i6")) {
                deliver(added(item));
            }
            failing.clear();

            ReleaseResult result = execution.release(ORDER);

            List<TypeMark> typeMarks = instance().streamMarks().get(ORDER).byType().values().stream().toList();
            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).as("the items of the instance").containsExactlyInAnyOrder("poison", "i3", "i4", "i5", "i6"),
                    () -> assertThat(typeMarks).as("the type marks of the stream").isNotEmpty().allSatisfy(typeMark -> assertThat(typeMark.applied()).hasSizeLessThanOrEqualTo(1))
            );
        }

        @Test
        void is_applied_by_the_release_when_an_earlier_record_raised_the_mark_past_it_and_is_not_deleted_unapplied() {
            execution = execution(CONFIG.withReorderWindow(1));
            deliver(placed());
            failing.add("poison");
            quarantineOn(added("poison"));
            String quarantineId = instance().quarantineId();
            CloudEvent behind = added("v3");
            added("never delivered");
            CloudEvent ahead = added("v5");
            deliver(ahead);
            deliver(behind);
            failing.clear();

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).as("the items of the instance").containsExactlyInAnyOrder("poison", "v5", "v3"),
                    () -> assertThat(packed()).as("the commands dispatched").extracting(Pack::item).containsExactlyInAnyOrder("poison", "v5", "v3"),
                    () -> assertThat(recordedKeysOf(quarantineId)).as("the records left").isEmpty(),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE)
            );
        }

        @Test
        void has_its_failure_recorded_by_the_release_and_is_applied_by_the_next_one() {
            execution = execution(CONFIG.withReorderWindow(1));
            deliver(placed());
            failing.add("poison");
            quarantineOn(added("poison"));
            CloudEvent behind = added("v3");
            added("never delivered");
            CloudEvent ahead = added("v5");
            deliver(ahead);
            deliver(behind);
            failing.clear();
            failing.add("v3");

            ReleaseResult first = execution.release(ORDER);
            SagaFailure recorded = instance().failure();
            failing.clear();
            ReleaseResult second = execution.release(ORDER);

            assertAll(
                    () -> assertThat(first).isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(recorded).isNotNull(),
                    () -> assertThat(recorded.input()).as("the input the failure is recorded on").isEqualTo(streamKey(behind)),
                    () -> assertThat(second).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).as("the items of the instance").containsExactlyInAnyOrder("poison", "v5", "v3")
            );
        }

        @Test
        void a_cloud_event_keyed_record_of_an_input_a_live_delivery_applied_before_the_quarantine_is_applied_again_under_best_effort() {
            // Under BEST_EFFORT a redelivery runs every reaction again, see ADR 109, and a release applies such a record again too
            execution = execution(CONFIG.withReorderWindow(1));
            deliver(placed());
            CloudEvent early = added("early");
            deliver(early);
            deliver(added("i3"));
            deliver(added("i4"));
            failing.add("poison");
            quarantineOn(added("poison"));
            deliver(withoutStream(early));
            List<String> recorded = recordedKeys();
            failing.clear();

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(recorded).as("the records before the release").contains(cloudEventKey(early)),
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).as("the items of the instance").containsExactly("early", "i3", "i4", "poison", "early"),
                    () -> assertThat(packed()).as("the commands dispatched").extracting(Pack::item).filteredOn("early"::equals).hasSize(2)
            );
        }
    }

    @Nested
    class AReleaseThatEndsAfterReplayingMoreVersionsOfATypeThanTheWindow {

        private static final int WINDOW = 1;
        private final Map<String, CloudEvent> events = new java.util.LinkedHashMap<>();

        // Placed at version 1, quarantined on "poison" at version 2, then each item appended in order from version 3 on and
        // skipped in the order given. Skipping i6 and i7 before i5, i3 and i4 makes the release raise the mark of the item
        // type to 7 before the later records, so a write that trims the marks puts those records behind the window
        private void quarantinedOnPoisonThenSkipping(List<String> appended, List<String> skipped) {
            execution = execution(CONFIG.withReorderWindow(WINDOW));
            deliver(placed());
            failing.add("poison");
            quarantineOn(added("poison"));
            appended.forEach(item -> events.put(item, added(item)));
            skipped.forEach(item -> deliver(events.get(item)));
            failing.clear();
            store.accepted.clear();
        }

        private void quarantinedOnPoisonThenSkippingHighVersionsFirst() {
            quarantinedOnPoisonThenSkipping(List.of("i3", "i4", "i5", "i6", "i7"), List.of("i6", "i7", "i5", "i3", "i4"));
        }

        private CloudEvent event(String item) {
            return events.get(item);
        }

        // The type marks of the instance that remember a version at or below mark - WINDOW, or forget less than that
        private static List<String> beyondTheWindow(SagaEnvelope<Order> envelope) {
            List<String> beyond = new ArrayList<>();
            envelope.streamMarks().forEach((streamId, marks) -> marks.byType().forEach((type, typeMark) -> {
                long bound = typeMark.mark() - WINDOW;
                if (typeMark.horizon() < bound || typeMark.applied().stream().anyMatch(version -> version <= bound)) {
                    beyond.add(streamId + " " + type + " " + typeMark);
                }
            }));
            return beyond;
        }

        private List<String> beyondTheWindowDuringTheRelease() {
            return store.accepted.stream().flatMap(envelope -> beyondTheWindow(envelope).stream()).toList();
        }

        private TypeMark markOfTheItems() {
            return instance().streamMarks().get(ORDER).byType().values().stream().max(java.util.Comparator.comparingLong(TypeMark::mark)).orElseThrow();
        }

        private ReleaseResult quarantinedAgainOnTheBudgetOfI5() {
            failing.add("i5");
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
            now.set(now.get().plus(BUDGET));
            return execution.release(ORDER);
        }

        private void assertRecordsLeftBehindTheWindow() {
            assertAll(
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(recordedKeys()).as("the records left").containsExactlyInAnyOrder(
                            streamKey(event("i5")), streamKey(event("i3")), streamKey(event("i4"))),
                    () -> assertThat(markOfTheItems().horizon()).as("the horizon, at or above version 5 of the records left").isGreaterThanOrEqualTo(5L)
            );
        }

        private void assertEveryItemIsAppliedOnceAndTheInstanceIsReleasedWithinTheWindow(ReleaseResult result) {
            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).as("the items of the instance").containsExactlyInAnyOrder("poison", "i3", "i4", "i5", "i6", "i7"),
                    () -> assertThat(packed()).as("the commands dispatched").extracting(Pack::item).containsExactlyInAnyOrder("poison", "i3", "i4", "i5", "i6", "i7"),
                    () -> assertThat(beyondTheWindow(instance())).as("the type marks beyond the window").isEmpty()
            );
        }

        @Test
        void leaves_the_marks_within_the_window_when_it_releases_the_instance() {
            quarantinedOnPoisonThenSkippingHighVersionsFirst();

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(beyondTheWindowDuringTheRelease()).as("the type marks beyond the window during the release").isNotEmpty(),
                    () -> assertThat(beyondTheWindow(instance())).as("the type marks beyond the window").isEmpty()
            );
        }

        @Test
        void leaves_the_marks_within_the_window_when_a_replayed_input_completes_the_instance() {
            quarantinedOnPoisonThenSkipping(List.of("i3", "i4", "i5", "i6", "done"), List.of("i6", "i3", "done"));

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(beyondTheWindowDuringTheRelease()).as("the type marks beyond the window during the release").isNotEmpty(),
                    () -> assertThat(beyondTheWindow(instance())).as("the type marks beyond the window").isEmpty()
            );
        }

        @Test
        void leaves_the_marks_within_the_window_when_the_budget_of_a_record_runs_out() {
            quarantinedOnPoisonThenSkippingHighVersionsFirst();

            ReleaseResult result = quarantinedAgainOnTheBudgetOfI5();

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(beyondTheWindowDuringTheRelease()).as("the type marks beyond the window during the release").isNotEmpty(),
                    () -> assertThat(beyondTheWindow(instance())).as("the type marks beyond the window").isEmpty()
            );
        }

        @Test
        void applies_each_record_left_behind_the_window_once_in_the_next_release_after_the_budget_of_a_record_ran_out() {
            quarantinedOnPoisonThenSkippingHighVersionsFirst();
            quarantinedAgainOnTheBudgetOfI5();
            assertRecordsLeftBehindTheWindow();
            failing.clear();

            ReleaseResult result = execution.release(ORDER);

            assertEveryItemIsAppliedOnceAndTheInstanceIsReleasedWithinTheWindow(result);
        }

        @Test
        void quarantines_the_instance_again_on_a_record_behind_the_window_that_keeps_failing_once_its_budget_is_used_up() {
            quarantinedOnPoisonThenSkippingHighVersionsFirst();
            quarantinedAgainOnTheBudgetOfI5();
            assertRecordsLeftBehindTheWindow();
            failing.remove("i5");
            failing.add("i3");

            ReleaseResult first = execution.release(ORDER);
            SagaFailure recorded = instance().failure();
            now.set(now.get().plus(BUDGET));
            ReleaseResult pastTheBudget = execution.release(ORDER);

            SagaFailure quarantinedOn = instance().failure();
            assertAll(
                    () -> assertThat(first).isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(recorded).as("the failure recorded by the first release").isNotNull()
                            .extracting(SagaFailure::input).isEqualTo(streamKey(event("i3"))),
                    () -> assertThat(pastTheBudget).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(quarantinedOn).as("the failure the instance is quarantined on").isNotNull()
                            .extracting(SagaFailure::input, SagaFailure::failureMessage).containsExactly(streamKey(event("i3")), "cannot pack i3")
            );
        }

        @Test
        void applies_a_record_behind_the_window_once_when_the_release_crashes_before_the_write_that_applies_it() {
            quarantinedOnPoisonThenSkippingHighVersionsFirst();
            quarantinedAgainOnTheBudgetOfI5();
            assertRecordsLeftBehindTheWindow();
            failing.remove("i5");
            store.beforeNextDeleteOfARecord(() -> store.beforeNextDeleteOfARecord(() -> {
                throw new OutOfMemoryError("crash before the write that applies i3");
            }));
            Throwable crash = catchThrowable(() -> execution.release(ORDER));
            List<String> itemsAfterTheCrash = items();
            List<String> recordsAfterTheCrash = recordedKeys();

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(crash).hasMessageContaining("crash before the write that applies i3"),
                    () -> assertThat(itemsAfterTheCrash).as("the items written before the crash").doesNotContain("i3"),
                    () -> assertThat(recordsAfterTheCrash).as("the records left by the crash").contains(streamKey(event("i3"))),
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).as("the items of the instance").containsExactlyInAnyOrder("poison", "i3", "i4", "i5", "i6", "i7"),
                    () -> assertThat(packed()).as("commands are dispatched before the write, so the release that crashed before it dispatched i3 and the second release dispatches it again").extracting(Pack::item).containsExactlyInAnyOrder("poison", "i3", "i3", "i4", "i5", "i6", "i7"),
                    () -> assertThat(beyondTheWindow(instance())).as("the type marks beyond the window").isEmpty()
            );
        }
    }

    @Nested
    class WhenTheInstanceMovesOnAfterTheRecordToReplayIsRead {

        private ReleaseResult releaseCatching(AtomicReference<@Nullable Throwable> thrown) {
            AtomicReference<@Nullable ReleaseResult> result = new AtomicReference<>();
            thrown.set(catchThrowable(() -> result.set(execution.release(ORDER))));
            return result.get();
        }

        @Test
        void a_release_whose_instance_is_deleted_after_it_read_the_record_returns_not_found() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            store.afterNextNonEmptyRead(() -> store.delegate.delete(ORDER));
            AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();

            ReleaseResult result = releaseCatching(thrown);

            assertAll(
                    () -> assertThat(thrown.get()).as("what the release threw").isNull(),
                    () -> assertThat(result).isEqualTo(ReleaseResult.NOT_FOUND),
                    () -> assertThat(dispatched).as("the commands dispatched, the record it read is dispatched before the write that finds the instance gone").containsExactly(new Pack(ORDER, "poison"))
            );
        }

        @Test
        void a_release_whose_instance_another_call_quarantined_again_after_it_read_the_record_returns_that_result_and_does_not_replay_the_record() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            now.set(start.plus(Duration.ofHours(1)));
            SagaExecution<OrderEvent, Order, OrderCommand> otherNode = execution(CONFIG);
            AtomicInteger deletesWhenOtherCallEnded = new AtomicInteger();
            store.afterNextNonEmptyRead(() -> {
                failing.add("a");
                assertThat(otherNode.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
                now.set(now.get().plus(BUDGET));
                assertThat(otherNode.release(ORDER)).isEqualTo(ReleaseResult.QUARANTINED_AGAIN);
                deletesWhenOtherCallEnded.set(store.deletedRecords.size());
            });
            AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();

            ReleaseResult result = releaseCatching(thrown);

            String quarantineId = instance().quarantineId();
            assertAll(
                    () -> assertThat(thrown.get()).as("what the release threw").isNull(),
                    () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(store.deletedRecords.subList(deletesWhenOtherCallEnded.get(), store.deletedRecords.size()))
                            .as("the records the release deleted after the other call quarantined the instance again").isEmpty(),
                    () -> assertThat(items()).containsExactly("poison"),
                    () -> assertThat(recordedKeysOf(quarantineId)).containsExactly(streamKey(quarantined.a()), streamKey(quarantined.b()))
            );
        }

        @Test
        void a_release_whose_instance_another_call_released_after_it_read_the_record_returns_released_and_does_not_apply_the_record_again() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            now.set(start.plus(Duration.ofHours(1)));
            SagaExecution<OrderEvent, Order, OrderCommand> otherNode = execution(CONFIG);
            AtomicInteger deletesWhenOtherCallEnded = new AtomicInteger();
            store.afterNextNonEmptyRead(() -> {
                assertThat(otherNode.release(ORDER)).isEqualTo(ReleaseResult.RELEASED);
                deletesWhenOtherCallEnded.set(store.deletedRecords.size());
            });
            AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();

            ReleaseResult result = releaseCatching(thrown);

            assertAll(
                    () -> assertThat(thrown.get()).as("what the release threw").isNull(),
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(store.deletedRecords.subList(deletesWhenOtherCallEnded.get(), store.deletedRecords.size()))
                            .as("the records the release deleted after the other call released the instance").isEmpty(),
                    () -> assertThat(items()).containsExactly("poison", "a", "b"),
                    () -> assertThat(packed()).as("the record it read is dispatched once more before its write loses").containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b"), new Pack(ORDER, "poison"))
            );
        }

        @Test
        void a_release_whose_instance_another_call_released_and_quarantined_again_under_a_new_quarantine_id_after_it_read_the_record_does_not_apply_the_record_of_the_old_id_again() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            now.set(start.plus(Duration.ofHours(1)));
            String oldQuarantineId = instance().quarantineId();
            SagaExecution<OrderEvent, Order, OrderCommand> otherNode = execution(CONFIG);
            AtomicInteger deletesWhenOtherCallEnded = new AtomicInteger();
            AtomicReference<@Nullable String> newQuarantineId = new AtomicReference<>();
            CloudEvent[] c = new CloudEvent[1];
            store.afterNextNonEmptyRead(() -> {
                assertThat(otherNode.release(ORDER)).isEqualTo(ReleaseResult.RELEASED);
                failing.add("c");
                c[0] = added("c");
                catchThrowable(() -> otherNode.onCloudEvent(c[0]));
                now.set(now.get().plus(BUDGET));
                catchThrowable(() -> otherNode.onCloudEvent(c[0]));
                newQuarantineId.set(instance().quarantineId());
                assertThat(otherNode.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
                failing.clear();
                deletesWhenOtherCallEnded.set(store.deletedRecords.size());
            });
            AtomicReference<@Nullable Throwable> thrown = new AtomicReference<>();

            ReleaseResult result = releaseCatching(thrown);

            assertAll(
                    () -> assertThat(newQuarantineId.get()).as("the quarantine id the instance was quarantined under again").isNotEqualTo(oldQuarantineId),
                    () -> assertThat(thrown.get()).as("what the release threw").isNull(),
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(store.deletedRecords.subList(deletesWhenOtherCallEnded.get(), store.deletedRecords.size()))
                            .as("the records the release deleted after the other call quarantined the instance under the new id")
                            .noneMatch(deleted -> deleted.startsWith(oldQuarantineId + "/")),
                    () -> assertThat(items()).containsExactly("poison", "a", "b", "c"),
                    () -> assertThat(packed()).as("the record it read is dispatched once more before its write loses").containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b"), new Pack(ORDER, "poison"), new Pack(ORDER, "c"))
            );
        }
    }

    @Nested
    class TheExitsOfARelease {

        private final AtomicInteger anotherWrites = new AtomicInteger();

        private SagaRunnerConfig withCasAttempts(int attempts) {
            return new SagaRunnerConfig(CONFIG.timerPollInterval(), CONFIG.timerBatchLimit(), attempts, CONFIG.redeliveryDetection(), CONFIG.quarantineAfter(), CONFIG.reorderWindow());
        }

        // A write from outside the release that leaves the instance as it is and moves its version on, so the write of a
        // release that read the instance before it loses
        private void anotherWriteMovesTheVersionOn() {
            anotherWrites.incrementAndGet();
            SagaEnvelope<Order> stored = instance();
            SagaEnvelope<Order> next = new SagaEnvelope<>(stored.sagaId(), stored.state(), stored.status(), stored.version() + 1, stored.timers(),
                    stored.streamWatermarks(), stored.positionWatermark(), stored.createdAt(), stored.updatedAt(), stored.completedAt(),
                    stored.currentStep(), stored.started(), stored.failure(), stored.streamMarks(), stored.quarantineSkipCount());
            assertThat(store.envelopes.compareAndSave(ORDER, next, stored.version())).isTrue();
        }

        private void anotherWriteBeforeEachOfTheNextActiveWrites(int count) {
            anotherWriteMovesTheVersionOn();
            if (count > 1) {
                store.beforeNextActiveWrite(() -> anotherWriteBeforeEachOfTheNextActiveWrites(count - 1));
            }
        }

        private void anotherWriteBeforeEachOfTheNextWritesWithoutState(int count) {
            anotherWriteMovesTheVersionOn();
            if (count > 1) {
                store.beforeNextWriteWithoutState(() -> anotherWriteBeforeEachOfTheNextWritesWithoutState(count - 1));
            }
        }

        @Test
        void a_release_whose_write_that_turns_the_instance_active_loses_on_every_attempt_throws_and_leaves_the_instance_releasing() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            execution = execution(withCasAttempts(3));
            store.beforeNextActiveWrite(() -> anotherWriteBeforeEachOfTheNextActiveWrites(3));

            Throwable thrown = catchThrowable(() -> execution.release(ORDER));

            assertAll(
                    () -> assertThat(thrown).isInstanceOf(SagaConcurrencyException.class)
                            .hasMessageContaining("Failed to go on with the release of saga 'o1' after 3 attempts"),
                    () -> assertThat(anotherWrites.get()).as("the writes that turn the instance active that lost").isEqualTo(3),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(instance().isReleasing()).isTrue(),
                    () -> assertThat(items()).containsExactly("poison", "a", "b")
            );
        }

        @Test
        void a_release_whose_start_write_loses_on_every_attempt_throws_and_leaves_the_instance_quarantined_and_not_releasing() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            execution = execution(withCasAttempts(3));
            store.beforeNextWriteWithoutState(() -> anotherWriteBeforeEachOfTheNextWritesWithoutState(3));

            Throwable thrown = catchThrowable(() -> execution.release(ORDER));

            assertAll(
                    () -> assertThat(thrown).isInstanceOf(SagaConcurrencyException.class)
                            .hasMessageContaining("Failed to start the release of saga 'o1' after 3 attempts"),
                    () -> assertThat(anotherWrites.get()).as("the writes that start the release that lost").isEqualTo(3),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(instance().isReleasing()).isFalse(),
                    () -> assertThat(items()).isEmpty()
            );
        }

        @Test
        void a_failure_of_a_replayed_input_is_recorded_again_when_the_write_that_records_it_loses() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            now.set(start.plus(Duration.ofHours(1)));
            failing.clear();
            failing.add("a");
            beforeTheNextFailure.set(() -> store.beforeNextWriteWithoutState(this::anotherWriteMovesTheVersionOn));

            ReleaseResult result = execution.release(ORDER);

            SagaEnvelope<Order> after = instance();
            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(after.isReleasing()).isTrue(),
                    () -> assertThat(after.failure().input()).isEqualTo(streamKey(quarantined.a())),
                    () -> assertThat(after.failure().failureMessage()).isEqualTo("cannot pack a"),
                    () -> assertThat(after.state().items()).containsExactly("poison")
            );
        }

        @Test
        void a_release_of_an_instance_with_quarantine_turned_off_stays_on_a_failing_input_however_long_it_has_failed() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            SagaExecution<OrderEvent, Order, OrderCommand> withoutQuarantine = execution(CONFIG.disableQuarantine());
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);
            ReleaseResult first = withoutQuarantine.release(ORDER);
            now.set(releasedAt.plus(Duration.ofDays(365)));

            ReleaseResult longAfter = withoutQuarantine.release(ORDER);

            assertAll(
                    () -> assertThat(first).isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(longAfter).isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(instance().isReleasing()).isTrue(),
                    () -> assertThat(items()).isEmpty()
            );
        }

        @Test
        void a_failure_of_an_input_that_another_call_applied_in_between_is_not_recorded_and_the_release_stays_as_that_call_left_it() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            now.set(start.plus(Duration.ofHours(1)));
            failing.clear();
            failing.add("a");
            SagaExecution<OrderEvent, Order, OrderCommand> otherNode = execution(CONFIG);
            // The other call applies "a" and crashes before it applies "b", so "b" is the record that is left
            beforeTheNextFailure.set(() -> {
                failing.clear();
                AtomicInteger replays = new AtomicInteger();
                store.beforeNextDeleteOfARecord(new Runnable() {
                    @Override
                    public void run() {
                        if (replays.incrementAndGet() == 2) {
                            throw new OutOfMemoryError("crash before the write that applies the record");
                        }
                        store.beforeNextDeleteOfARecord(this);
                    }
                });
                catchThrowable(() -> otherNode.release(ORDER));
            });

            ReleaseResult result = execution.release(ORDER);

            SagaEnvelope<Order> after = instance();
            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(after.isReleasing()).isTrue(),
                    () -> assertThat(after.state().items()).containsExactly("poison", "a"),
                    () -> assertThat(after.failure().input()).isEqualTo(streamKey(quarantined.k())),
                    () -> assertThat(after.failure().failureMessage()).isEqualTo("cannot pack poison"),
                    () -> assertThat(recordedKeys()).containsExactly(streamKey(quarantined.b()))
            );
        }

        @Test
        void a_release_that_finds_the_instance_quarantined_again_by_another_call_returns_that_result() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            now.set(start.plus(Duration.ofHours(1)));
            SagaExecution<OrderEvent, Order, OrderCommand> otherNode = execution(CONFIG);
            AtomicReference<@Nullable ReleaseResult> otherResult = new AtomicReference<>();
            store.afterNextWriteWithoutState(() -> {
                failing.add("poison");
                assertThat(otherNode.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
                now.set(now.get().plus(BUDGET));
                otherResult.set(otherNode.release(ORDER));
            });

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(otherResult).hasValue(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(instance().isReleasing()).isFalse()
            );
        }
    }

    @Nested
    class WhenItsStateCannotBeLoaded {

        @Test
        void a_quarantined_instance_still_records_the_input_it_skips_without_writing_its_state() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            SagaEnvelope<Order> before = instance();
            List<String> recordsBefore = recordedKeys();
            CloudEvent c = added("c");
            store.refusesToDecode = true;

            Throwable thrown = catchThrowable(() -> deliver(c));

            store.refusesToDecode = false;
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(recordedKeys()).containsExactlyElementsOf(appended(recordsBefore, streamKey(c))),
                    () -> assertThat(instance().state()).isEqualTo(before.state()),
                    () -> assertThat(instance().streamWatermarks()).isEqualTo(before.streamWatermarks())
            );
        }

        @Test
        void a_lost_write_of_the_record_is_retried() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            List<String> recordsBefore = recordedKeys();
            CloudEvent c = added("c");
            CloudEvent d = added("d");
            store.refusesToDecode = true;
            store.beforeNextWriteWithoutState(() -> deliver(d));

            Throwable thrown = catchThrowable(() -> deliver(c));

            store.refusesToDecode = false;
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(recordedKeys()).containsExactlyElementsOf(appended(recordsBefore, streamKey(d), streamKey(c)))
            );
        }
    }

    @Nested
    class WhenDecodingItsStateFails {

        @Test
        void the_release_is_refused_naming_the_failure_and_delete_as_the_way_out_and_the_instance_is_left_as_it_was() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            SagaEnvelope<Order> before = store.envelopes.findWithoutState(ORDER).orElseThrow();
            store.refusesToDecode = true;

            Throwable thrown = catchThrowable(() -> execution.release(ORDER));

            store.refusesToDecode = false;
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("reading its state failed with java.lang.IllegalStateException: the state can no longer be decoded")
                            .hasMessageContaining("SagaStateStore.delete(\"" + ORDER + "\")"),
                    () -> assertThat(thrown.getCause()).isInstanceOf(IllegalStateException.class).hasMessage("the state can no longer be decoded"),
                    () -> assertThat(store.envelopes.findWithoutState(ORDER).orElseThrow()).isEqualTo(before),
                    () -> assertThat(instance().isReleasing()).isFalse()
            );
        }

        @Test
        void the_instance_is_released_once_its_state_decodes_again() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            store.refusesToDecode = true;
            catchThrowable(() -> execution.release(ORDER));
            store.refusesToDecode = false;

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).containsExactly("poison", "a", "b")
            );
        }

        @Test
        void a_read_that_fails_while_the_instance_cannot_be_read_without_its_state_either_propagates_as_it_is_with_the_second_failure_suppressed() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            IllegalStateException readFailure = new IllegalStateException("the store is down");
            UnsupportedOperationException secondFailure = new UnsupportedOperationException("cannot read without the state");
            store.findFailure = readFailure;
            store.findWithoutStateFailure = secondFailure;

            Throwable thrown = catchThrowable(() -> execution.release(ORDER));

            store.findFailure = null;
            store.findWithoutStateFailure = null;
            assertAll(
                    () -> assertThat(thrown).isSameAs(readFailure),
                    () -> assertThat(thrown).hasMessage("the store is down"),
                    () -> assertThat(thrown.getSuppressed()).containsExactly(secondFailure),
                    () -> assertThat(instance().isReleasing()).isFalse()
            );
        }

        @Test
        void a_read_that_fails_while_no_such_instance_is_found_without_its_state_propagates_as_it_is() {
            IllegalStateException readFailure = new IllegalStateException("the store is down");
            store.findFailure = readFailure;

            Throwable thrown = catchThrowable(() -> execution.release("unknown"));

            store.findFailure = null;
            assertAll(
                    () -> assertThat(thrown).isSameAs(readFailure),
                    () -> assertThat(thrown.getSuppressed()).isEmpty()
            );
        }
    }

    // A release that quarantines the instance again keeps the record it replayed. Were it to delete the record after
    // its write, a second release that started in between would lose it. The hook stands for that delete, so it only runs when the
    // release deletes after the write that quarantined the instance again, and the tests pass without it running
    private void onADeleteOnceQuarantinedAgain(java.util.function.Predicate<SagaEnvelope<Order>> quarantinedAgain, Runnable secondRelease) {
        store.beforeNextDeleteOfARecord(new Runnable() {
            @Override
            public void run() {
                if (quarantinedAgain.test(instance())) {
                    secondRelease.run();
                } else {
                    store.beforeNextDeleteOfARecord(this);
                }
            }
        });
    }

    private java.util.function.Predicate<SagaEnvelope<Order>> quarantinedAgainOn(String key) {
        return envelope -> !envelope.isReleasing() && key.equals(envelope.failure().input());
    }

    // A hook still armed from the first release is disarmed, so the next release does not run it at its first delete
    private void assertEveryInputIsAppliedByTheNextRelease(SagaExecution<OrderEvent, Order, OrderCommand> node, ReleaseResult firstResult, ReleaseResult expectedFirst) {
        store.beforeNextDeleteOfARecord(() -> {
        });
        failing.clear();
        String quarantineId = instance().quarantineId();
        ReleaseResult last = node.release(ORDER);
        assertAll(
                () -> assertThat(firstResult).isEqualTo(expectedFirst),
                () -> assertThat(last).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(items()).containsExactly("poison", "a", "b"),
                () -> assertThat(recordedKeysOf(quarantineId)).isEmpty()
        );
    }

    @Nested
    class WhenARecordIsLeftByTheReleaseThatQuarantinedTheInstanceAgain {

        @Test
        void an_input_that_failed_again_is_applied_by_the_release_that_goes_active() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);
            failing.clear();
            failing.add("a");
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
            now.set(releasedAt.plus(BUDGET));
            SagaExecution<OrderEvent, Order, OrderCommand> secondNode = execution(CONFIG);
            onADeleteOnceQuarantinedAgain(quarantinedAgainOn(streamKey(quarantined.a())), () -> secondNode.release(ORDER));

            ReleaseResult firstResult = execution.release(ORDER);

            assertEveryInputIsAppliedByTheNextRelease(secondNode, firstResult, ReleaseResult.QUARANTINED_AGAIN);
        }

        /**
         * With no stream key the failure is named by the CloudEvent key of the record, so it has a budget too.
         */
        @Test
        void a_streamless_input_that_keeps_failing_is_quarantined_again_past_its_budget_and_applied_once_it_stops() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoSkipped();
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);
            failing.clear();
            failing.add("a");
            ReleaseResult first = execution.release(ORDER);
            SagaEnvelope<Order> afterTheFirstFailure = instance();
            now.set(releasedAt.plus(BUDGET.dividedBy(3)));
            ReleaseResult insideTheBudget = execution.release(ORDER);
            SagaEnvelope<Order> afterTheRetry = instance();
            now.set(releasedAt.plus(BUDGET));
            ReleaseResult pastTheBudget = execution.release(ORDER);
            SagaEnvelope<Order> requarantined = instance();
            List<String> recordsLeft = recordedKeys();
            String quarantineId = requarantined.quarantineId();
            failing.clear();

            ReleaseResult last = execution.release(ORDER);

            assertAll(
                    () -> assertThat(first).as("first failure").isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(afterTheFirstFailure.failure().input()).as("input named by the first failure")
                            .isEqualTo(cloudEventKey(quarantined.a())),
                    () -> assertThat(insideTheBudget).as("inside the budget").isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(afterTheRetry.version()).as("version").isEqualTo(afterTheFirstFailure.version()),
                    () -> assertThat(pastTheBudget).as("past the budget").isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(requarantined.status()).as("status").isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(requarantined.isReleasing()).as("releasing").isFalse(),
                    () -> assertThat(requarantined.failure().input()).as("input")
                            .isEqualTo(cloudEventKey(quarantined.a())),
                    () -> assertThat(requarantined.failure().releasedAt()).as("releasedAt").isNull(),
                    () -> assertThat(recordsLeft).as("records held")
                            .containsExactly(cloudEventKey(quarantined.a()), streamKey(quarantined.b())),
                    () -> assertThat(last).as("release once it stops failing").isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(instance().status()).as("status after the release").isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).as("items").containsExactly("poison", "a", "b"),
                    () -> assertThat(packed()).extracting(Pack::item).as("packed").containsExactly("poison", "a", "b"),
                    () -> assertThat(recordedKeysOf(quarantineId)).as("records left").isEmpty()
            );
        }

        @Test
        void a_streamless_input_held_under_best_effort_is_applied_once_by_a_release_from_a_required_runner() {
            quarantinedOnPoisonWithTwoSkipped();
            failing.clear();
            now.set(start.plus(Duration.ofHours(1)));
            SagaExecution<OrderEvent, Order, OrderCommand> required =
                    execution(CONFIG.withRedeliveryDetection(RedeliveryDetection.REQUIRED));

            ReleaseResult result = required.release(ORDER);

            assertAll(
                    () -> assertThat(result).as("result").isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).as("items").containsExactly("poison", "a", "b"),
                    () -> assertThat(packed()).extracting(Pack::item).as("packed").containsExactly("poison", "a", "b")
            );
        }
    }

    @Nested
    class WhenTheRecordOfTheQuarantiningInputIsLeftByTheReleaseThatQuarantinedTheInstanceAgain {

        @Test
        void the_input_the_instance_was_quarantined_on_that_failed_again_is_applied_by_the_release_that_goes_active() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
            now.set(releasedAt.plus(BUDGET));
            SagaExecution<OrderEvent, Order, OrderCommand> secondNode = execution(CONFIG);
            onADeleteOnceQuarantinedAgain(quarantinedAgainOn(streamKey(quarantined.k())), () -> secondNode.release(ORDER));

            ReleaseResult firstResult = execution.release(ORDER);

            assertEveryInputIsAppliedByTheNextRelease(secondNode, firstResult, ReleaseResult.QUARANTINED_AGAIN);
        }
    }

    @Nested
    class WhenThePollerGoesOnWithReleases {

        private static final int BATCH_LIMIT = 4;
        private final SagaRunnerConfig limited = new SagaRunnerConfig(Duration.ofSeconds(15), BATCH_LIMIT, 50, RedeliveryDetection.BEST_EFFORT, Optional.of(BUDGET));

        @BeforeEach
        void useTheBatchLimit() {
            execution = execution(limited);
        }

        // Quarantined on "poison" with the items skipped after it, and releasing, because the release stopped on "poison" again
        private void releasingOnPoison(String orderId, String... skipped) {
            deliver(appendOn(orderId, new OrderPlaced(nextEventId(), orderId)));
            failing.add("poison");
            CloudEvent poison = appendOn(orderId, new ItemAdded(nextEventId(), orderId, "poison"));
            assertThatThrownBy(() -> deliver(poison)).isInstanceOf(IllegalStateException.class);
            now.set(now.get().plus(BUDGET));
            catchThrowable(() -> deliver(poison));
            for (String item : skipped) {
                deliver(appendOn(orderId, new ItemAdded(nextEventId(), orderId, item)));
            }
            assertThat(execution.release(orderId)).isEqualTo(ReleaseResult.STILL_RELEASING);
        }

        private SagaEnvelope<Order> instanceOf(String orderId) {
            return store.envelopes.find(orderId).orElseThrow();
        }

        private List<String> itemsOf(String orderId) {
            return instanceOf(orderId).state().items();
        }

        @Test
        void replays_at_most_the_share_of_each_instance_and_the_batch_limit_in_total_and_an_instance_with_few_records_is_not_held_back_by_one_with_many() {
            releasingOnPoison("o1", "a1", "a2", "a3", "a4", "a5", "a6");
            releasingOnPoison("o2", "b1");
            failing.clear();
            dispatched.clear();

            pollAfter(limited);

            assertAll(
                    () -> assertThat(itemsOf("o1")).containsExactly("poison", "a1"),
                    () -> assertThat(instanceOf("o1").isReleasing()).isTrue(),
                    () -> assertThat(itemsOf("o2")).containsExactly("poison", "b1"),
                    () -> assertThat(instanceOf("o2").status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(packed()).hasSize(BATCH_LIMIT)
            );
        }

        @Test
        void the_next_poll_goes_on_where_the_last_one_stopped() {
            releasingOnPoison("o1", "a1", "a2", "a3", "a4", "a5", "a6");
            releasingOnPoison("o2", "b1");
            failing.clear();
            pollAfter(limited);

            pollAfter(limited);
            List<String> afterTheSecondPoll = itemsOf("o1");
            boolean releasingAfterTheSecondPoll = instanceOf("o1").isReleasing();
            pollAfter(limited);

            assertAll(
                    () -> assertThat(afterTheSecondPoll).containsExactly("poison", "a1", "a2", "a3", "a4", "a5"),
                    () -> assertThat(releasingAfterTheSecondPoll).isTrue(),
                    () -> assertThat(itemsOf("o1")).containsExactly("poison", "a1", "a2", "a3", "a4", "a5", "a6"),
                    () -> assertThat(instanceOf("o1").status()).isEqualTo(SagaStatus.ACTIVE)
            );
        }

        @Test
        void gives_each_of_more_instances_than_the_batch_limit_allows_one_record_and_leaves_the_rest_for_a_later_poll() {
            for (int i = 1; i <= 6; i++) {
                releasingOnPoison("o" + i, "x", "y", "z");
            }
            failing.clear();
            dispatched.clear();

            pollAfter(limited);

            assertAll(
                    () -> assertThat(itemsOf("o1")).containsExactly("poison"),
                    () -> assertThat(itemsOf("o2")).containsExactly("poison"),
                    () -> assertThat(itemsOf("o3")).containsExactly("poison"),
                    () -> assertThat(itemsOf("o4")).containsExactly("poison"),
                    () -> assertThat(itemsOf("o5")).isEmpty(),
                    () -> assertThat(itemsOf("o6")).isEmpty(),
                    () -> assertThat(packed()).hasSize(BATCH_LIMIT)
            );
        }

        @Test
        void gives_every_instance_a_store_lists_beyond_the_limit_one_record_rather_than_none() {
            for (int i = 1; i <= 6; i++) {
                releasingOnPoison("o" + i, "x", "y");
            }
            failing.clear();
            store.listsEveryReleasingInstanceWhateverTheLimit = true;

            pollAfter(limited);

            assertAll(
                    () -> assertThat(itemsOf("o1")).containsExactly("poison"),
                    () -> assertThat(itemsOf("o6")).containsExactly("poison")
            );
        }

        @Test
        void turns_the_instance_active_in_the_poll_whose_share_its_last_record_uses_up() {
            releasingOnPoison("o1", "a1", "a2", "a3");
            failing.clear();

            pollAfter(limited);

            assertAll(
                    () -> assertThat(itemsOf("o1")).containsExactly("poison", "a1", "a2", "a3"),
                    () -> assertThat(instanceOf("o1").status()).isEqualTo(SagaStatus.ACTIVE)
            );
        }

        @Test
        void gives_every_one_of_more_releasing_instances_than_the_batch_limit_a_record_within_the_polls_it_takes_to_list_them_all_and_one_more() {
            int instances = 6;
            for (int i = 1; i <= instances; i++) {
                releasingOnPoison("o" + i, "x", "y", "z");
            }
            failing.clear();

            int polls = (instances + BATCH_LIMIT - 1) / BATCH_LIMIT + 1;
            for (int poll = 0; poll < polls; poll++) {
                pollAfter(limited);
            }

            for (int i = 1; i <= instances; i++) {
                assertThat(itemsOf("o" + i)).as("o%d after %d polls", i, polls).isNotEmpty();
            }
        }

        @Test
        void lists_from_the_first_instance_again_in_the_poll_after_the_one_that_listed_a_short_page() {
            for (int i = 1; i <= 6; i++) {
                releasingOnPoison("o" + i, "x", "y", "z");
            }
            failing.clear();
            pollAfter(limited);
            pollAfter(limited);

            pollAfter(limited);

            assertAll(
                    () -> assertThat(itemsOf("o1")).containsExactly("poison", "x"),
                    () -> assertThat(itemsOf("o4")).containsExactly("poison", "x"),
                    () -> assertThat(itemsOf("o5")).containsExactly("poison", "x"),
                    () -> assertThat(itemsOf("o6")).containsExactly("poison", "x")
            );
        }

        @Test
        void a_release_asked_for_directly_replays_every_record_whatever_the_batch_limit() {
            releasingOnPoison("o1", "a1", "a2", "a3", "a4", "a5", "a6");
            failing.clear();

            ReleaseResult result = execution.release("o1");

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(itemsOf("o1")).containsExactly("poison", "a1", "a2", "a3", "a4", "a5", "a6"),
                    () -> assertThat(instanceOf("o1").status()).isEqualTo(SagaStatus.ACTIVE)
            );
        }
    }

    @Nested
    class WhenTheRecordWriteLoses {

        @Test
        void the_skip_is_retried_so_both_inputs_are_recorded_once() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            List<String> recordsBefore = recordedKeys();
            CloudEvent c = added("c");
            CloudEvent d = added("d");
            store.beforeNextWriteWithoutState(() -> deliver(d));

            deliver(c);

            assertThat(recordedKeys()).containsExactlyElementsOf(appended(recordsBefore, streamKey(d), streamKey(c)));
        }
    }

    @Nested
    class TheRecordsOfSkippedInputs {

        @Test
        void each_gets_a_sequence_number_of_its_own_and_is_read_in_the_order_the_inputs_were_skipped() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            CloudEvent c = added("c");

            deliver(c);

            assertAll(
                    () -> assertThat(records()).extracting(SkippedInput::key).containsExactly(streamKey(quarantined.k()), streamKey(quarantined.a()), streamKey(quarantined.b()), streamKey(c)),
                    () -> assertThat(records()).extracting(SkippedInput::seq).doesNotHaveDuplicates().isSorted(),
                    () -> assertThat(instance().quarantineSkipCount()).isEqualTo(4L)
            );
        }

        @Test
        void a_skip_whose_record_was_never_written_leaves_a_gap_and_the_inputs_still_replay_in_the_order_they_arrived_across_streams() {
            deliver(placed());
            failing.add("poison");
            CloudEvent k = added("poison");
            quarantineOn(k);
            CloudEvent x = appendOn("o1-other", new ItemAdded(nextEventId(), ORDER, "x"));
            store.beforeNextRecord(() -> {
                throw new IllegalStateException("crash after the count and before the record");
            });
            assertThatThrownBy(() -> deliver(x)).hasMessage("crash after the count and before the record");
            CloudEvent y = added("y");
            deliver(y);

            // x was never acknowledged, so it is offered again after y
            deliver(x);

            List<SkippedInput> records = records();
            failing.clear();
            ReleaseResult result = execution.release(ORDER);
            assertAll(
                    () -> assertThat(records).extracting(SkippedInput::key).containsExactly(streamKey(k), streamKey(y), streamKey(x)),
                    () -> assertThat(records).extracting(SkippedInput::seq).doesNotHaveDuplicates().isSorted(),
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).containsExactly("poison", "y", "x")
            );
        }

        @Test
        void a_skip_whose_record_was_written_but_not_finished_keeps_its_place_when_it_is_offered_again_after_a_later_input() {
            deliver(placed());
            failing.add("poison");
            CloudEvent k = added("poison");
            quarantineOn(k);
            CloudEvent x = appendOn("o1-other", new ItemAdded(nextEventId(), ORDER, "x"));
            store.afterNextRecord(() -> {
                throw new IllegalStateException("crash after the record and before the version");
            });
            assertThatThrownBy(() -> deliver(x)).hasMessage("crash after the record and before the version");
            CloudEvent y = added("y");
            deliver(y);
            SkippedInput recordedFirst = records().get(1);

            deliver(x);

            List<SkippedInput> records = records();
            failing.clear();
            ReleaseResult result = execution.release(ORDER);
            assertAll(
                    () -> assertThat(recordedFirst.key()).isEqualTo(streamKey(x)),
                    () -> assertThat(records).extracting(SkippedInput::key).containsExactly(streamKey(k), streamKey(x), streamKey(y)),
                    () -> assertThat(records.get(1)).isEqualTo(recordedFirst),
                    () -> assertThat(records).extracting(SkippedInput::seq).doesNotHaveDuplicates().isSorted(),
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).containsExactly("poison", "x", "y")
            );
        }

        @Test
        void a_skip_whose_last_write_loses_to_another_skip_keeps_its_record_and_its_place() {
            deliver(placed());
            failing.add("poison");
            CloudEvent k = added("poison");
            quarantineOn(k);
            CloudEvent x = appendOn("o1-other", new ItemAdded(nextEventId(), ORDER, "x"));
            CloudEvent y = added("y");
            // y is skipped in full between x's record and x's last write, so x's last write loses while the instance stays quarantined
            store.afterNextRecord(() -> deliver(y));

            deliver(x);

            List<SkippedInput> records = records();
            failing.clear();
            ReleaseResult result = execution.release(ORDER);
            assertAll(
                    () -> assertThat(records).extracting(SkippedInput::key).containsExactly(streamKey(k), streamKey(x), streamKey(y)),
                    () -> assertThat(records).extracting(SkippedInput::seq).doesNotHaveDuplicates().isSorted(),
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).containsExactly("poison", "x", "y")
            );
        }

        @Test
        void a_redelivered_skip_of_a_recorded_key_raises_the_count_keeps_the_first_record_and_its_number_and_changes_the_version_once() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            SagaEnvelope<Order> before = instance();
            List<SkippedInput> recordsBefore = records();

            deliver(quarantined.b());

            assertAll(
                    () -> assertThat(records()).isEqualTo(recordsBefore),
                    () -> assertThat(instance().quarantineSkipCount()).isEqualTo(before.quarantineSkipCount() + 1),
                    () -> assertThat(instance().version()).isEqualTo(before.version() + 1),
                    () -> assertThat(instance().streamWatermarks()).isEqualTo(before.streamWatermarks())
            );
        }

        @Test
        void a_record_left_under_an_earlier_quarantine_id_is_never_replayed() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            CloudEvent ghost = added("ghost");
            recordUnder(ORDER, "earlier", new SkippedInput(0, streamKey(ghost), ghost));
            failing.clear();

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).containsExactly("poison", "a", "b")
            );
        }
    }

    @Nested
    class TheQuarantiningWrite {

        @Test
        void sets_the_skip_count_to_1_and_a_quarantine_id_when_an_active_instance_is_quarantined() {
            deliver(placed());
            failing.add("poison");

            quarantineOn(added("poison"));

            assertAll(
                    () -> assertThat(instance().quarantineSkipCount()).isEqualTo(1L),
                    () -> assertThat(instance().quarantineId()).isNotBlank()
            );
        }
    }

    @Nested
    class AnInstanceQuarantinedBy034 {

        private SagaEnvelope<Order> quarantinedWithNoQuarantineId(@Nullable Long skipCount) {
            CloudEvent placed = placed();
            SagaFailure failure = new SagaFailure(streamKey(placed), 1L, start, IllegalStateException.class.getName(), "boom", null, null);
            SagaEnvelope<Order> envelope = new SagaEnvelope<>(ORDER, new Order(ORDER, List.of()), SagaStatus.QUARANTINED, 1, List.of(),
                    Map.of(ORDER, 1L), null, start, start, null, null, true, failure, Map.of(), skipCount);
            store.envelopes.compareAndSave(ORDER, envelope, 0);
            return envelope;
        }

        @Test
        void records_nothing_writes_no_count_and_keeps_its_version() {
            SagaEnvelope<Order> before = quarantinedWithNoQuarantineId(null);

            deliver(added("a"));

            assertAll(
                    () -> assertThat(instance()).isEqualTo(before),
                    () -> assertThat(store.writesWithoutState).hasValue(0)
            );
        }

        @Test
        void records_nothing_writes_no_count_and_keeps_its_version_even_when_a_store_left_a_skip_count_on_it() {
            SagaEnvelope<Order> before = quarantinedWithNoQuarantineId(5L);

            deliver(added("a"));

            assertAll(
                    () -> assertThat(instance()).isEqualTo(before),
                    () -> assertThat(store.writesWithoutState).hasValue(0)
            );
        }
    }

    @Nested
    class AReleaseThatIsQuarantinedAgain {

        @Test
        void keeps_the_quarantine_id_and_the_skip_count_when_the_budget_of_the_input_it_replays_runs_out() {
            quarantinedOnPoisonWithTwoSkipped();
            String quarantineId = instance().quarantineId();
            Long skipCount = instance().quarantineSkipCount();
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);
            execution.release(ORDER);
            now.set(releasedAt.plus(BUDGET));

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(instance().quarantineId()).isEqualTo(quarantineId),
                    () -> assertThat(instance().quarantineSkipCount()).isEqualTo(skipCount)
            );
        }

        @Test
        void is_released_by_a_second_release_that_applies_the_input_it_was_quarantined_on_and_then_the_records() {
            quarantinedOnPoisonWithTwoSkipped();
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);
            execution.release(ORDER);
            now.set(releasedAt.plus(BUDGET));
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.QUARANTINED_AGAIN);
            failing.clear();

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).containsExactly("poison", "a", "b"),
                    () -> assertThat(packed()).extracting(Pack::item).containsExactly("poison", "a", "b")
            );
        }
    }

    @Nested
    class AReplayedEventThatHoldsAByteArray {

        private CloudEvent withByteArrayExtension(CloudEvent event) {
            return CloudEventBuilder.v1(event).withExtension("blob", new byte[]{1, 2}).build();
        }

        private CloudEvent withByteArrayInMapData(ItemAdded item) {
            Map<String, Object> data = Map.of("eventId", item.eventId(), "orderId", item.orderId(), "item", item.item(), "blob", new byte[]{1, 2});
            return CloudEventBuilder.v1(added(item.item())).withData(PojoCloudEventData.wrap(data, ignored -> new byte[0])).build();
        }

        private CloudEventConverter<OrderEvent> readingMapData() {
            return new CloudEventConverter<>() {
                @Override
                public CloudEvent toCloudEvent(OrderEvent domainEvent) {
                    return converter.toCloudEvent(domainEvent);
                }

                @Override
                public OrderEvent toDomainEvent(CloudEvent cloudEvent) {
                    if (cloudEvent.getData() instanceof PojoCloudEventData<?> pojo && pojo.getValue() instanceof Map<?, ?> map) {
                        return new ItemAdded((String) map.get("eventId"), (String) map.get("orderId"), (String) map.get("item"));
                    }
                    return converter.toDomainEvent(cloudEvent);
                }

                @Override
                public String getCloudEventType(Class<? extends OrderEvent> type) {
                    return converter.getCloudEventType(type);
                }
            };
        }

        private void assertThatARepeatedlyFailingReleaseRunsOutOfItsBudget(SagaExecution<OrderEvent, Order, OrderCommand> releasing) {
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);
            ReleaseResult first = releasing.release(ORDER);
            SagaFailure afterTheFirst = instance().failure();
            now.set(releasedAt.plus(BUDGET));

            ReleaseResult second = releasing.release(ORDER);

            assertAll(
                    () -> assertThat(first).isEqualTo(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(afterTheFirst.releasedAt()).as("the failure recorded by the first release").isEqualTo(releasedAt),
                    () -> assertThat(second).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(instance().failure().releasedAt()).isNull()
            );
        }

        @Test
        void a_release_that_fails_every_time_on_an_event_with_a_byte_array_extension_runs_out_of_its_budget_and_is_quarantined_again() {
            deliver(placed());
            failing.add("poison");
            quarantineOn(withByteArrayExtension(added("poison")));

            assertThatARepeatedlyFailingReleaseRunsOutOfItsBudget(execution);
        }

        @Test
        void a_release_that_fails_every_time_on_an_event_with_a_byte_array_in_its_map_data_runs_out_of_its_budget_and_is_quarantined_again() {
            deliver(placed());
            failing.add("poison");
            SagaExecution<OrderEvent, Order, OrderCommand> readingMaps = new SagaExecution<>("orders", saga(), store, dispatched::add, readingMapData(), CONFIG, now::get);
            CloudEvent poison = withByteArrayInMapData(new ItemAdded(nextEventId(), ORDER, "poison"));
            assertThatThrownBy(() -> readingMaps.onCloudEvent(poison)).isInstanceOf(IllegalStateException.class);
            now.set(now.get().plus(BUDGET));
            catchThrowable(() -> readingMaps.onCloudEvent(poison));
            assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED);

            assertThatARepeatedlyFailingReleaseRunsOutOfItsBudget(readingMaps);
        }

        @Test
        void a_write_lost_only_to_a_skip_is_written_again_and_the_reaction_dispatched_once_when_the_replayed_event_has_a_byte_array_extension() {
            deliver(placed());
            failing.add("poison");
            quarantineOn(withByteArrayExtension(added("poison")));
            failing.clear();
            CloudEvent live = added("live");
            store.beforeNextStateWrite(() -> deliver(live));

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(dispatched).containsOnlyOnce(new Pack(ORDER, "poison")),
                    () -> assertThat(items()).containsExactly("poison", "live")
            );
        }
    }

    @Nested
    class TheInputTheInstanceWasQuarantinedOn {

        @Test
        void is_recorded_with_its_event_in_the_write_that_quarantines_the_instance_at_a_number_below_every_other() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();

            List<SkippedInput> recorded = records();

            assertAll(
                    () -> assertThat(recorded).extracting(SkippedInput::key)
                            .containsExactly(streamKey(quarantined.k()), streamKey(quarantined.a()), streamKey(quarantined.b())),
                    () -> assertThat(recorded.getFirst().seq()).isEqualTo(0L),
                    () -> assertThat(recorded.getFirst().event()).isEqualTo(quarantined.k()),
                    () -> assertThat(recorded).extracting(SkippedInput::seq).isSorted().doesNotHaveDuplicates()
            );
        }

        @Test
        void is_applied_once_when_a_crash_before_the_write_that_applies_it_leaves_it_and_its_record_for_the_next_pass() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
            failing.clear();
            store.beforeNextDeleteOfARecord(() -> {
                throw new OutOfMemoryError("crash before the write that applies the record");
            });
            assertThatThrownBy(() -> execution.release(ORDER)).hasMessage("crash before the write that applies the record");
            List<String> keysAfterTheCrash = recordedKeys();
            List<String> itemsAfterTheCrash = items();

            pollAfterTheInterval();

            assertAll(
                    () -> assertThat(itemsAfterTheCrash).as("nothing was applied before the crash").isEmpty(),
                    () -> assertThat(keysAfterTheCrash).hasSize(3),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).containsExactly("poison", "a", "b"),
                    () -> assertThat(packed()).as("commands are dispatched before the write, so the pass that crashed before it dispatched the first one and the next pass dispatches it again").extracting(Pack::item).containsExactly("poison", "poison", "a", "b")
            );
        }
    }

    @Nested
    class WhenAnInputIsSkippedWhileTheInstanceIsReleasing {

        @Test
        void the_write_that_finishes_the_skip_makes_the_release_lose_its_write_that_makes_the_instance_active_and_the_release_replays_the_new_record() throws Exception {
            deliver(placed());
            failing.add("poison");
            quarantineOn(added("poison"));
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
            failing.clear();
            CloudEvent live = added("live");
            CountDownLatch counted = new CountDownLatch(1);
            CountDownLatch mayRecord = new CountDownLatch(1);
            AtomicReference<@Nullable Throwable> skipFailure = new AtomicReference<>();
            store.beforeNextRecord(() -> {
                counted.countDown();
                await(mayRecord);
            });
            Thread skipper = new Thread(() -> {
                try {
                    deliver(live);
                } catch (Throwable e) {
                    skipFailure.set(e);
                }
            });
            skipper.start();
            assertThat(counted.await(10, TimeUnit.SECONDS)).isTrue();
            // The release has read the instance after the count and the records before the record
            store.afterNextEmptyRead(() -> {
                mayRecord.countDown();
                join(skipper);
            });

            pollAfterTheInterval();

            assertAll(
                    () -> assertThat(skipFailure.get()).isNull(),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).containsExactly("poison", "live"),
                    () -> assertThat(packed()).extracting(Pack::item).containsExactly("poison", "live")
            );
        }

        @Test
        void a_skip_that_finds_the_instance_active_when_it_finishes_applies_the_input_like_a_live_one() {
            deliver(placed());
            failing.add("poison");
            quarantineOn(added("poison"));
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
            failing.clear();
            CloudEvent live = added("live");
            // The release finishes between the skip's count and its record
            store.beforeNextRecord(SagaReleaseTest.this::pollAfterTheInterval);

            deliver(live);

            assertAll(
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).containsExactly("poison", "live"),
                    () -> assertThat(packed()).extracting(Pack::item).containsExactly("poison", "live")
            );
        }

        @Test
        void a_skip_whose_last_write_loses_to_a_release_that_found_no_record_deletes_its_record_and_applies_the_input_live() {
            deliver(placed());
            failing.add("poison");
            quarantineOn(added("poison"));
            String quarantineId = instance().quarantineId();
            failing.clear();
            CloudEvent live = added("live");
            AtomicReference<@Nullable ReleaseResult> released = new AtomicReference<>();
            // The release reads the instance after the skip's count and finds no record, then the record is written
            store.beforeNextRecord(() -> released.set(execution.release(ORDER)));

            deliver(live);

            assertAll(
                    () -> assertThat(released.get()).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(recordedKeysOf(quarantineId)).isEmpty(),
                    () -> assertThat(items()).containsExactly("poison", "live"),
                    () -> assertThat(packed()).extracting(Pack::item).containsExactly("poison", "live")
            );
        }

        @Test
        void a_write_lost_only_to_such_a_skip_is_written_again_and_the_reaction_is_dispatched_once() {
            deliver(placed());
            failing.add("poison");
            quarantineOn(added("poison"));
            failing.clear();
            CloudEvent live = added("live");
            store.beforeNextStateWrite(() -> deliver(live));

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(dispatched).containsOnlyOnce(new Pack(ORDER, "poison")),
                    () -> assertThat(items()).containsExactly("poison", "live")
            );
        }

        private static void await(CountDownLatch latch) {
            try {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("timed out");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        private static void join(Thread thread) {
            try {
                thread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            if (thread.isAlive()) {
                throw new IllegalStateException("timed out");
            }
        }
    }

    @Nested
    class WhenAReplayedInputCompletesTheInstance {

        private String quarantinedWithDoneAndLateSkipped() {
            deliver(placed());
            failing.add("poison");
            quarantineOn(added("poison"));
            deliver(added("done"));
            deliver(added("late"));
            failing.clear();
            return instance().quarantineId();
        }

        @Test
        void the_records_still_left_are_deleted_under_its_quarantine_id_only() {
            String quarantineId = quarantinedWithDoneAndLateSkipped();
            CloudEvent ghost = added("ghost");
            recordUnder(ORDER, "later", new SkippedInput(0, streamKey(ghost), ghost));

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(items()).containsExactly("poison", "done"),
                    () -> assertThat(store.delegate.findSkippedInputs(ORDER, quarantineId, 10)).isEmpty(),
                    () -> assertThat(store.delegate.findSkippedInputs(ORDER, "later", 10)).containsExactly(new SkippedInput(0, streamKey(ghost), ghost))
            );
        }

    }

    @Nested
    class WhenTheStoreDoesNotImplementTheSkippedInputsCapability {

        @Test
        void a_release_is_refused_with_a_message_that_names_the_capability() {
            SagaExecution<OrderEvent, Order, OrderCommand> withoutRecords = new SagaExecution<>("orders", saga(), new StoreWithoutSkippedInputs(),
                    dispatched::add, converter, CONFIG, now::get);

            assertThatThrownBy(() -> withoutRecords.release(ORDER))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("SagaStateStoreSkippedInputs");
        }
    }

    @Nested
    class WhenTheStoreFailsWhileRecordingTheFailureOfAReleasedInput {

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

        // The input "a" fails during the release, and the store fails on its first call after the reaction threw, which is
        // the first call that records the failure
        private final AtomicReference<@Nullable ReleaseResult> result = new AtomicReference<>();

        private Throwable releaseWhoseFailureCannotBeRecordedBecause(Runnable storeFails) {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            now.set(start.plus(Duration.ofHours(1)));
            failing.clear();
            failing.add("a");
            beforeTheNextFailure.set(storeFails);
            return catchThrowable(() -> result.set(execution.release(ORDER)));
        }

        private List<String> suppressedOnTheFailureOfTheReleasedInput() {
            return appender.list.stream()
                    .filter(e -> e.getFormattedMessage().contains("It stays releasing, and the timer poller tries the event again"))
                    .flatMap(e -> Arrays.stream(((ThrowableProxy) e.getThrowableProxy()).getSuppressed()))
                    .map(IThrowableProxy::getMessage)
                    .toList();
        }

        @Test
        void the_release_is_still_releasing_and_the_failure_of_the_record_read_is_suppressed_on_the_failure_of_the_input() {
            Throwable thrown = releaseWhoseFailureCannotBeRecordedBecause(() -> store.findSkippedInputsFailure = new IllegalStateException("cannot read the records"));

            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(result).hasValue(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(suppressedOnTheFailureOfTheReleasedInput()).containsExactly("cannot read the records"),
                    () -> assertThat(instance().isReleasing()).isTrue()
            );
        }

        @Test
        void the_release_is_still_releasing_and_the_failure_of_the_write_is_suppressed_on_the_failure_of_the_input() {
            Throwable thrown = releaseWhoseFailureCannotBeRecordedBecause(() -> store.compareAndSaveWithoutStateFailure = new IllegalStateException("cannot write"));

            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(result).hasValue(ReleaseResult.STILL_RELEASING),
                    () -> assertThat(suppressedOnTheFailureOfTheReleasedInput()).containsExactly("cannot write"),
                    () -> assertThat(instance().isReleasing()).isTrue()
            );
        }
    }

    @Nested
    class WhenAReleasingInstanceHasNoQuarantineId {

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
        void the_poller_refuses_to_go_on_with_it_and_leaves_it_as_it_is() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            SagaEnvelope<Order> quarantined = instance();
            SagaFailure failure = quarantined.failure();
            SagaFailure releasingWithoutId = new SagaFailure(failure.input(), failure.position(), failure.firstFailedAt(), failure.failureType(),
                    failure.failureMessage(), now.get(), null);
            SagaEnvelope<Order> stored = new SagaEnvelope<>(quarantined.sagaId(), quarantined.state(), quarantined.status(), quarantined.version() + 1, quarantined.timers(),
                    quarantined.streamWatermarks(), quarantined.positionWatermark(), quarantined.createdAt(), quarantined.updatedAt(), quarantined.completedAt(),
                    quarantined.currentStep(), quarantined.started(), releasingWithoutId, quarantined.streamMarks(), quarantined.quarantineSkipCount());
            assertThat(store.envelopes.compareAndSave(ORDER, stored, quarantined.version())).isTrue();

            pollAfterTheInterval();

            List<String> refusals = appender.list.stream()
                    .filter(e -> e.getFormattedMessage().contains("could not go on with the release"))
                    .map(e -> e.getThrowableProxy().getMessage())
                    .toList();
            assertAll(
                    () -> assertThat(refusals).singleElement().asString().contains("is releasing with no quarantine id"),
                    () -> assertThat(instance().version()).isEqualTo(stored.version()),
                    () -> assertThat(instance().isReleasing()).isTrue()
            );
        }
    }

    @Nested
    class WhenAStreamEventArrivesFurtherBehindThanTheReorderWindow {

        private ListAppender<ILoggingEvent> appender;
        private Logger executionLog;
        private @Nullable Level originalLevel;

        @BeforeEach
        void startCapturing() {
            execution = execution(CONFIG.withReorderWindow(1));
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

        // The stream holds placed, late, "b", "c" and the poison event. The instance applies placed, "b" and "c", so it
        // remembers version 4 of the type and "late" is two behind it, further than a window of 1.
        private CloudEvent quarantinedWithAnEventBehindTheWindow() {
            CloudEvent placed = placed();
            CloudEvent late = added("late");
            CloudEvent b = added("b");
            CloudEvent c = added("c");
            failing.add("poison");
            CloudEvent poison = added("poison");
            deliver(placed);
            deliver(b);
            deliver(c);
            quarantineOn(poison);
            return late;
        }

        private List<String> behindTheWindowWarnings() {
            return appender.list.stream()
                    .filter(e -> e.getLevel() == Level.WARN)
                    .map(ILoggingEvent::getFormattedMessage)
                    .filter(m -> m.contains("further behind than the instance remembers"))
                    .toList();
        }

        @Test
        void a_quarantined_instance_with_a_quarantine_id_warns_once_and_records_no_skipped_input_for_it() {
            CloudEvent late = quarantinedWithAnEventBehindTheWindow();
            List<String> recordedBefore = recordedKeys();
            SagaEnvelope<Order> before = instance();

            deliver(late);
            deliver(late);

            List<String> warnings = behindTheWindowWarnings();
            assertAll(
                    () -> assertThat(before.quarantineId()).isNotNull(),
                    () -> assertThat(warnings).hasSize(1),
                    () -> assertThat(warnings.getFirst()).contains("Saga 'o1' skipped version 2 of stream 'o1'"),
                    () -> assertThat(recordedKeys()).isEqualTo(recordedBefore),
                    () -> assertThat(instance()).isEqualTo(before)
            );
        }

        @Test
        void the_same_holds_while_the_state_of_the_instance_no_longer_decodes() {
            CloudEvent late = quarantinedWithAnEventBehindTheWindow();
            List<String> recordedBefore = recordedKeys();
            SagaEnvelope<Order> before = instance();
            store.refusesToDecode = true;

            Throwable thrown = catchThrowable(() -> {
                deliver(late);
                deliver(late);
            });

            store.refusesToDecode = false;
            List<String> warnings = behindTheWindowWarnings();
            assertAll(
                    () -> assertThat(thrown).isNull(),
                    () -> assertThat(warnings).hasSize(1),
                    () -> assertThat(warnings.getFirst()).contains("Saga 'o1' skipped version 2 of stream 'o1'"),
                    () -> assertThat(recordedKeys()).isEqualTo(recordedBefore),
                    () -> assertThat(instance()).isEqualTo(before)
            );
        }

        @Test
        void an_instance_quarantined_by_034_with_no_quarantine_id_does_not_warn() {
            CloudEvent late = quarantinedWithAnEventBehindTheWindow();
            SagaEnvelope<Order> quarantined = instance();
            SagaFailure failure034 = new SagaFailure(quarantined.failure().input(), 1L, start, IllegalStateException.class.getName(), "boom", null, null);
            SagaEnvelope<Order> by034 = new SagaEnvelope<>(ORDER, quarantined.state(), SagaStatus.QUARANTINED, quarantined.version() + 1, List.of(),
                    quarantined.streamWatermarks(), null, start, start, null, null, true, failure034, quarantined.streamMarks(), null);
            store.envelopes.compareAndSave(ORDER, by034, quarantined.version());

            deliver(late);

            assertAll(
                    () -> assertThat(instance().quarantineId()).isNull(),
                    () -> assertThat(behindTheWindowWarnings()).isEmpty(),
                    () -> assertThat(instance()).isEqualTo(by034)
            );
        }
    }

    @Nested
    class ThroughTheSagaSubscription {

        private final InMemorySubscriptionModel subscriptionModel = new InMemorySubscriptionModel();
        private final List<SagaSubscription> subscriptions = new ArrayList<>();

        @AfterEach
        void close() {
            subscriptions.forEach(SagaSubscription::close);
            subscriptionModel.shutdown();
        }

        private SagaSubscription run(SagaRunner<OrderEvent, OrderCommand> runner) {
            SagaSubscription subscription = runner.run("orders", saga(), store, dispatched::add, null, CONFIG);
            subscriptions.add(subscription);
            return subscription;
        }

        @Test
        void release_reaches_the_subscription_of_the_runner() {
            SagaSubscription subscription = run(SagaRunner.<OrderEvent, OrderCommand>agnostic(subscriptionModel, converter));

            assertThat(subscription.release("unknown")).isEqualTo(ReleaseResult.NOT_FOUND);
        }

        @Test
        void the_timer_poller_holds_back_the_release_while_timers_are_disabled_and_finishes_it_once_they_are_enabled() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            failing.add("a");
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
            failing.clear();
            AtomicBoolean timersEnabled = new AtomicBoolean(false);
            SagaRunner<OrderEvent, OrderCommand> runner = SagaRunner.<OrderEvent, OrderCommand>agnostic(subscriptionModel, converter);
            SagaSubscription subscription = runner.run("orders", saga(), store, dispatched::add, null,
                    CONFIG.withTimerPollInterval(Duration.ofMillis(20)), timersEnabled::get);
            subscriptions.add(subscription);

            // Many poll intervals elapse with the gate closed, and none of them may continue the release
            org.awaitility.Awaitility.await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertAll(
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(packed()).extracting(Pack::item).containsExactly("poison")
            ));

            timersEnabled.set(true);

            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertAll(
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(packed()).extracting(Pack::item).containsExactly("poison", "a", "b")
            ));
        }
    }

    private static List<String> appended(@Nullable List<String> list, String... keys) {
        List<String> appended = new ArrayList<>(list == null ? List.of() : list);
        appended.addAll(List.of(keys));
        return appended;
    }

    // The in-memory store without the optional SagaStateStoreSkippedInputs capability
    private static final class StoreWithoutSkippedInputs implements SagaStateStore<Order> {
        private final SagaStateStore<Order> delegate = SagaStateStore.inMemory();

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
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }
    }

    /**
     * An in-memory store that runs a hook just before its next write, which is how a test puts another write between
     * a read and the write that follows it, and records every write it accepts.
     */
    private static final class InterceptingStore implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
        final InMemorySagaStateStore<Order> delegate = new InMemorySagaStateStore<>();
        private final SagaCodec<Order> codec = SagaCodec.of(delegate.stateCodec(), null);
        final SagaEnvelopes<Order> envelopes = SagaEnvelopes.of(delegate, null);
        final List<SagaEnvelope<Order>> accepted = new CopyOnWriteArrayList<>();
        private final AtomicReference<@Nullable Runnable> beforeNextStateWrite = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> beforeNextWriteWithoutState = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> afterNextWriteWithoutState = new AtomicReference<>();
        final AtomicInteger writesWithoutState = new AtomicInteger();
        private static final int MAX_RECORD_READS = 500;
        private final AtomicInteger recordReads = new AtomicInteger();
        private final AtomicReference<@Nullable Runnable> beforeNextRecord = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> afterNextRecord = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> beforeNextDeleteOfARecord = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> afterNextEmptyRead = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> afterNextNonEmptyRead = new AtomicReference<>();
        private final AtomicInteger nonEmptyReadsToLetPass = new AtomicInteger();
        final List<String> deletedRecords = new CopyOnWriteArrayList<>();
        private final AtomicReference<@Nullable Runnable> beforeNextActiveWrite = new AtomicReference<>();
        volatile boolean refusesToDecode;
        volatile @Nullable RuntimeException findFailure;
        volatile @Nullable RuntimeException findWithoutStateFailure;
        volatile @Nullable RuntimeException listFailure;
        volatile @Nullable String findFailsFor;
        volatile @Nullable String findRunsOutOfMemoryFor;
        volatile @Nullable RuntimeException findSkippedInputsFailure;
        volatile @Nullable RuntimeException compareAndSaveWithoutStateFailure;
        private final AtomicReference<@Nullable Runnable> beforeNextFindWithoutState = new AtomicReference<>();
        volatile boolean listsEveryReleasingInstanceWhateverTheLimit;

        void beforeNextRecord(Runnable hook) {
            beforeNextRecord.set(hook);
        }

        void afterNextRecord(Runnable hook) {
            afterNextRecord.set(hook);
        }

        void beforeNextDeleteOfARecord(Runnable hook) {
            beforeNextDeleteOfARecord.set(hook);
        }

        void afterNextEmptyRead(Runnable hook) {
            afterNextEmptyRead.set(hook);
        }

        // The hook runs after the first non-empty read that the replay makes. The read before it, which a release makes to
        // check that the instance holds the input it was quarantined on before it starts, is let pass
        void afterNextNonEmptyRead(Runnable hook) {
            nonEmptyReadsToLetPass.set(1);
            afterNextNonEmptyRead.set(hook);
        }

        void beforeNextFindWithoutState(Runnable hook) {
            beforeNextFindWithoutState.set(hook);
        }

        void beforeNextActiveWrite(Runnable hook) {
            beforeNextActiveWrite.set(hook);
        }

        private static void run(AtomicReference<@Nullable Runnable> hook) {
            Runnable next = hook.getAndSet(null);
            if (next != null) {
                next.run();
            }
        }

        void beforeNextStateWrite(Runnable hook) {
            beforeNextStateWrite.set(hook);
        }

        void beforeNextWriteWithoutState(Runnable hook) {
            beforeNextWriteWithoutState.set(hook);
        }

        void afterNextWriteWithoutState(Runnable hook) {
            afterNextWriteWithoutState.set(hook);
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
            failsTheInstanceReadIfAsked(sagaId);
            return delegate.find(sagaId);
        }

        private void failsTheInstanceReadIfAsked(String sagaId) {
            RuntimeException failure = findFailure;
            if (failure != null) {
                throw failure;
            }
            if (refusesToDecode) {
                throw new IllegalStateException("the state can no longer be decoded");
            }
            if (sagaId.equals(findFailsFor)) {
                throw new IllegalStateException("cannot read " + sagaId);
            }
            if (sagaId.equals(findRunsOutOfMemoryFor)) {
                throw new OutOfMemoryError("Java heap space");
            }
        }

        @Override
        public Optional<StoredSaga> findWithoutState(String sagaId) {
            RuntimeException failure = findWithoutStateFailure;
            if (failure != null) {
                throw failure;
            }
            run(beforeNextFindWithoutState);
            return delegate.findWithoutState(sagaId);
        }

        @Override
        public boolean compareAndSave(StoredSaga saga, long expectedVersion) {
            Runnable hook = beforeNextStateWrite.getAndSet(null);
            if (hook != null) {
                hook.run();
            }
            boolean saved = delegate.compareAndSave(saga, expectedVersion);
            if (saved) {
                accepted.add(codec.decode(saga));
            }
            return saved;
        }

        @Override
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            writesWithoutState.incrementAndGet();
            SagaEnvelope<Order> envelope = codec.decodeWithoutState(saga);
            if (envelope.status() == SagaStatus.ACTIVE) {
                run(beforeNextActiveWrite);
            }
            run(beforeNextWriteWithoutState);
            RuntimeException failure = compareAndSaveWithoutStateFailure;
            if (failure != null) {
                throw failure;
            }
            boolean saved = delegate.compareAndSaveWithoutState(saga, expectedVersion);
            if (saved) {
                accepted.add(envelope);
                run(afterNextWriteWithoutState);
            }
            return saved;
        }

        @Override
        public List<StoredSaga> findWithDueTimers(Instant now, int limit) {
            return delegate.findWithDueTimers(now, limit);
        }

        @Override
        public List<String> findReleasingSagaIds(@Nullable String after, int limit) {
            RuntimeException failure = listFailure;
            if (failure != null) {
                throw failure;
            }
            return delegate.findReleasingSagaIds(after, listsEveryReleasingInstanceWhateverTheLimit ? Integer.MAX_VALUE : limit);
        }

        private boolean skipWrite(StoredSaga saga, Supplier<Boolean> write) {
            writesWithoutState.incrementAndGet();
            SagaEnvelope<Order> envelope = codec.decodeWithoutState(saga);
            if (envelope.status() == SagaStatus.ACTIVE) {
                run(beforeNextActiveWrite);
            }
            run(beforeNextRecord);
            run(beforeNextWriteWithoutState);
            boolean saved = write.get();
            if (saved) {
                accepted.add(envelope);
                run(afterNextRecord);
                run(afterNextWriteWithoutState);
            }
            return saved;
        }

        @Override
        public boolean compareAndSaveSkipped(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput input) {
            return skipWrite(saga, () -> delegate.compareAndSaveSkipped(saga, expectedVersion, quarantineId, input));
        }

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            run(beforeNextStateWrite);
            run(beforeNextDeleteOfARecord);
            boolean saved = delegate.compareAndSaveReplayed(saga, expectedVersion, quarantineId, replayed);
            if (saved) {
                accepted.add(codec.decode(saga));
                deletedRecords.add(quarantineId + "/" + replayed.key());
            }
            return saved;
        }

        @Override
        public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
            if (codec.decodeWithoutState(saga).status() == SagaStatus.ACTIVE) {
                run(beforeNextActiveWrite);
            }
            run(beforeNextStateWrite);
            boolean saved = delegate.compareAndSaveQuarantineEnded(saga, expectedVersion, quarantineId);
            if (saved) {
                accepted.add(codec.decode(saga));
            }
            return saved;
        }

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            return delegate.findSkippedInputs(sagaId, quarantineId, limit);
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInput> findWithFirstSkippedInput(String sagaId) {
            failsTheInstanceReadIfAsked(sagaId);
            RuntimeException failure = findSkippedInputsFailure;
            if (failure != null) {
                throw failure;
            }
            if (recordReads.incrementAndGet() > MAX_RECORD_READS) {
                // A release that reads a record it cannot delete reads it again, and a test that waits for that never ends
                throw new AssertionError("The release read the records more than " + MAX_RECORD_READS + " times, so it is not getting anywhere");
            }
            Optional<StoredSagaWithFirstSkippedInput> found = delegate.findWithFirstSkippedInput(sagaId);
            boolean noRecord = found.map(read -> read.firstSkippedInput() == null).orElse(true);
            if (noRecord) {
                run(afterNextEmptyRead);
            } else if (nonEmptyReadsToLetPass.get() > 0 && afterNextNonEmptyRead.get() != null) {
                nonEmptyReadsToLetPass.decrementAndGet();
            } else {
                run(afterNextNonEmptyRead);
            }
            return found;
        }

        // Fails as findWithFirstSkippedInput does when the store is down, but not on a state that refuses to decode, since
        // the read leaves the state out
        @Override
        public Optional<StoredSagaWithFirstSkippedInputKey> findWithFirstSkippedInputKey(String sagaId) {
            RuntimeException failure = findFailure;
            if (failure != null) {
                throw failure;
            }
            if (sagaId.equals(findFailsFor)) {
                throw new IllegalStateException("cannot read " + sagaId);
            }
            if (sagaId.equals(findRunsOutOfMemoryFor)) {
                throw new OutOfMemoryError("Java heap space");
            }
            RuntimeException recordsFailure = findSkippedInputsFailure;
            if (recordsFailure != null) {
                throw recordsFailure;
            }
            return delegate.findWithFirstSkippedInputKey(sagaId);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }
    }
}
