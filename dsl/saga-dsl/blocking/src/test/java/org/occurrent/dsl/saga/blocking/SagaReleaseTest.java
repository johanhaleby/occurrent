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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.*;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
import org.occurrent.dsl.saga.internal.SkippedInputKey;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Releasing a quarantined saga instance, <a href="https://github.com/johanhaleby/occurrent/issues/1259">#1259</a>
 * and ADR 178. A quarantined instance records the key of every input it skips, and a release replays the input it was
 * quarantined on and then those, read back from the event store. Driven through {@link SagaExecution} with a clock and
 * a state store the test controls, so every interleaving is deterministic.
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
    private final EventStoreReads queries = new EventStoreReads(eventStore);
    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final InterceptingStore store = new InterceptingStore();
    private final List<OrderCommand> dispatched = new CopyOnWriteArrayList<>();
    private final AtomicInteger eventIds = new AtomicInteger();
    private SagaExecution<OrderEvent, Order, OrderCommand> execution = execution(CONFIG);

    private SagaExecution<OrderEvent, Order, OrderCommand> execution(SagaRunnerConfig config) {
        return new SagaExecution<>("orders", saga(), store, dispatched::add, converter, config, event -> true, now::get, queries);
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
        return store.delegate.find(ORDER).orElseThrow();
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
        void the_quarantined_instance_records_each_skipped_input_by_its_key_and_moves_no_watermark() {
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
                    () -> assertThat(recordedKeys()).containsExactly(cloudEventKey(a), streamKey(b)),
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
        void an_instance_quarantined_on_its_start_event_records_what_it_skips_and_starts_when_it_is_released() {
            failing.add("placed");
            quarantineOn(placed());
            CloudEvent a = added("a");
            deliver(a);
            assertThat(recordedKeys()).containsExactly(streamKey(a));
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

            execution.pollTimers();
            boolean remindedWhileReleasing = dispatched.contains(new Remind(ORDER));
            failing.clear();
            // This poll goes on with the release, and the next fires what came due
            execution.pollTimers();
            execution.pollTimers();

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

            execution.pollTimers();

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
                    () -> assertThat(appliedStates).containsExactly(List.of("poison"), List.of("poison", "a"), List.of("poison", "a", "b"))
            );
        }
    }

    @Nested
    class WhenAnEventToReplayIsMissingFromTheStore {

        @Test
        void the_input_it_was_quarantined_on_quarantines_it_again_at_once_keeping_its_position_and_every_record() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoSkipped();
            Long position = instance().failure().position();
            queries.hide(quarantined.k());
            failing.clear();
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);

            ReleaseResult result = execution.release(ORDER);

            SagaEnvelope<Order> requarantined = instance();
            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_EVENT_MISSING),
                    () -> assertThat(position).isNotNull(),
                    () -> assertThat(requarantined.status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(requarantined.isReleasing()).isFalse(),
                    () -> assertThat(requarantined.failure().input()).isEqualTo(streamKey(quarantined.k())),
                    () -> assertThat(requarantined.failure().position()).isEqualTo(position),
                    () -> assertThat(requarantined.failure().failureType()).isEqualTo("EventMissingFromStore"),
                    () -> assertThat(requarantined.failure().failureMessage()).contains(streamKey(quarantined.k())),
                    () -> assertThat(requarantined.failure().firstFailedAt()).isEqualTo(releasedAt),
                    () -> assertThat(recordedKeys()).containsExactly(streamKey(quarantined.k()), cloudEventKey(quarantined.a()), streamKey(quarantined.b())),
                    () -> assertThat(requarantined.state().items()).isEmpty(),
                    () -> assertThat(packed()).isEmpty()
            );
        }

        @Test
        void a_recorded_input_quarantines_it_again_at_once_with_no_position_and_keeps_its_record() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoSkipped();
            queries.hide(quarantined.a());
            failing.clear();

            ReleaseResult result = execution.release(ORDER);

            SagaEnvelope<Order> requarantined = instance();
            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_EVENT_MISSING),
                    () -> assertThat(requarantined.status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(requarantined.failure().releasedAt()).isNull(),
                    () -> assertThat(requarantined.failure().input()).isEqualTo(cloudEventKey(quarantined.a())),
                    () -> assertThat(requarantined.failure().position()).isNull(),
                    () -> assertThat(requarantined.failure().failureType()).isEqualTo("EventMissingFromStore"),
                    () -> assertThat(requarantined.failure().failureMessage()).contains(cloudEventKey(quarantined.a())),
                    () -> assertThat(recordedKeys()).containsExactly(cloudEventKey(quarantined.a()), streamKey(quarantined.b())),
                    () -> assertThat(requarantined.state().items()).containsExactly("poison")
            );
        }

        @Test
        void a_later_release_completes_once_the_event_is_back() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoSkipped();
            queries.hide(quarantined.a());
            failing.clear();
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.QUARANTINED_EVENT_MISSING);
            queries.restore(quarantined.a());

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).containsExactly("poison", "a", "b")
            );
        }

        @Test
        void a_read_that_throws_makes_the_release_throw_and_leaves_the_instance_releasing_for_the_poller() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoSkipped();
            failing.clear();
            IllegalStateException storeDown = new IllegalStateException("event store is down");
            queries.failReadsWith(storeDown);

            Throwable thrown = catchThrowable(() -> execution.release(ORDER));
            SagaEnvelope<Order> afterTheFailedRead = instance();
            List<String> recordsAfterTheFailedRead = recordedKeys();
            queries.failReadsWith(null);
            execution.pollTimers();

            assertAll(
                    () -> assertThat(thrown).isSameAs(storeDown),
                    () -> assertThat(afterTheFailedRead.isReleasing()).isTrue(),
                    () -> assertThat(recordsAfterTheFailedRead).first().isEqualTo(streamKey(quarantined.k())),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).containsExactly("poison", "a", "b")
            );
        }
    }

    @Nested
    class WhenTheReleaseIsRefused {

        @Test
        void an_instance_quarantined_by_0_34_0_with_no_quarantine_id_is_refused_naming_delete_as_the_only_way_out() {
            SagaFailure failure = new SagaFailure("o1@2", 7L, start, IllegalStateException.class.getName(), "boom");
            store.delegate.compareAndSave(ORDER, new SagaEnvelope<>(ORDER, new Order(ORDER, List.of()), SagaStatus.QUARANTINED, 1, List.of(),
                    Map.of(ORDER, 1L), null, start, start, null, null, true, failure), 0);

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
        void a_runner_without_event_store_queries_is_refused_naming_how_to_give_it_one() {
            SagaExecution<OrderEvent, Order, OrderCommand> withoutQueries = new SagaExecution<>("orders", saga(), store, dispatched::add, converter,
                    CONFIG, event -> true, now::get);
            execution = withoutQueries;
            quarantinedOnPoisonWithTwoSkipped();
            failing.clear();

            Throwable thrown = catchThrowable(() -> withoutQueries.release(ORDER));

            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("EventStoreQueries")
                            .hasMessageContaining("SagaRunner.eventStoreQueries(..)"),
                    () -> assertThat(instance().isReleasing()).isFalse(),
                    () -> assertThat(recordedKeys()).hasSize(2)
            );
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

            execution.pollTimers();

            assertAll(
                    () -> assertThat(warnings()).anyMatch(m -> m.contains("could not go on with the release of instance 'o1'")),
                    () -> assertThat(warnings()).noneMatch(m -> m.contains("could not list the instances it is releasing")),
                    () -> assertThat(store.delegate.find("o1").orElseThrow().state().items()).isEmpty(),
                    () -> assertThat(store.delegate.find("o2").orElseThrow().state().items()).containsExactly("poison", "b1"),
                    () -> assertThat(store.delegate.find("o2").orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE)
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

        @Test
        void a_store_that_forgets_the_release_time_it_was_given_is_still_refused_naming_what_it_did_not_persist() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            now.set(start.plus(Duration.ofHours(1)));
            store.dropsTheReleaseTime = true;

            assertThatThrownBy(() -> execution.release(ORDER))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("did not persist SagaFailure.releasedAt");
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
            SagaEnvelope<Order> before = store.delegate.findWithoutState(ORDER).orElseThrow();
            store.refusesToDecode = true;

            Throwable thrown = catchThrowable(() -> execution.release(ORDER));

            store.refusesToDecode = false;
            assertAll(
                    () -> assertThat(thrown).isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("decoding its state failed with java.lang.IllegalStateException: the state can no longer be decoded")
                            .hasMessageContaining("SagaStateStore.delete(\"" + ORDER + "\")"),
                    () -> assertThat(thrown.getCause()).isInstanceOf(IllegalStateException.class).hasMessage("the state can no longer be decoded"),
                    () -> assertThat(store.delegate.findWithoutState(ORDER).orElseThrow()).isEqualTo(before),
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

    // A release that quarantines the instance again leaves the record it replayed, and the next release moves it to
    // the head. Were it to delete the record after its write, a second release that started in between and wrote
    // the same input again as its head would lose it. The hook stands for that delete, so it only runs when the
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

    // The next release starts by deleting the head's record by key, which would run a hook still armed, so it is disarmed
    private void assertEveryInputIsAppliedByTheNextRelease(SagaExecution<OrderEvent, Order, OrderCommand> node, ReleaseResult firstResult, ReleaseResult expectedFirst, CloudEvent... hidden) {
        store.beforeNextDeleteOfARecord(() -> {
        });
        failing.clear();
        for (CloudEvent event : hidden) {
            queries.restore(event);
        }
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
    class WhenARecordAfterTheHeadIsLeftByTheReleaseThatQuarantinedTheInstanceAgain {

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

        @Test
        void an_input_missing_from_the_store_is_applied_by_the_release_that_goes_active() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            queries.hide(quarantined.a());
            SagaExecution<OrderEvent, Order, OrderCommand> secondNode = execution(CONFIG);
            onADeleteOnceQuarantinedAgain(quarantinedAgainOn(streamKey(quarantined.a())), () -> {
                queries.restore(quarantined.a());
                failing.add("a");
                secondNode.release(ORDER);
            });

            ReleaseResult firstResult = execution.release(ORDER);

            assertEveryInputIsAppliedByTheNextRelease(secondNode, firstResult, ReleaseResult.QUARANTINED_EVENT_MISSING, quarantined.a());
        }

        /**
         * The record is under a CloudEvent key and the failure names the stream key the event was read back with, so the
         * two do not match by key. The next release applies the input from the failure, and the record left under the
         * other key is only deleted, which is the second apply that would show an input applied twice.
         */
        @Test
        void an_input_skipped_under_a_cloud_event_key_that_failed_again_is_applied_once_and_its_leftover_record_deleted() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoSkipped();
            Instant releasedAt = start.plus(Duration.ofHours(1));
            now.set(releasedAt);
            failing.clear();
            failing.add("a");
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
            now.set(releasedAt.plus(BUDGET));
            ReleaseResult firstResult = execution.release(ORDER);
            SagaEnvelope<Order> requarantined = instance();
            List<String> leftOver = recordedKeys();
            String quarantineId = requarantined.quarantineId();
            failing.clear();

            ReleaseResult last = execution.release(ORDER);

            assertAll(
                    () -> assertThat(firstResult).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(requarantined.failure().input()).isEqualTo(streamKey(quarantined.a())),
                    () -> assertThat(leftOver).contains(cloudEventKey(quarantined.a())),
                    () -> assertThat(last).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).containsExactly("poison", "a", "b"),
                    () -> assertThat(recordedKeysOf(quarantineId)).isEmpty()
            );
        }
    }

    @Nested
    class WhenTheHeadRecordIsLeftByTheReleaseThatQuarantinedTheInstanceAgain {

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

        @Test
        void the_input_the_instance_was_quarantined_on_that_is_missing_from_the_store_is_applied_by_the_release_that_goes_active() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            queries.hide(quarantined.k());
            SagaExecution<OrderEvent, Order, OrderCommand> secondNode = execution(CONFIG);
            // Not asked of the instance quarantined on it, which is what the release starts from
            onADeleteOnceQuarantinedAgain(envelope -> !envelope.isReleasing() && "EventMissingFromStore".equals(envelope.failure().failureType()), () -> {
                queries.restore(quarantined.k());
                failing.add("poison");
                secondNode.release(ORDER);
            });

            ReleaseResult firstResult = execution.release(ORDER);

            assertEveryInputIsAppliedByTheNextRelease(secondNode, firstResult, ReleaseResult.QUARANTINED_EVENT_MISSING, quarantined.k());
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
            return store.delegate.find(orderId).orElseThrow();
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

            execution.pollTimers();

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
            execution.pollTimers();

            execution.pollTimers();
            List<String> afterTheSecondPoll = itemsOf("o1");
            boolean releasingAfterTheSecondPoll = instanceOf("o1").isReleasing();
            execution.pollTimers();

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

            execution.pollTimers();

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

            execution.pollTimers();

            assertAll(
                    () -> assertThat(itemsOf("o1")).containsExactly("poison"),
                    () -> assertThat(itemsOf("o6")).containsExactly("poison")
            );
        }

        @Test
        void turns_the_instance_active_in_the_poll_whose_share_its_last_record_uses_up() {
            releasingOnPoison("o1", "a1", "a2", "a3");
            failing.clear();

            execution.pollTimers();

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
                execution.pollTimers();
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
            execution.pollTimers();
            execution.pollTimers();

            execution.pollTimers();

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
                    () -> assertThat(records()).extracting(SkippedInput::key).containsExactly(streamKey(quarantined.a()), streamKey(quarantined.b()), streamKey(c)),
                    () -> assertThat(records()).extracting(SkippedInput::seq).doesNotHaveDuplicates().isSorted(),
                    () -> assertThat(instance().quarantineSkipCount()).isEqualTo(3L)
            );
        }

        @Test
        void a_skip_whose_record_was_never_written_leaves_a_gap_and_the_inputs_still_replay_in_the_order_they_arrived_across_streams() {
            deliver(placed());
            failing.add("poison");
            quarantineOn(added("poison"));
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
                    () -> assertThat(records).extracting(SkippedInput::key).containsExactly(streamKey(y), streamKey(x)),
                    () -> assertThat(records).extracting(SkippedInput::seq).doesNotHaveDuplicates().isSorted(),
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).containsExactly("poison", "y", "x")
            );
        }

        @Test
        void a_skip_whose_record_was_written_but_not_finished_keeps_its_place_when_it_is_offered_again_after_a_later_input() {
            deliver(placed());
            failing.add("poison");
            quarantineOn(added("poison"));
            CloudEvent x = appendOn("o1-other", new ItemAdded(nextEventId(), ORDER, "x"));
            store.afterNextRecord(() -> {
                throw new IllegalStateException("crash after the record and before the version");
            });
            assertThatThrownBy(() -> deliver(x)).hasMessage("crash after the record and before the version");
            CloudEvent y = added("y");
            deliver(y);
            SkippedInput recordedFirst = records().getFirst();

            deliver(x);

            List<SkippedInput> records = records();
            failing.clear();
            ReleaseResult result = execution.release(ORDER);
            assertAll(
                    () -> assertThat(recordedFirst.key()).isEqualTo(streamKey(x)),
                    () -> assertThat(records).extracting(SkippedInput::key).containsExactly(streamKey(x), streamKey(y)),
                    () -> assertThat(records.getFirst()).isEqualTo(recordedFirst),
                    () -> assertThat(records).extracting(SkippedInput::seq).doesNotHaveDuplicates().isSorted(),
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).containsExactly("poison", "x", "y")
            );
        }

        @Test
        void a_skip_whose_last_write_loses_to_another_skip_keeps_its_record_and_its_place() {
            deliver(placed());
            failing.add("poison");
            quarantineOn(added("poison"));
            CloudEvent x = appendOn("o1-other", new ItemAdded(nextEventId(), ORDER, "x"));
            CloudEvent y = added("y");
            // y is skipped in full between x's record and x's last write, so x's last write loses while the instance stays quarantined
            store.afterNextRecord(() -> deliver(y));

            deliver(x);

            List<SkippedInput> records = records();
            failing.clear();
            ReleaseResult result = execution.release(ORDER);
            assertAll(
                    () -> assertThat(records).extracting(SkippedInput::key).containsExactly(streamKey(x), streamKey(y)),
                    () -> assertThat(records).extracting(SkippedInput::seq).doesNotHaveDuplicates().isSorted(),
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(items()).containsExactly("poison", "x", "y")
            );
        }

        @Test
        void a_redelivered_skip_of_a_recorded_key_raises_the_count_keeps_the_first_record_and_its_number_and_changes_the_version_twice() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            SagaEnvelope<Order> before = instance();
            List<SkippedInput> recordsBefore = records();

            deliver(quarantined.b());

            assertAll(
                    () -> assertThat(records()).isEqualTo(recordsBefore),
                    () -> assertThat(instance().quarantineSkipCount()).isEqualTo(before.quarantineSkipCount() + 1),
                    () -> assertThat(instance().version()).isEqualTo(before.version() + 2),
                    () -> assertThat(instance().streamWatermarks()).isEqualTo(before.streamWatermarks())
            );
        }

        @Test
        void a_record_left_under_an_earlier_quarantine_id_is_never_replayed() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            CloudEvent ghost = added("ghost");
            store.delegate.recordSkippedInput(ORDER, "earlier", new SkippedInput(0, streamKey(ghost)));
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
        void sets_the_skip_count_to_0_and_a_quarantine_id_when_an_active_instance_is_quarantined() {
            deliver(placed());
            failing.add("poison");

            quarantineOn(added("poison"));

            assertAll(
                    () -> assertThat(instance().quarantineSkipCount()).isZero(),
                    () -> assertThat(instance().quarantineId()).isNotBlank()
            );
        }
    }

    @Nested
    class AnInstanceQuarantinedBy034 {

        private SagaEnvelope<Order> quarantinedWithNoQuarantineId(@Nullable Long skipCount) {
            CloudEvent placed = placed();
            SagaFailure failure = new SagaFailure(streamKey(placed), 1L, start, IllegalStateException.class.getName(), "boom");
            SagaEnvelope<Order> envelope = new SagaEnvelope<>(ORDER, new Order(ORDER, List.of()), SagaStatus.QUARANTINED, 1, List.of(),
                    Map.of(ORDER, 1L), null, start, start, null, null, true, failure, Map.of(), skipCount);
            store.delegate.compareAndSave(ORDER, envelope, 0);
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
        void keeps_the_quarantine_id_and_the_skip_count_when_the_event_it_replays_is_missing() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoSkipped();
            String quarantineId = instance().quarantineId();
            Long skipCount = instance().quarantineSkipCount();
            queries.hide(quarantined.k());
            failing.clear();

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_EVENT_MISSING),
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
    class TheInputTheInstanceWasQuarantinedOn {

        @Test
        void is_recorded_before_the_release_starts_at_a_number_below_every_other_and_replaces_a_record_of_its_key_left_behind() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            store.delegate.recordSkippedInput(ORDER, instance().quarantineId(), new SkippedInput(5, streamKey(quarantined.k())));
            AtomicReference<List<SkippedInput>> whenTheReleaseStarts = new AtomicReference<>();
            store.beforeNextWriteWithoutState(() -> whenTheReleaseStarts.set(records()));

            execution.release(ORDER);

            assertAll(
                    () -> assertThat(whenTheReleaseStarts.get()).extracting(SkippedInput::key)
                            .containsExactly(streamKey(quarantined.k()), streamKey(quarantined.a()), streamKey(quarantined.b())),
                    () -> assertThat(whenTheReleaseStarts.get().getFirst().seq()).isEqualTo(-1L)
            );
        }

        @Test
        void is_applied_once_when_a_crash_between_its_write_and_its_delete_leaves_its_record_for_the_next_pass_to_delete() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
            failing.clear();
            store.beforeNextDeleteOfARecord(() -> {
                throw new IllegalStateException("crash after the write and before the delete");
            });
            assertThatThrownBy(() -> execution.release(ORDER)).hasMessage("crash after the write and before the delete");
            List<String> keysAfterTheCrash = recordedKeys();
            List<String> itemsAfterTheCrash = items();

            execution.pollTimers();

            assertAll(
                    () -> assertThat(itemsAfterTheCrash).containsExactly("poison"),
                    () -> assertThat(keysAfterTheCrash).hasSize(3),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(items()).containsExactly("poison", "a", "b"),
                    () -> assertThat(packed()).extracting(Pack::item).containsExactly("poison", "a", "b")
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

            execution.pollTimers();

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
            store.beforeNextRecord(execution::pollTimers);

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
            store.delegate.recordSkippedInput(ORDER, "later", new SkippedInput(0, "ghost@1"));

            ReleaseResult result = execution.release(ORDER);

            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(items()).containsExactly("poison", "done"),
                    () -> assertThat(store.delegate.findSkippedInputs(ORDER, quarantineId, 10)).isEmpty(),
                    () -> assertThat(store.delegate.findSkippedInputs(ORDER, "later", 10)).containsExactly(new SkippedInput(0, "ghost@1"))
            );
        }

    }

    @Nested
    class WhenAnEventReadBackHasNoStreamIdAndVersion {

        @Test
        void the_instance_is_quarantined_again_at_once_with_a_failure_type_of_its_own_and_its_record_is_kept() {
            Quarantined quarantined = quarantinedOnPoisonWithTwoSkipped();
            queries.stripStreamKeyFrom(quarantined.a());
            failing.clear();

            ReleaseResult result = execution.release(ORDER);

            SagaEnvelope<Order> requarantined = instance();
            assertAll(
                    () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                    () -> assertThat(requarantined.status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(requarantined.isReleasing()).isFalse(),
                    () -> assertThat(requarantined.failure().failureType()).isEqualTo("EventWithoutStreamKey"),
                    () -> assertThat(requarantined.failure().input()).isEqualTo(cloudEventKey(quarantined.a())),
                    () -> assertThat(recordedKeys()).containsExactly(cloudEventKey(quarantined.a()), streamKey(quarantined.b())),
                    () -> assertThat(requarantined.state().items()).containsExactly("poison")
            );
        }
    }

    @Nested
    class WhenTheStoreDoesNotImplementTheSkippedInputsCapability {

        @Test
        void a_release_is_refused_with_a_message_that_names_the_capability() {
            SagaExecution<OrderEvent, Order, OrderCommand> withoutRecords = new SagaExecution<>("orders", saga(), new StoreWithoutSkippedInputs(),
                    dispatched::add, converter, CONFIG, event -> true, now::get, queries);

            assertThatThrownBy(() -> withoutRecords.release(ORDER))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("SagaStateStoreSkippedInputs");
        }
    }

    @Nested
    class WhenTheStoreDoesNotPersistTheQuarantineId {

        @Test
        void the_delivery_that_quarantines_the_instance_is_not_acknowledged() {
            deliver(placed());
            failing.add("poison");
            CloudEvent poison = added("poison");
            assertThatThrownBy(() -> deliver(poison)).isInstanceOf(IllegalStateException.class);
            store.dropsTheQuarantineId = true;
            now.set(now.get().plus(BUDGET));

            assertThatThrownBy(() -> deliver(poison))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("cannot pack poison");
        }

        @Test
        void every_redelivery_of_the_event_fails_and_the_instance_stays_out_of_quarantine() {
            deliver(placed());
            failing.add("poison");
            CloudEvent poison = added("poison");
            assertThatThrownBy(() -> deliver(poison)).isInstanceOf(IllegalStateException.class);
            store.dropsTheQuarantineId = true;
            now.set(now.get().plus(BUDGET));
            assertThatThrownBy(() -> deliver(poison))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("cannot pack poison");

            Throwable redelivery = catchThrowable(() -> deliver(poison));

            assertAll(
                    () -> assertThat(redelivery)
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("cannot pack poison"),
                    () -> assertThat(instance().status()).isNotEqualTo(SagaStatus.QUARANTINED)
            );
        }

        @Test
        void the_quarantine_is_undone_when_another_write_lands_before_the_undo() {
            deliver(placed());
            failing.add("poison");
            CloudEvent poison = added("poison");
            assertThatThrownBy(() -> deliver(poison)).isInstanceOf(IllegalStateException.class);
            store.dropsTheQuarantineId = true;
            now.set(now.get().plus(BUDGET));
            // The first write quarantines the instance. The write after it is the undo, which loses to a write from outside
            // the saga that leaves the instance quarantined without an id at the next version.
            store.beforeNextWriteWithoutState(() -> store.beforeNextWriteWithoutState(this::anotherWriteLeavesTheInstanceQuarantinedWithoutAnId));

            Throwable deliveryThatHitTheBudget = catchThrowable(() -> deliver(poison));
            Throwable redelivery = catchThrowable(() -> deliver(poison));

            assertAll(
                    () -> assertThat(deliveryThatHitTheBudget)
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("cannot pack poison"),
                    () -> assertThat(redelivery)
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("cannot pack poison"),
                    () -> assertThat(instance().status()).isNotEqualTo(SagaStatus.QUARANTINED)
            );
        }

        @Test
        void the_undo_gives_up_when_every_attempt_loses_to_a_write_from_outside_the_saga() {
            CloudEvent poison = pendingPoisonOnAStoreThatDropsTheQuarantineId();
            store.beforeNextWriteWithoutState(() -> store.beforeNextWriteWithoutState(() -> anotherWriteBeforeEachOfTheNext(CONFIG.maxCasAttempts())));

            Throwable delivery = catchThrowable(() -> deliver(poison));

            assertAll(
                    () -> assertThat(delivery)
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("cannot pack poison"),
                    () -> assertThat(delivery.getSuppressed()).isEmpty(),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(instance().quarantineId()).isNull(),
                    () -> assertThat(errors()).anyMatch(m -> m.contains("could not be undone because the instance was written by something else on each of "
                                                                         + CONFIG.maxCasAttempts() + " attempts")),
                    () -> assertThat(errors()).noneMatch(m -> m.contains("so the quarantine was undone"))
            );
        }

        @Test
        void the_undo_stops_and_recreates_nothing_when_the_instance_is_gone_after_a_lost_write() {
            CloudEvent poison = pendingPoisonOnAStoreThatDropsTheQuarantineId();
            store.beforeNextWriteWithoutState(() -> store.beforeNextWriteWithoutState(() -> {
                anotherWriteLeavesTheInstanceQuarantinedWithoutAnId();
                store.delegate.delete(ORDER);
            }));

            Throwable delivery = catchThrowable(() -> deliver(poison));

            assertAll(
                    () -> assertThat(delivery)
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("cannot pack poison"),
                    () -> assertThat(delivery.getSuppressed()).isEmpty(),
                    () -> assertThat(store.delegate.find(ORDER)).isEmpty(),
                    () -> assertThat(errors()).noneMatch(m -> m.contains("could not be undone"))
            );
        }

        @Test
        void the_undo_leaves_the_instance_as_it_is_when_a_lost_write_took_it_out_of_quarantine() {
            CloudEvent poison = pendingPoisonOnAStoreThatDropsTheQuarantineId();
            AtomicReference<SagaEnvelope<Order>> leftByTheOtherWrite = new AtomicReference<>();
            store.beforeNextWriteWithoutState(() -> store.beforeNextWriteWithoutState(() -> {
                anotherWriteLeaves(SagaStatus.ACTIVE, null, null);
                leftByTheOtherWrite.set(instance());
            }));

            Throwable delivery = catchThrowable(() -> deliver(poison));

            assertAll(
                    () -> assertThat(delivery)
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("cannot pack poison"),
                    () -> assertThat(delivery.getSuppressed()).isEmpty(),
                    () -> assertThat(leftByTheOtherWrite.get().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(instance().status()).isEqualTo(leftByTheOtherWrite.get().status()),
                    () -> assertThat(instance().version()).isEqualTo(leftByTheOtherWrite.get().version()),
                    () -> assertThat(errors()).noneMatch(m -> m.contains("could not be undone"))
            );
        }

        @Test
        void the_undo_does_not_overwrite_a_quarantine_that_a_lost_write_left_with_both_its_id_and_its_skip_count() {
            CloudEvent poison = pendingPoisonOnAStoreThatDropsTheQuarantineId();
            AtomicReference<SagaEnvelope<Order>> leftByTheOtherWrite = new AtomicReference<>();
            store.beforeNextWriteWithoutState(() -> store.beforeNextWriteWithoutState(() -> {
                SagaFailure failure = instance().failure();
                SagaFailure withId = new SagaFailure(failure.input(), failure.position(), failure.firstFailedAt(), failure.failureType(),
                        failure.failureMessage(), failure.releasedAt(), "another-quarantine");
                anotherWriteLeaves(SagaStatus.QUARANTINED, withId, 7L);
                leftByTheOtherWrite.set(instance());
            }));

            Throwable delivery = catchThrowable(() -> deliver(poison));

            assertAll(
                    () -> assertThat(delivery)
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("cannot pack poison"),
                    () -> assertThat(delivery.getSuppressed()).isEmpty(),
                    () -> assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(instance().quarantineId()).isEqualTo("another-quarantine"),
                    () -> assertThat(instance().quarantineSkipCount()).isEqualTo(7L),
                    () -> assertThat(instance().version()).isEqualTo(leftByTheOtherWrite.get().version()),
                    () -> assertThat(errors()).noneMatch(m -> m.contains("could not be undone"))
            );
        }

        @Test
        void the_delivery_fails_with_the_original_failure_and_the_store_failure_suppressed_when_the_store_throws_inside_the_undo() {
            CloudEvent poison = pendingPoisonOnAStoreThatDropsTheQuarantineId();
            RuntimeException storeFailure = new IllegalStateException("the store went away");
            store.beforeNextWriteWithoutState(() -> store.beforeNextWriteWithoutState(() -> {
                throw storeFailure;
            }));

            Throwable delivery = catchThrowable(() -> deliver(poison));

            assertAll(
                    () -> assertThat(delivery)
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("cannot pack poison"),
                    () -> assertThat(delivery.getSuppressed()).containsExactly(storeFailure),
                    () -> assertThat(errors()).anyMatch(m -> m.contains("undoing the quarantine failed because the state store failed")),
                    () -> assertThat(errors()).noneMatch(m -> m.contains("could not list it as pending"))
            );
        }

        // The failing event is pending, and the next delivery of it is past its budget, which quarantines the instance.
        private CloudEvent pendingPoisonOnAStoreThatDropsTheQuarantineId() {
            deliver(placed());
            failing.add("poison");
            CloudEvent poison = added("poison");
            assertThatThrownBy(() -> deliver(poison)).isInstanceOf(IllegalStateException.class);
            store.dropsTheQuarantineId = true;
            now.set(now.get().plus(BUDGET));
            return poison;
        }

        // Runs just before a write, makes another write succeed first, and does the same before each of the next count - 1 writes
        private void anotherWriteBeforeEachOfTheNext(int count) {
            anotherWriteLeavesTheInstanceQuarantinedWithoutAnId();
            if (count > 1) {
                store.beforeNextWriteWithoutState(() -> anotherWriteBeforeEachOfTheNext(count - 1));
            }
        }

        private List<String> errors() {
            return appender.list.stream()
                    .filter(e -> e.getLevel() == Level.ERROR)
                    .map(ILoggingEvent::getFormattedMessage)
                    .toList();
        }

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
            executionLog.setLevel(Level.ERROR);
        }

        @AfterEach
        void stopCapturing() {
            executionLog.setLevel(originalLevel);
            executionLog.detachAppender(appender);
            appender.stop();
        }

        private void anotherWriteLeavesTheInstanceQuarantinedWithoutAnId() {
            SagaEnvelope<Order> stored = instance();
            anotherWriteLeaves(stored.status(), stored.failure(), stored.quarantineSkipCount());
        }

        private void anotherWriteLeaves(SagaStatus status, @Nullable SagaFailure failure, @Nullable Long quarantineSkipCount) {
            SagaEnvelope<Order> stored = instance();
            SagaEnvelope<Order> next = new SagaEnvelope<>(stored.sagaId(), stored.state(), status, stored.version() + 1, stored.timers(),
                    stored.streamWatermarks(), stored.positionWatermark(), stored.createdAt(), stored.updatedAt(), stored.completedAt(),
                    stored.currentStep(), stored.started(), failure, stored.streamMarks(), quarantineSkipCount);
            assertThat(store.delegate.compareAndSave(ORDER, next, stored.version())).isTrue();
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
            SagaFailure failure034 = new SagaFailure(quarantined.failure().input(), 1L, start, IllegalStateException.class.getName(), "boom");
            SagaEnvelope<Order> by034 = new SagaEnvelope<>(ORDER, quarantined.state(), SagaStatus.QUARANTINED, quarantined.version() + 1, List.of(),
                    quarantined.streamWatermarks(), null, start, start, null, null, true, failure034, quarantined.streamMarks(), null);
            store.delegate.compareAndSave(ORDER, by034, quarantined.version());

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
        void release_reaches_the_runner_s_event_store_queries() {
            SagaSubscription subscription = run(SagaRunner.<OrderEvent, OrderCommand>agnostic(subscriptionModel, converter).eventStoreQueries(queries));

            assertThat(subscription.release("unknown")).isEqualTo(ReleaseResult.NOT_FOUND);
        }

        @Test
        void release_is_refused_naming_EventStoreQueries_when_the_runner_was_given_none() {
            SagaSubscription subscription = run(SagaRunner.agnostic(subscriptionModel, converter));

            assertThatThrownBy(() -> subscription.release("unknown"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("SagaRunner.eventStoreQueries(..)");
        }

        @Test
        void the_timer_poller_holds_back_the_release_while_timers_are_disabled_and_finishes_it_once_they_are_enabled() {
            quarantinedOnPoisonWithTwoStreamKeyedSkipped();
            failing.clear();
            failing.add("a");
            assertThat(execution.release(ORDER)).isEqualTo(ReleaseResult.STILL_RELEASING);
            failing.clear();
            AtomicBoolean timersEnabled = new AtomicBoolean(false);
            SagaRunner<OrderEvent, OrderCommand> runner = SagaRunner.<OrderEvent, OrderCommand>agnostic(subscriptionModel, converter).eventStoreQueries(queries);
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

    /** Reads the event store, with events that can be hidden as if deleted and reads that can be made to throw. */
    private static final class EventStoreReads implements EventStoreQueries {
        private final EventStoreQueries delegate;
        private final Set<String> hidden = ConcurrentHashMap.newKeySet();
        private final Set<String> withoutStreamKey = ConcurrentHashMap.newKeySet();
        private volatile @Nullable RuntimeException readFailure;

        private EventStoreReads(EventStoreQueries delegate) {
            this.delegate = delegate;
        }

        void hide(CloudEvent event) {
            hidden.add(event.getId());
        }

        void restore(CloudEvent event) {
            hidden.remove(event.getId());
        }

        void stripStreamKeyFrom(CloudEvent event) {
            withoutStreamKey.add(event.getId());
        }

        void failReadsWith(@Nullable RuntimeException failure) {
            readFailure = failure;
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            RuntimeException failure = readFailure;
            if (failure != null) {
                throw failure;
            }
            return delegate.query(filter, skip, limit, sortBy).filter(event -> !hidden.contains(event.getId()))
                    .map(event -> withoutStreamKey.contains(event.getId()) ? withoutStream(event) : event);
        }

        @Override
        public long count(Filter filter) {
            return delegate.count(filter);
        }

        @Override
        public boolean exists(Filter filter) {
            return delegate.exists(filter);
        }
    }

    // The in-memory store without the optional SagaStateStoreSkippedInputs capability
    private static final class StoreWithoutSkippedInputs implements SagaStateStore<Order> {
        private final SagaStateStore<Order> delegate = SagaStateStore.inMemory();

        @Override
        public Optional<SagaEnvelope<Order>> find(String sagaId) {
            return delegate.find(sagaId);
        }

        @Override
        public boolean compareAndSave(String sagaId, SagaEnvelope<Order> envelope, long expectedVersion) {
            return delegate.compareAndSave(sagaId, envelope, expectedVersion);
        }

        @Override
        public List<SagaEnvelope<Order>> findWithDueTimers(Instant now, int limit) {
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
        final List<SagaEnvelope<Order>> accepted = new CopyOnWriteArrayList<>();
        private final AtomicReference<@Nullable Runnable> beforeNextStateWrite = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> beforeNextWriteWithoutState = new AtomicReference<>();
        final AtomicInteger writesWithoutState = new AtomicInteger();
        private static final int MAX_RECORD_READS = 500;
        private final AtomicInteger recordReads = new AtomicInteger();
        private final AtomicReference<@Nullable Runnable> beforeNextRecord = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> afterNextRecord = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> beforeNextDeleteOfARecord = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> afterNextEmptyRead = new AtomicReference<>();
        private final AtomicReference<@Nullable Runnable> beforeNextActiveWrite = new AtomicReference<>();
        volatile boolean refusesToDecode;
        volatile @Nullable RuntimeException findFailure;
        volatile @Nullable RuntimeException findWithoutStateFailure;
        volatile @Nullable RuntimeException listFailure;
        volatile @Nullable String findFailsFor;
        volatile boolean dropsTheReleaseTime;
        volatile boolean dropsTheQuarantineId;
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

        @Override
        public Optional<SagaEnvelope<Order>> find(String sagaId) {
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
            return delegate.find(sagaId);
        }

        @Override
        public Optional<SagaEnvelope<Order>> findWithoutState(String sagaId) {
            RuntimeException failure = findWithoutStateFailure;
            if (failure != null) {
                throw failure;
            }
            run(beforeNextFindWithoutState);
            return delegate.findWithoutState(sagaId);
        }

        @Override
        public boolean compareAndSave(String sagaId, SagaEnvelope<Order> envelope, long expectedVersion) {
            Runnable hook = beforeNextStateWrite.getAndSet(null);
            if (hook != null) {
                hook.run();
            }
            boolean saved = delegate.compareAndSave(sagaId, envelope, expectedVersion);
            if (saved) {
                accepted.add(envelope);
            }
            return saved;
        }

        @Override
        public boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<Order> envelope, long expectedVersion) {
            writesWithoutState.incrementAndGet();
            if (envelope.status() == SagaStatus.ACTIVE) {
                run(beforeNextActiveWrite);
            }
            run(beforeNextWriteWithoutState);
            SagaEnvelope<Order> toSave = dropsTheReleaseTime && envelope.isReleasing() ? withoutTheReleaseTime(envelope) : envelope;
            if (dropsTheQuarantineId && envelope.isQuarantined()) {
                toSave = withoutTheQuarantineId(toSave);
            }
            boolean saved = delegate.compareAndSaveWithoutState(sagaId, toSave, expectedVersion);
            if (saved) {
                accepted.add(new SagaEnvelope<>(envelope.sagaId(), null, envelope.status(), envelope.version(), envelope.timers(),
                        envelope.streamWatermarks(), envelope.positionWatermark(), envelope.createdAt(), envelope.updatedAt(), envelope.completedAt(),
                        envelope.currentStep(), envelope.started(), envelope.failure(), envelope.streamMarks(), envelope.quarantineSkipCount()));
            }
            return saved;
        }

        // What a store that forgets SagaFailure.quarantineId and SagaEnvelope.quarantineSkipCount persists
        private static SagaEnvelope<Order> withoutTheQuarantineId(SagaEnvelope<Order> envelope) {
            SagaFailure failure = envelope.failure();
            SagaFailure forgotten = new SagaFailure(failure.input(), failure.position(), failure.firstFailedAt(), failure.failureType(), failure.failureMessage(), failure.releasedAt(), null);
            return new SagaEnvelope<>(envelope.sagaId(), envelope.state(), envelope.status(), envelope.version(), envelope.timers(),
                    envelope.streamWatermarks(), envelope.positionWatermark(), envelope.createdAt(), envelope.updatedAt(), envelope.completedAt(),
                    envelope.currentStep(), envelope.started(), forgotten, envelope.streamMarks(), null);
        }

        // What a store that forgets SagaFailure.releasedAt persists
        private static SagaEnvelope<Order> withoutTheReleaseTime(SagaEnvelope<Order> envelope) {
            SagaFailure failure = envelope.failure();
            SagaFailure forgotten = new SagaFailure(failure.input(), failure.position(), failure.firstFailedAt(), failure.failureType(), failure.failureMessage(), null, failure.quarantineId());
            return new SagaEnvelope<>(envelope.sagaId(), envelope.state(), envelope.status(), envelope.version(), envelope.timers(),
                    envelope.streamWatermarks(), envelope.positionWatermark(), envelope.createdAt(), envelope.updatedAt(), envelope.completedAt(),
                    envelope.currentStep(), envelope.started(), forgotten, envelope.streamMarks(), envelope.quarantineSkipCount());
        }

        @Override
        public List<SagaEnvelope<Order>> findWithDueTimers(Instant now, int limit) {
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

        @Override
        public void recordSkippedInput(String sagaId, String quarantineId, SkippedInput input) {
            run(beforeNextRecord);
            delegate.recordSkippedInput(sagaId, quarantineId, input);
            run(afterNextRecord);
        }

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            if (recordReads.incrementAndGet() > MAX_RECORD_READS) {
                // A release that reads a record it cannot delete reads it again, and a test that waits for that never ends
                throw new AssertionError("The release read the records more than " + MAX_RECORD_READS + " times, so it is not getting anywhere");
            }
            List<SkippedInput> found = delegate.findSkippedInputs(sagaId, quarantineId, limit);
            if (found.isEmpty()) {
                run(afterNextEmptyRead);
            }
            return found;
        }

        @Override
        public void deleteSkippedInputByKey(String sagaId, String quarantineId, String key) {
            run(beforeNextDeleteOfARecord);
            delegate.deleteSkippedInputByKey(sagaId, quarantineId, key);
        }

        @Override
        public void deleteSkippedInput(String sagaId, String quarantineId, SkippedInput input) {
            run(beforeNextDeleteOfARecord);
            delegate.deleteSkippedInput(sagaId, quarantineId, input);
        }

        @Override
        public void deleteSkippedInputs(String sagaId, String quarantineId) {
            delegate.deleteSkippedInputs(sagaId, quarantineId);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }
    }
}
