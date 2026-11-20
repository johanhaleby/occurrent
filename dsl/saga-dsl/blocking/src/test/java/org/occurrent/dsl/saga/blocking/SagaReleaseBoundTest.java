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
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
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
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.jspecify.annotations.Nullable;
import java.util.Optional;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.dsl.saga.internal.SagaExecutionSupport;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A release keeps every version it applies in the stream marks of the instance until it ends, so the marks are bound,
 * see {@link SagaExecutionSupport#MAX_APPLIED_VERSIONS_IN_A_RELEASE}. At the bound a release quarantines the instance
 * again on the next record without applying it. Seeding that many versions through a real flow is not possible, so each
 * test writes the stream marks of the instance through its state store, and the rest is driven through
 * {@link SagaExecution} with a test clock.
 */
@DisplayName("Releasing a quarantined saga instance that keeps as many applied versions as a release may")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaReleaseBoundTest {

    private static final int BOUND = SagaExecutionSupport.MAX_APPLIED_VERSIONS_IN_A_RELEASE;
    private static final String ORDER = "o1";
    private static final String SEEDED_TYPE = "seeded";
    private static final Duration BUDGET = Duration.ofMinutes(1);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(15);
    private static final SagaRunnerConfig CONFIG = SagaRunnerConfig.defaults().withQuarantineAfter(BUDGET)
            .withTimerPollInterval(POLL_INTERVAL)
            .withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT);
    private static final SagaRunnerConfig CONFIG_WITHOUT_QUARANTINE = SagaRunnerConfig.defaults().disableQuarantine()
            .withTimerPollInterval(POLL_INTERVAL)
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
    private final InMemorySagaStateStore<Order> store = new InMemorySagaStateStore<>();
    private final SagaEnvelopes<Order> envelopes = SagaEnvelopes.of(store, null);

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

    private SagaExecution<OrderEvent, Order, Object> execution(SagaRunnerConfig config) {
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

    private static String streamKey(CloudEvent event) {
        return event.getExtension(OccurrentCloudEventExtension.STREAM_ID) + "@" + event.getExtension(OccurrentCloudEventExtension.STREAM_VERSION);
    }

    private record Quarantined(SagaExecution<OrderEvent, Order, Object> execution, CloudEvent poison, CloudEvent a, CloudEvent b) {
    }

    // Quarantined on "poison", with "a" and "b" skipped after it as stream-keyed inputs
    private Quarantined quarantinedOnPoisonWithTwoSkipped() {
        SagaExecution<OrderEvent, Order, Object> execution = execution(CONFIG);
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        CloudEvent poison = append(new ItemAdded(nextEventId(), ORDER, "poison"));
        catchThrowable(() -> execution.onCloudEvent(poison));
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> execution.onCloudEvent(poison));
        assertThat(instance().status()).isEqualTo(SagaStatus.QUARANTINED);
        poisonFails = false;
        CloudEvent a = append(new ItemAdded(nextEventId(), ORDER, "a"));
        execution.onCloudEvent(a);
        CloudEvent b = append(new ItemAdded(nextEventId(), ORDER, "b"));
        execution.onCloudEvent(b);
        return new Quarantined(execution, poison, a, b);
    }

    // Adds applied versions of an event type no saga event has, so the instance keeps exactly the count asked for
    // over what it keeps already. The versions sit far above the stream's own, so no recorded input is among them
    private void keepAppliedVersions(long count) {
        SagaEnvelope<Order> current = instance();
        long extra = count - SagaExecutionSupport.appliedVersions(current);
        assertThat(extra).as("the instance keeps fewer versions than asked for").isPositive();
        List<Long> applied = new ArrayList<>();
        for (long version = 0; version < extra; version++) {
            applied.add(1_000_000L + version);
        }
        StreamMarks existing = current.streamMarks().get(ORDER);
        Map<String, TypeMark> byType = new HashMap<>(existing.byType());
        byType.put(SEEDED_TYPE, new TypeMark(1_000_000L + extra, applied, 0));
        Map<String, StreamMarks> marks = new HashMap<>(current.streamMarks());
        marks.put(ORDER, new StreamMarks(existing.floor(), byType, existing.pending(), existing.legacyFailed(), existing.pendingSince()));
        SagaEnvelope<Order> seeded = new SagaEnvelope<>(current.sagaId(), current.state(), current.status(), current.version(), current.timers(),
                current.streamWatermarks(), current.positionWatermark(), current.createdAt(), current.updatedAt(), current.completedAt(),
                current.currentStep(), current.started(), current.failure(), marks, current.quarantineSkipCount());
        assertThat(envelopes.compareAndSave(ORDER, seeded, current.version())).isTrue();
        assertThat(SagaExecutionSupport.appliedVersions(instance())).isEqualTo(count);
    }

    @Test
    void a_release_at_the_bound_quarantines_the_instance_again_on_the_record_without_applying_it_or_dispatching_its_commands_and_trims_the_marks() {
        Quarantined q = quarantinedOnPoisonWithTwoSkipped();
        keepAppliedVersions(BOUND);
        dispatched.clear();

        ReleaseResult result = q.execution().release(ORDER);

        SagaEnvelope<Order> after = instance();
        assertAll(
                () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(dispatched).as("nothing is dispatched").isEmpty(),
                () -> assertThat(after.state().items()).as("the state takes no input").isEmpty(),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.isReleasing()).isFalse(),
                () -> assertThat(after.failure().input()).as("the failure record names the record's input").isEqualTo(streamKey(q.poison())),
                () -> assertThat(after.failure().failureMessage()).contains(String.valueOf(SagaExecutionSupport.MAX_APPLIED_VERSIONS_IN_A_RELEASE)),
                () -> assertThat(SagaExecutionSupport.appliedVersions(after)).as("the marks are trimmed").isLessThan(1_000),
                () -> assertThat(store.findSkippedInputs(ORDER, after.quarantineId(), Integer.MAX_VALUE)).extracting(i -> i.key())
                        .as("the record is still recorded, first").containsExactly(streamKey(q.poison()), streamKey(q.a()), streamKey(q.b()))
        );
    }

    @Test
    void a_further_release_after_the_bound_goes_on_from_the_record_and_applies_every_record_once_in_order() {
        Quarantined q = quarantinedOnPoisonWithTwoSkipped();
        keepAppliedVersions(BOUND);
        ReleaseResult atTheBound = q.execution().release(ORDER);
        dispatched.clear();

        ReleaseResult result = q.execution().release(ORDER);

        SagaEnvelope<Order> after = instance();
        assertAll(
                () -> assertThat(atTheBound).as("control: the first release stopped at the bound").isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(result).isEqualTo(ReleaseResult.RELEASED),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.ACTIVE),
                () -> assertThat(after.state().items()).containsExactly("poison", "a", "b"),
                () -> assertThat(dispatched).containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a"), new Pack(ORDER, "b"))
        );
    }

    @Test
    void a_release_one_version_under_the_bound_applies_the_records_until_its_own_versions_take_it_to_the_bound_and_stops_there() {
        Quarantined q = quarantinedOnPoisonWithTwoSkipped();
        keepAppliedVersions(BOUND - 1);
        dispatched.clear();

        ReleaseResult result = q.execution().release(ORDER);

        SagaEnvelope<Order> after = instance();
        assertAll(
                () -> assertThat(dispatched).as("each record before the bound is applied and dispatched once").containsExactly(new Pack(ORDER, "poison"), new Pack(ORDER, "a")),
                () -> assertThat(after.state().items()).containsExactly("poison", "a"),
                () -> assertThat(result).as("the versions it applied took the instance to the bound").isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(after.failure().input()).isEqualTo(streamKey(q.b()))
        );
    }

    @Test
    void a_release_at_the_bound_quarantines_the_instance_again_also_when_the_config_has_no_quarantine_budget() {
        Quarantined q = quarantinedOnPoisonWithTwoSkipped();
        keepAppliedVersions(BOUND);
        dispatched.clear();
        SagaExecution<OrderEvent, Order, Object> withoutBudget = execution(CONFIG_WITHOUT_QUARANTINE);

        ReleaseResult result = withoutBudget.release(ORDER);

        SagaEnvelope<Order> after = instance();
        assertAll(
                () -> assertThat(CONFIG_WITHOUT_QUARANTINE.quarantineAfter()).as("control: no budget").isEmpty(),
                () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(after.state().items()).isEmpty(),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.failure().input()).isEqualTo(streamKey(q.poison())),
                () -> assertThat(SagaExecutionSupport.appliedVersions(after)).isLessThan(1_000)
        );
    }

    @Test
    void a_release_at_the_bound_on_a_store_that_cannot_write_atomically_quarantines_the_instance_again_without_applying_the_record() {
        Quarantined q = quarantinedOnPoisonWithTwoSkipped();
        keepAppliedVersions(BOUND);
        dispatched.clear();
        SagaExecution<OrderEvent, Order, Object> nonAtomic = new SagaExecution<>("orders", saga(), new StoreThatCannotWriteAtomically(), dispatched::add, converter, CONFIG_WITHOUT_QUARANTINE, now::get);

        ReleaseResult result = nonAtomic.release(ORDER);

        SagaEnvelope<Order> after = instance();
        assertAll(
                () -> assertThat(result).isEqualTo(ReleaseResult.QUARANTINED_AGAIN),
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(after.state().items()).isEmpty(),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.failure().input()).isEqualTo(streamKey(q.poison())),
                () -> assertThat(SagaExecutionSupport.appliedVersions(after)).isLessThan(1_000),
                () -> assertThat(store.findSkippedInputs(ORDER, after.quarantineId(), Integer.MAX_VALUE)).extracting(i -> i.key())
                        .containsExactly(streamKey(q.poison()), streamKey(q.a()), streamKey(q.b()))
        );
    }

    // Delegates to the atomic store, but refuses atomic transitions, so the execution skips inputs unrecorded. Any write of a record fails
    private final class StoreThatCannotWriteAtomically implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
        @Override
        public void requireAtomicTransitions() {
            throw new IllegalStateException("this store cannot write atomically");
        }

        @Override
        public SagaStateCodec<Order> stateCodec() {
            return store.stateCodec();
        }

        @Override
        public Optional<StoredSaga> find(String sagaId) {
            return store.find(sagaId);
        }

        @Override
        public Optional<StoredSaga> findWithoutState(String sagaId) {
            return store.findWithoutState(sagaId);
        }

        @Override
        public boolean compareAndSave(StoredSaga saga, long expectedVersion) {
            return store.compareAndSave(saga, expectedVersion);
        }

        @Override
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            return store.compareAndSaveWithoutState(saga, expectedVersion);
        }

        @Override
        public List<StoredSaga> findWithDueTimers(Instant now, int limit) {
            return store.findWithDueTimers(now, limit);
        }

        @Override
        public List<String> findReleasingSagaIds(@Nullable String after, int limit) {
            return store.findReleasingSagaIds(after, limit);
        }

        @Override
        public boolean compareAndSaveSkipped(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput input) {
            throw new IllegalStateException("the server refuses the transaction");
        }

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            return store.findSkippedInputs(sagaId, quarantineId, limit);
        }

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            throw new IllegalStateException("the server refuses the transaction");
        }

        @Override
        public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
            throw new IllegalStateException("the server refuses the transaction");
        }

        @Override
        public void delete(String sagaId) {
            store.delete(sagaId);
        }
    }

    @Test
    void the_timer_poller_going_on_with_a_release_at_the_bound_quarantines_the_instance_again_without_dispatching_the_record() {
        Quarantined q = quarantinedOnPoisonWithTwoSkipped();
        aFails = true;
        ReleaseResult stopped = q.execution().release(ORDER);
        aFails = false;
        keepAppliedVersions(BOUND);
        dispatched.clear();
        now.set(instance().updatedAt().plus(POLL_INTERVAL).plusMillis(1));

        q.execution().pollTimers();

        SagaEnvelope<Order> after = instance();
        assertAll(
                () -> assertThat(stopped).as("control: the release stopped on 'a'").isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(dispatched).isEmpty(),
                () -> assertThat(after.state().items()).containsExactly("poison"),
                () -> assertThat(after.status()).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(after.isReleasing()).isFalse(),
                () -> assertThat(after.failure().input()).isEqualTo(streamKey(q.a())),
                () -> assertThat(SagaExecutionSupport.appliedVersions(after)).isLessThan(1_000)
        );
    }
}
