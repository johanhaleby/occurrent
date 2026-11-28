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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.*;
import org.occurrent.dsl.saga.blocking.SagaSubscription.ReleaseResult;
import org.occurrent.dsl.saga.codec.SagaStateCodec;
import org.occurrent.dsl.saga.codec.StoredSaga;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.dsl.saga.internal.SkippedInputKey;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A release whose read of the first record fails charges the failure to that record by its key when the event of the
 * record doesn't read. A record of an input with no stream id and version is keyed by its CloudEvent id and source, so
 * the failure must be counted under that key, and a key the saga never wrote names no input to count against.
 */
@DisplayName("A release whose read fails on a record whose event doesn't decode")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaReleaseReadFailureOnACloudEventKeyedRecordTest {

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
        Order with(String item) {
            List<String> added = new ArrayList<>(items);
            added.add(item);
            return new Order(orderId, List.copyOf(added));
        }
    }

    private volatile boolean poisonFails = false;
    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
    private final InMemoryEventStore eventStore = new InMemoryEventStore();
    private final CloudEventConverter<OrderEvent> converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:orders"))
            .idMapper(OrderEvent::eventId).build();
    private final List<Object> dispatched = new CopyOnWriteArrayList<>();
    private final AtomicInteger eventIds = new AtomicInteger();
    private final InMemorySagaStateStore<Order> inMemory = new InMemorySagaStateStore<>();
    private final SagaEnvelopes<Order> envelopes = SagaEnvelopes.of(inMemory, null);
    private final FailsToReadTheFirstRecord store = new FailsToReadTheFirstRecord();
    private final SagaExecution<OrderEvent, Order, Object> execution = new SagaExecution<>("orders", saga(), store, dispatched::add, converter, CONFIG, now::get);
    private String cloudEventKeyOfA = "";

    private Saga<OrderEvent, Order, Object> saga() {
        return Saga.<OrderEvent, Order, Object>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(e.orderId(), List.of()))
                .react(OrderPlaced.class, (state, e) -> List.of())
                .evolve(ItemAdded.class, (state, e) -> state.with(e.item()))
                .react(ItemAdded.class, (state, e) -> {
                    if (poisonFails && e.item().equals("poison")) {
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

    // What a feed hands over for an event that has no stream extensions
    private static CloudEvent withoutStream(CloudEvent event) {
        return CloudEventBuilder.v1(event)
                .withoutExtension(OccurrentCloudEventExtension.STREAM_ID)
                .withoutExtension(OccurrentCloudEventExtension.STREAM_VERSION)
                .build();
    }

    private String nextEventId() {
        return "e" + eventIds.incrementAndGet();
    }

    private SagaEnvelope<Order> instance() {
        return envelopes.findWithoutState(ORDER).orElseThrow();
    }

    // Quarantined on a poison input, with a recorded after it with no stream id and version. The first record the release
    // reads after it has replayed the poison is a, keyed by its CloudEvent id and source
    @BeforeEach
    void quarantinedOnPoisonWithAnInputKeyedByItsCloudEventIdAndSourceRecorded() {
        execution.onCloudEvent(append(new OrderPlaced(nextEventId(), ORDER)));
        poisonFails = true;
        CloudEvent poison = append(new ItemAdded(nextEventId(), ORDER, "poison"));
        catchThrowable(() -> execution.onCloudEvent(poison));
        now.set(now.get().plus(BUDGET));
        catchThrowable(() -> execution.onCloudEvent(poison));
        poisonFails = false;
        CloudEvent a = withoutStream(append(new ItemAdded(nextEventId(), ORDER, "a")));
        execution.onCloudEvent(a);
        cloudEventKeyOfA = SkippedInputKey.cloudEventKey(a.getId(), a.getSource().toString());
        List<SkippedInput> records = inMemory.findSkippedInputs(ORDER, Objects.requireNonNull(instance().quarantineId()), Integer.MAX_VALUE);
        assertThat(records).as("records of the quarantine").hasSize(2);
        assertThat(records.get(1).key()).as("the second record is keyed by the CloudEvent id and source").isEqualTo(cloudEventKeyOfA);
        store.armed = true;
    }

    private void pollAfterTheInterval() {
        now.set(now.get().plus(CONFIG.timerPollInterval()).plusMillis(1));
        execution.pollTimers();
    }

    private String evidence(@Nullable ReleaseResult result, @Nullable Throwable thrown) {
        SagaEnvelope<Order> current = instance();
        return "result " + result + ", threw " + thrown + ", status " + current.status() + ", releasing " + current.isReleasing()
               + ", failure " + current.failure() + ", version " + current.version() + ", dispatched " + dispatched;
    }

    @Test
    void the_failure_is_charged_under_the_cloud_event_key_of_the_record_across_polls_and_the_instance_is_quarantined_again_past_its_budget() {
        AtomicReference<@Nullable ReleaseResult> released = new AtomicReference<>();
        Throwable thrownByRelease = catchThrowable(() -> released.set(execution.release(ORDER)));
        SagaEnvelope<Order> afterRelease = instance();
        String releaseEvidence = evidence(released.get(), thrownByRelease);
        pollAfterTheInterval();
        SagaEnvelope<Order> afterFirstPoll = instance();
        String pollEvidence = evidence(null, null);
        now.set(now.get().plus(BUDGET));
        Throwable thrownByLastPoll = catchThrowable(execution::pollTimers);

        SagaEnvelope<Order> after = instance();
        String evidence = evidence(null, thrownByLastPoll);
        assertAll(
                () -> assertThat(store.firstRecordFailedToRead).as("the release read the next record and it failed. " + releaseEvidence).isTrue(),
                () -> assertThat(thrownByRelease).as("the failed read is recorded and not thrown. " + releaseEvidence).isNull(),
                () -> assertThat(released.get()).as(releaseEvidence).isEqualTo(ReleaseResult.STILL_RELEASING),
                () -> assertThat(afterRelease.failure()).as(releaseEvidence).isNotNull(),
                () -> assertThat(Objects.requireNonNull(afterRelease.failure()).input()).as("charged under the CloudEvent key. " + releaseEvidence).isEqualTo(cloudEventKeyOfA),
                () -> assertThat(afterFirstPoll.isReleasing()).as("inside the budget the instance stays releasing. " + pollEvidence).isTrue(),
                () -> assertThat(Objects.requireNonNull(afterFirstPoll.failure()).input()).as("still charged under the CloudEvent key. " + pollEvidence).isEqualTo(cloudEventKeyOfA),
                () -> assertThat(Objects.requireNonNull(afterFirstPoll.failure()).firstFailedAt()).as("the clock of the budget did not restart. " + pollEvidence)
                        .isEqualTo(Objects.requireNonNull(afterRelease.failure()).firstFailedAt()),
                () -> assertThat(thrownByLastPoll).as("past the budget the poll does not throw. " + evidence).isNull(),
                () -> assertThat(after.isReleasing()).as("past the budget the release has ended. " + evidence).isFalse(),
                () -> assertThat(after.status()).as(evidence).isEqualTo(SagaStatus.QUARANTINED),
                () -> assertThat(Objects.requireNonNull(after.failure()).input()).as("quarantined again on the CloudEvent key. " + evidence).isEqualTo(cloudEventKeyOfA),
                () -> assertThat(inMemory.findSkippedInputs(ORDER, Objects.requireNonNull(after.quarantineId()), Integer.MAX_VALUE)).as("the record stays. " + evidence).hasSize(1)
        );
    }

    @Test
    void a_first_record_whose_key_the_saga_never_wrote_propagates_the_read_failure_as_it_is_with_the_bad_key_suppressed_and_charges_nothing() {
        store.firstRecordKeyOverride = "not a key";

        Throwable thrown = catchThrowable(() -> execution.release(ORDER));

        SagaEnvelope<Order> after = instance();
        String evidence = evidence(null, thrown);
        assertAll(
                () -> assertThat(store.firstRecordFailedToRead).as("the release read the next record and it failed. " + evidence).isTrue(),
                () -> assertThat(thrown).as("the read failure propagates as it is. " + evidence).isSameAs(store.readFailure),
                () -> assertThat(thrown.getSuppressed()).as("the bad key is suppressed on it. " + evidence).anySatisfy(suppressed -> assertThat(suppressed).isInstanceOf(IllegalArgumentException.class)),
                () -> assertThat(store.writesAfterTheFailedRead.get()).as("nothing is written. " + evidence).isZero(),
                () -> assertThat(after.version()).as("the instance is as the failed read found it. " + evidence).isEqualTo(store.versionAtTheFailedRead),
                () -> assertThat(after.isReleasing()).as(evidence).isTrue()
        );
    }

    private class Delegating implements SagaStateStore<Order>, SagaStateStoreSkippedInputs {
        @Override
        public boolean fits(StoredSaga candidate) {
            return inMemory.fits(candidate);
        }

        @Override
        public boolean fitsWithoutState(StoredSaga saga, long expectedVersion) {
            return inMemory.fitsWithoutState(saga, expectedVersion);
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

    // Once the first record is replayed, the event of the next doesn't decode, so the read of the instance with it and the
    // read of the record alone throw while the read of its seq and key works
    private final class FailsToReadTheFirstRecord extends Delegating {
        final RuntimeException readFailure = new IllegalStateException("the event of the record does not decode");
        volatile boolean armed = false;
        volatile boolean replayedTheFirstRecord = false;
        volatile boolean firstRecordFailedToRead = false;
        volatile @Nullable String firstRecordKeyOverride = null;
        volatile long versionAtTheFailedRead = -1;
        final AtomicInteger writesAfterTheFailedRead = new AtomicInteger();

        private void failIfTheFirstRecordDoesNotDecode() {
            if (armed && replayedTheFirstRecord) {
                if (!firstRecordFailedToRead) {
                    versionAtTheFailedRead = inMemory.findWithoutState(ORDER).map(StoredSaga::version).orElse(-1L);
                }
                firstRecordFailedToRead = true;
                throw readFailure;
            }
        }

        private boolean counted(boolean written) {
            if (firstRecordFailedToRead) {
                writesAfterTheFailedRead.incrementAndGet();
            }
            return written;
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInput> findWithFirstSkippedInput(String sagaId) {
            failIfTheFirstRecordDoesNotDecode();
            return super.findWithFirstSkippedInput(sagaId);
        }

        @Override
        public List<SkippedInput> findSkippedInputs(String sagaId, String quarantineId, int limit) {
            if (firstRecordFailedToRead) {
                throw new IllegalStateException("the event of the record does not decode, read alone");
            }
            return super.findSkippedInputs(sagaId, quarantineId, limit);
        }

        @Override
        public Optional<StoredSagaWithFirstSkippedInputKey> findWithFirstSkippedInputKey(String sagaId) {
            Optional<StoredSagaWithFirstSkippedInputKey> read = super.findWithFirstSkippedInputKey(sagaId);
            String override = firstRecordKeyOverride;
            if (override == null || !firstRecordFailedToRead) {
                return read;
            }
            return read.map(found -> new StoredSagaWithFirstSkippedInputKey(found.saga(),
                    found.first() == null ? null : new StoredSagaWithFirstSkippedInputKey.RecordKey(found.first().seq(), override)));
        }

        @Override
        public boolean compareAndSave(StoredSaga saga, long expectedVersion) {
            return counted(super.compareAndSave(saga, expectedVersion));
        }

        @Override
        public boolean compareAndSaveWithoutState(StoredSaga saga, long expectedVersion) {
            return counted(super.compareAndSaveWithoutState(saga, expectedVersion));
        }

        @Override
        public boolean compareAndSaveReplayed(StoredSaga saga, long expectedVersion, String quarantineId, SkippedInput replayed) {
            boolean saved = counted(super.compareAndSaveReplayed(saga, expectedVersion, quarantineId, replayed));
            replayedTheFirstRecord = true;
            return saved;
        }

        @Override
        public boolean compareAndSaveQuarantineEnded(StoredSaga saga, long expectedVersion, String quarantineId) {
            return counted(super.compareAndSaveQuarantineEnded(saga, expectedVersion, quarantineId));
        }
    }
}
