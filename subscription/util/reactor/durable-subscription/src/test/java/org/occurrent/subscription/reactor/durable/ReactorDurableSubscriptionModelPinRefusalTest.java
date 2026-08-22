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

package org.occurrent.subscription.reactor.durable;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.api.reactor.SubscriptionHandle;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What happens when the first position recorded for a subscription id is not the one this registration read.
 * <p>
 * The model reads storage, finds nothing, reads a position and writes it on the condition that nothing is stored yet.
 * A refused write means a checkpoint arrived between those two, and nothing here can order it against the position
 * this registration read, so the registration is refused rather than started from a position it never read. Reading
 * the stored position back is the one thing that lets a registration through, and only when it holds that same
 * position.
 * <p>
 * Hand-rolled storages rather than MongoDB, because every case here is about the order two calls reach storage in,
 * and about what a second read answers, both of which a real database hides rather than shows.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelPinRefusalTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void the_first_position_recorded_for_a_subscription_is_written_only_if_nothing_is_stored() {
        ConditionRecordingCheckpointStorage storage = new ConditionRecordingCheckpointStorage();
        ReactorDurableSubscriptionModel model = coldModel(new RecordingSubscriptionModel("at-registration"), storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThat(storage.conditions).containsExactly(CheckpointWriteCondition.ifAbsent());
    }

    @Test
    void a_registration_that_loses_the_write_to_a_position_it_did_not_read_is_refused() {
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        // Another node's write arrives after this registration read and found nothing, which is the whole of the
        // race. Both nodes saw an empty storage, and only one of the two positions can be kept.
        storage.whenTheFirstReadFindsNothing = () -> storage.writeWithoutScripting("landed-during-registration");
        ReactorDurableSubscriptionModel model = coldModel(new RecordingSubscriptionModel("this-nodes-own-position"), storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .as("the position stored is not the one this registration read, so starting from it would skip whatever lies between them")
                .isInstanceOf(StartPositionAlreadyPinnedException.class);
    }

    @Test
    void a_refusal_names_the_position_read_and_the_one_storage_held_when_it_was_read_back() {
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        storage.whenTheFirstReadFindsNothing = () -> storage.writeWithoutScripting("landed-during-registration");
        ReactorDurableSubscriptionModel model = coldModel(new RecordingSubscriptionModel("this-nodes-own-position"), storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class, refusal -> {
                    assertThat(refusal.subscriptionId).isEqualTo(SUBSCRIPTION_ID);
                    assertThat(refusal.positionRead.asString()).isEqualTo("this-nodes-own-position");
                    assertThat(refusal.positionStored).hasValueSatisfying(
                            stored -> assertThat(stored.asString()).isEqualTo("landed-during-registration"));
                    assertThat(refusal.getCause()).isNull();
                })
                .hasMessageContaining("in a second call");
    }

    @Test
    void a_registration_that_loses_the_write_adopts_the_earlier_position_instead_of_being_refused_when_the_storage_can_order_them() {
        // Same shape as a_registration_that_loses_the_write_to_a_position_it_did_not_read_is_refused, but the storage
        // can compare the two positions, so the loss no longer refuses the registration: this node's earlier position
        // is durably written in place of the later one that landed during registration.
        OrderAwareCheckpointStorage storage = new OrderAwareCheckpointStorage();
        storage.whenTheFirstReadFindsNothing = () -> storage.writeWithoutScripting(new OrderedCheckpoint(50));
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("this-nodes-own-position");
        delegate.globalCheckpoint = new OrderedCheckpoint(10);
        ReactorDurableSubscriptionModel model = coldModel(delegate, storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThat(subscription.waitUntilStarted().block(TIMEOUT)).isNull();
        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT).asString())
                .as("the earlier position replaces the later one that landed during registration, so nothing between them is skipped")
                .isEqualTo("order-10");
    }

    @Test
    void a_registration_is_refused_when_the_position_that_was_stored_cannot_be_read_back() {
        // Nothing here can show the stored position is the one this registration read, so it is refused for the same
        // reason a differing position is. The failure that stopped it from being read is the cause.
        RuntimeException unreachable = new IllegalStateException("the checkpoint store is unreachable");
        CheckpointStorage storage = alwaysRefusingStorage(() -> Mono.error(unreachable));
        ReactorDurableSubscriptionModel model = coldModel(new RecordingSubscriptionModel("this-nodes-own-position"), storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class, refusal -> {
                    assertThat(refusal.positionStored).isEmpty();
                    assertThat(refusal.getCause()).isSameAs(unreachable);
                })
                .hasMessageContaining("failed");
    }

    @Test
    void a_stored_position_that_reads_back_as_nothing_is_refused_without_being_named_as_null() {
        // An empty read is not proof the checkpoint was removed. A read served from somewhere that has not seen the
        // write answers the same way, so this is refused for the weaker reason that nothing here can show they agree.
        CheckpointStorage storage = alwaysRefusingStorage(Mono::empty);
        ReactorDurableSubscriptionModel model = coldModel(new RecordingSubscriptionModel("this-nodes-own-position"), storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class, refusal -> {
                    assertThat(refusal.positionStored).isEmpty();
                    assertThat(refusal.getCause()).isNull();
                })
                .hasMessageContaining("found nothing")
                .hasMessageNotContaining("null");
    }

    @Test
    void a_write_that_failed_for_some_other_reason_is_reported_as_that_failure_and_not_as_a_refusal() {
        // Only a refused condition says a position was recorded before this registration's. A write that failed
        // because the store could not be reached says nothing of the kind, and reporting it as a refusal would send
        // whoever reads it looking for a position that was never stored.
        RuntimeException unreachable = new IllegalStateException("the checkpoint store is unreachable");
        CheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                return Mono.error(unreachable);
            }
        };
        ReactorDurableSubscriptionModel model = coldModel(new RecordingSubscriptionModel("this-nodes-own-position"), storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isNotInstanceOf(StartPositionAlreadyPinnedException.class)
                .hasMessageContaining("unreachable");
    }

    @Test
    void a_registration_completes_when_the_stored_position_holds_what_it_read_however_that_checkpoint_was_built() {
        // The two positions are compared as the only thing Checkpoint promises, its string. A comparison by value or
        // by identity would refuse this, and the two nodes do agree on where the subscription starts.
        Checkpoint builtSomeOtherWay = () -> "this-nodes-own-position";
        CheckpointStorage storage = alwaysRefusingStorage(() -> Mono.just(builtSomeOtherWay));
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("this-nodes-own-position");
        ReactorDurableSubscriptionModel model = coldModel(delegate, storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        subscription.waitUntilStarted().block(TIMEOUT);
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("this-nodes-own-position");
    }

    @Test
    void a_storage_that_reports_a_write_of_the_position_already_stored_as_success_never_reaches_the_read_back() {
        // The shape the MongoDB storages have, and the one most applications run against. ifAbsent lets a storage
        // report a write of the value already stored as success, so two nodes that read the same position both
        // complete there without the read back being reached at all.
        ValueComparingCheckpointStorage storage = new ValueComparingCheckpointStorage();
        storage.writeWithoutScripting("the-same-position");
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("the-same-position");
        ReactorDurableSubscriptionModel model = coldModel(delegate, storage);
        storage.hideWhatIsStoredFromTheNextRead();

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        subscription.waitUntilStarted().block(TIMEOUT);
        assertThat(storage.readsAfterTheWrite).hasValue(0);
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("the-same-position");
    }

    @Test
    void a_node_at_a_different_position_is_refused_on_a_storage_that_reports_a_matching_write_as_success() {
        ValueComparingCheckpointStorage storage = new ValueComparingCheckpointStorage();
        storage.writeWithoutScripting("another-position");
        ReactorDurableSubscriptionModel model = coldModel(new RecordingSubscriptionModel("this-nodes-own-position"), storage);
        storage.hideWhatIsStoredFromTheNextRead();

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(StartPositionAlreadyPinnedException.class);
    }

    @Test
    void a_position_already_stored_when_the_read_ran_is_taken_without_a_write_of_any_kind() {
        // The branch the refusal must never reach. A node joining a subscription that has been running elsewhere
        // finds a position and starts from it, and writes nothing, which is what it did before any of this.
        ConditionRecordingCheckpointStorage storage = new ConditionRecordingCheckpointStorage();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("from-a-previous-run")).block(TIMEOUT);
        storage.conditions.clear();
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("wherever-the-feed-is-now");
        ReactorDurableSubscriptionModel model = coldModel(delegate, storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        subscription.waitUntilStarted().block(TIMEOUT);
        assertThat(storage.conditions).isEmpty();
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("from-a-previous-run");
    }

    @Test
    void the_refusal_is_thrown_from_subscribe_when_the_wrapped_model_manages_named_subscriptions() {
        // This path awaits the position so that the wrapped model is handed one, so the refusal is what the caller
        // gets back from subscribe itself, unwrapped, the way the blocking model refuses a registration.
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        storage.whenTheFirstReadFindsNothing = () -> storage.writeWithoutScripting("landed-during-registration");
        NamedRecordingSubscriptionModel delegate = new NamedRecordingSubscriptionModel("this-nodes-own-position");
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()))
                .isInstanceOf(StartPositionAlreadyPinnedException.class);
        assertThat(delegate.subscribedIds)
                .as("a registration that was refused is not handed to the wrapped model, so nothing runs from a position nobody read")
                .isEmpty();
    }

    @Test
    void the_refusal_reaches_a_caller_that_asks_whether_the_subscription_started_only_afterwards() {
        // The cold path cannot throw from subscribe, since resolving the position waits on storage and this call
        // holds the model's monitor. It reports the refusal where it reports any start it could not make, and a
        // caller that asks after the fact still gets it rather than an answer that never comes.
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        storage.whenTheFirstReadFindsNothing = () -> storage.writeWithoutScripting("landed-during-registration");
        ReactorDurableSubscriptionModel model = coldModel(new RecordingSubscriptionModel("this-nodes-own-position"), storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isFalse();

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(StartPositionAlreadyPinnedException.class);
    }

    @Test
    void a_subscription_registered_while_the_model_was_stopped_is_refused_when_it_is_resumed() {
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        ReactorDurableSubscriptionModel model = coldModel(delegate, storage);
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        // Nothing is stored until the subscription starts, so the race is run at the point it is resumed.
        storage.whenTheFirstReadFindsNothing = () -> storage.writeWithoutScripting("landed-while-it-waited");

        SubscriptionHandle resumed = model.resumeSubscription(SUBSCRIPTION_ID);

        assertThatThrownBy(() -> resumed.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(StartPositionAlreadyPinnedException.class);
        assertThat(delegate.startedAt).isEmpty();
    }

    @Test
    void a_storage_that_cannot_evaluate_write_conditions_keeps_the_unconditional_write() {
        // Nothing here can make such a storage write conditionally, and refusing it would take out a storage that
        // has worked until now over a capability it never claimed. So the write is the one 0.32.0 made.
        ConditionRecordingCheckpointStorage storage = new ConditionRecordingCheckpointStorage();
        storage.evaluatesWriteConditions = false;
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        ReactorDurableSubscriptionModel model = coldModel(delegate, storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        subscription.waitUntilStarted().block(TIMEOUT);
        assertThat(storage.conditions).containsExactly(CheckpointWriteCondition.any());
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("at-registration");
    }

    @Test
    void a_storage_that_answers_nothing_about_its_own_write_is_refused_rather_than_started_from_now() {
        // save is documented to hand the checkpoint back, so a storage answering nothing has said neither that the
        // position was recorded nor that it was not. Starting anyway would begin wherever the feed has reached.
        CheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                return Mono.empty();
            }
        };
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        ReactorDurableSubscriptionModel model = coldModel(delegate, storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(SUBSCRIPTION_ID)
                .hasMessageContaining("answered nothing");
        assertThat(delegate.startedAt).isEmpty();
    }

    @Test
    void a_storage_that_evaluates_no_write_conditions_and_answers_nothing_is_refused_the_same_way() {
        // The unconditional write is a second way through the same code, so the guard has to sit where both reach it.
        // Without it this one completes empty, which the caller reads as a start position that opted out, and the
        // subscription starts from the caller's original default with no position recorded at all.
        CheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public boolean evaluatesWriteConditions() {
                return false;
            }

            @Override
            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
                return Mono.empty();
            }
        };
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        ReactorDurableSubscriptionModel model = coldModel(delegate, storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("answered nothing");
        assertThat(delegate.startedAt).isEmpty();
    }

    @Test
    void a_start_position_the_caller_named_is_not_recorded_at_all() {
        // Only the model default reads a stored checkpoint, so writing for any other position would record one
        // nothing starts from, over a subscription the caller asked to replay.
        ConditionRecordingCheckpointStorage storage = new ConditionRecordingCheckpointStorage();
        ReactorDurableSubscriptionModel model = coldModel(new RecordingSubscriptionModel("at-registration"), storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(new StringBasedCheckpoint("replay-from-here")), __ -> Mono.empty());

        assertThat(storage.conditions).isEmpty();
        assertThat(storage.read(SUBSCRIPTION_ID).blockOptional(TIMEOUT)).isEmpty();
    }

    private static ReactorDurableSubscriptionModel coldModel(CheckpointAwareSubscriptionModel delegate, CheckpointStorage storage) {
        return new ReactorDurableSubscriptionModel(delegate, storage);
    }

    private static String startedAtCheckpoint(RecordingSubscriptionModel delegate) {
        assertThat(delegate.startedAt).hasSize(1);
        assertThat(delegate.startedAt.getFirst()).isInstanceOf(StartAt.StartAtCheckpoint.class);
        return ((StartAt.StartAtCheckpoint) delegate.startedAt.getFirst()).checkpoint.asString();
    }

    /**
     * A storage whose first read finds nothing, so a registration gets as far as writing, and whose write is then
     * refused, so the tests using it decide what reading the position back answers.
     */
    private static CheckpointStorage alwaysRefusingStorage(Supplier<Mono<Checkpoint>> readBack) {
        return new InMemoryCheckpointStorage() {

            private final AtomicInteger reads = new AtomicInteger();

            @Override
            public Mono<Checkpoint> read(String subscriptionId) {
                return Mono.defer(() -> reads.getAndIncrement() == 0 ? Mono.empty() : readBack.get());
            }

            @Override
            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                return Mono.error(new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), condition));
            }
        };
    }

    /**
     * Runs a hook the first time a read finds nothing, which is where a write that beats this registration's own
     * arrives. The write that follows is then refused by {@link InMemoryCheckpointStorage}'s own {@code ifAbsent}.
     */
    private static class RaceSimulatingCheckpointStorage extends InMemoryCheckpointStorage {

        private final AtomicInteger reads = new AtomicInteger();

        @Nullable Runnable whenTheFirstReadFindsNothing;

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.defer(() -> {
                boolean isTheFirstRead = reads.getAndIncrement() == 0;
                Mono<Checkpoint> read = super.read(subscriptionId);
                return isTheFirstRead && whenTheFirstReadFindsNothing != null
                        ? read.switchIfEmpty(Mono.fromRunnable(whenTheFirstReadFindsNothing))
                        : read;
            });
        }

        /**
         * Writes the way another node would, without going through the scripting above, so planting a position does
         * not count as one of this registration's own reads.
         */
        void writeWithoutScripting(String checkpoint) {
            super.save(SUBSCRIPTION_ID, new StringBasedCheckpoint(checkpoint), CheckpointWriteCondition.any()).block(TIMEOUT);
        }
    }

    /**
     * The MongoDB shape: {@code ifAbsent} compares values and reports a write of the position already stored as
     * success. What is stored can be hidden from the next read so a registration reaches the write at all.
     */
    private static final class ValueComparingCheckpointStorage extends InMemoryCheckpointStorage {

        final AtomicInteger readsAfterTheWrite = new AtomicInteger();

        private final ConcurrentHashMap<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();
        private volatile boolean hideFromTheNextRead = false;
        private volatile boolean written = false;

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.defer(() -> {
                if (written) {
                    readsAfterTheWrite.incrementAndGet();
                }
                if (hideFromTheNextRead) {
                    hideFromTheNextRead = false;
                    return Mono.empty();
                }
                return Mono.justOrEmpty(checkpoints.get(subscriptionId));
            });
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.defer(() -> {
                written = true;
                Checkpoint stored = checkpoints.get(subscriptionId);
                if (condition instanceof CheckpointWriteCondition.IfAbsent && stored != null) {
                    // Reported as success when it writes what is already there, which is what the write ends up
                    // doing, so the position stays as it is.
                    return stored.asString().equals(checkpoint.asString())
                            ? Mono.just(checkpoint)
                            : Mono.error(new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), condition));
                }
                checkpoints.put(subscriptionId, checkpoint);
                return Mono.just(checkpoint);
            });
        }

        void writeWithoutScripting(String checkpoint) {
            checkpoints.put(SUBSCRIPTION_ID, new StringBasedCheckpoint(checkpoint));
        }

        void hideWhatIsStoredFromTheNextRead() {
            hideFromTheNextRead = true;
        }
    }

    /**
     * Records the condition every write used, and can answer that it evaluates none of them.
     */
    private static final class ConditionRecordingCheckpointStorage extends InMemoryCheckpointStorage {

        final List<CheckpointWriteCondition> conditions = new CopyOnWriteArrayList<>();

        boolean evaluatesWriteConditions = true;

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            conditions.add(condition);
            return super.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return evaluatesWriteConditions;
        }
    }

    /**
     * Same scripting hook as {@link RaceSimulatingCheckpointStorage}, plus a real {@code resolveFirstCheckpointRace}
     * that compares by {@link OrderedCheckpoint#order()}, standing in for what the MongoDB storages do by comparing
     * operation time. Answers empty for a candidate or a stored checkpoint that is not an {@link OrderedCheckpoint},
     * which only real delivery, never this fixture's own writes, produces.
     */
    private static class OrderAwareCheckpointStorage extends InMemoryCheckpointStorage {

        private final AtomicInteger reads = new AtomicInteger();

        @Nullable Runnable whenTheFirstReadFindsNothing;

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.defer(() -> {
                boolean isTheFirstRead = reads.getAndIncrement() == 0;
                Mono<Checkpoint> read = super.read(subscriptionId);
                return isTheFirstRead && whenTheFirstReadFindsNothing != null
                        ? read.switchIfEmpty(Mono.fromRunnable(whenTheFirstReadFindsNothing))
                        : read;
            });
        }

        @Override
        public Mono<Checkpoint> resolveFirstCheckpointRace(String subscriptionId, Checkpoint candidate) {
            if (!(candidate instanceof OrderedCheckpoint candidateOrdered)) {
                return Mono.empty();
            }
            return super.read(subscriptionId)
                    .map(Optional::of)
                    .defaultIfEmpty(Optional.empty())
                    .flatMap(storedOptional -> {
                        if (storedOptional.isEmpty()) {
                            return super.save(subscriptionId, candidate, CheckpointWriteCondition.any());
                        }
                        Checkpoint stored = storedOptional.get();
                        if (!(stored instanceof OrderedCheckpoint storedOrdered)) {
                            return Mono.empty();
                        }
                        return storedOrdered.order() > candidateOrdered.order()
                                ? super.save(subscriptionId, candidate, CheckpointWriteCondition.any())
                                : Mono.just(stored);
                    });
        }

        /**
         * Writes the way another node would, without going through the read scripting above.
         */
        void writeWithoutScripting(Checkpoint checkpoint) {
            super.save(SUBSCRIPTION_ID, checkpoint, CheckpointWriteCondition.any()).block(TIMEOUT);
        }
    }

    private record OrderedCheckpoint(int order) implements Checkpoint {
        @Override
        public String asString() {
            return "order-" + order;
        }
    }
}
