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

package org.occurrent.subscription.blocking.durable;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StartAt.SubscriptionModelContext;
import org.occurrent.subscription.StartPositionAlreadyPinnedException;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.blocking.durable.WinnerHiddenFromTheConfirmReadStorage.ConfirmRead;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code recordFirstPositionOrRefuse} reads storage, finds nothing, then writes what the wrapped model answers for
 * {@code globalCheckpoint()}. A checkpoint that lands in storage between those two calls, from another node
 * registering the same subscription id concurrently, must not be silently overwritten by this node's write, the
 * same guarantee {@code ManualStartSubscriptionModel} and {@code ReactorDurableSubscriptionModel} already give.
 * <p>
 * A hand-rolled storage rather than MongoDB, because this is about the order two calls reach storage in, which a
 * real database hides.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelFirstPositionRaceTest {

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void a_registration_that_loses_the_first_position_write_to_a_checkpoint_it_did_not_read_is_refused_rather_than_overwriting_it() {
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        // Another node's registration for the same subscription id wins the write in between this node's read
        // (which finds nothing) and its own write.
        storage.whenTheFirstReadFindsNothing = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("landed-during-registration"));
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(feed, storage);

        assertThatThrownBy(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        }))
                .as("the position stored is not the one this registration read, so starting from it would skip whatever the other node's own subscription is about to see")
                .isInstanceOf(StartPositionAlreadyPinnedException.class);

        assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString())
                .as("the other node's checkpoint must survive this node's losing write untouched")
                .isEqualTo("landed-during-registration");
    }

    @Test
    void a_registration_that_loses_the_write_from_inside_the_dynamic_supplier_adopts_the_winning_position_instead_of_throwing_from_inside_it() {
        // startWhenNoStartPositionCanBeRecorded(true) plus a feed that answers null for globalCheckpoint() only on
        // the eager, outside-the-supplier call lets recordFirstPositionOrRefuse return null without ever writing,
        // so the dynamic supplier's own retry-path branch runs on this registration's very first (and only)
        // evaluation, the same branch a wrapped model's own retry loop could otherwise evaluate more than once.
        RaceSimulatingCheckpointStorage storage = new RaceSimulatingCheckpointStorage();
        storage.whenTheSecondReadFindsNothing = () -> storage.delegate.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("landed-during-registration"));
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        feed.answersNullOnFirstCallOnly = true;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(
                feed, storage, new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));

        // Must not throw. Reading the stored position back named it, and it is where every later evaluation starts
        // from too, so refusing it would only delay a start that cannot skip anything.
        assertThatCode(() -> durable.subscribe(SUBSCRIPTION_ID, __ -> {
        })).doesNotThrowAnyException();

        assertThat(storage.delegate.read(SUBSCRIPTION_ID).asString())
                .as("the other node's already-stored position is adopted rather than lost or refused")
                .isEqualTo("landed-during-registration");
    }

    @Test
    void a_lost_first_position_write_whose_confirm_read_fails_never_starts_from_this_nodes_later_position() {
        aLostFirstPositionWriteWhoseConfirmReadCannotNameTheWinnerNeverStartsFromThisNodesLaterPosition(ConfirmRead.FAILS);
    }

    @Test
    void a_lost_first_position_write_whose_confirm_read_finds_nothing_never_starts_from_this_nodes_later_position() {
        aLostFirstPositionWriteWhoseConfirmReadCannotNameTheWinnerNeverStartsFromThisNodesLaterPosition(ConfirmRead.FINDS_NOTHING);
    }

    private static void aLostFirstPositionWriteWhoseConfirmReadCannotNameTheWinnerNeverStartsFromThisNodesLaterPosition(ConfirmRead confirmRead) {
        // The other node read the feed first, so the position it stored is earlier than the one this node reads
        // below. Starting from this node's position would skip every event between the two.
        WinnerHiddenFromTheConfirmReadStorage storage = new WinnerHiddenFromTheConfirmReadStorage(confirmRead, new StringBasedCheckpoint("earlier-position-the-other-node-stored"));
        InMemoryFeed feed = new InMemoryFeed();
        feed.answersCurrentPosition = true;
        feed.currentPosition = "later-position-this-node-read";
        feed.answersNullOnFirstCallOnly = true;
        feed.keepsGoingWhenTheStartPositionThrows = true;
        DurableSubscriptionModel durable = new DurableSubscriptionModel(
                feed, storage, new DurableSubscriptionModelConfig(1).startWhenNoStartPositionCanBeRecorded(true));

        durable.subscribe(SUBSCRIPTION_ID, __ -> {
        });

        assertThat(feed.positionsStartedFrom)
                .as("the confirm-read could not name what the other node stored, so this node's own position cannot be ordered against it")
                .doesNotContain("later-position-this-node-read");
        assertThat(feed.startPositionFailures)
                .singleElement()
                .isInstanceOfSatisfying(StartPositionAlreadyPinnedException.class, refusal -> {
                    assertThat(refusal.positionRead.asString()).isEqualTo("later-position-this-node-read");
                    assertThat(refusal.positionStored).isEmpty();
                    assertThat(refusal).hasMessageContaining("could skip the events between the two");
                    if (confirmRead == ConfirmRead.FAILS) {
                        assertThat(refusal.getCause())
                                .as("a read back that failed is told apart by its cause, the failure of the read itself")
                                .hasMessage("Checkpoint storage cannot be reached");
                    } else {
                        assertThat(refusal.getCause())
                                .as("a read back that found nothing is told apart by having no cause")
                                .isNull();
                    }
                });

        storage.answersReadsAgain();
        feed.evaluateTheStartPositionAgain(SUBSCRIPTION_ID);

        assertThat(feed.positionsStartedFrom)
                .as("evaluated again once storage answers, the start is the position the other node stored")
                .containsExactly("earlier-position-the-other-node-stored");
    }

    /**
     * Delegates every call to a real {@link InMemoryCheckpointStorage}, except that the first {@link #read} for a
     * subscription id runs {@link #whenTheFirstReadFindsNothing} right after finding nothing, before this node's
     * own write reaches storage. That reproduces the interleaving a real race needs without a second thread.
     */
    private static final class RaceSimulatingCheckpointStorage implements CheckpointStorage {
        final InMemoryCheckpointStorage delegate = new InMemoryCheckpointStorage();
        @Nullable Runnable whenTheFirstReadFindsNothing;
        @Nullable Runnable whenTheSecondReadFindsNothing;
        private int nullReadCount = 0;

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            Checkpoint found = delegate.read(subscriptionId);
            if (found == null) {
                nullReadCount++;
                if (nullReadCount == 1 && whenTheFirstReadFindsNothing != null) {
                    Runnable hook = whenTheFirstReadFindsNothing;
                    whenTheFirstReadFindsNothing = null;
                    hook.run();
                } else if (nullReadCount == 2 && whenTheSecondReadFindsNothing != null) {
                    Runnable hook = whenTheSecondReadFindsNothing;
                    whenTheSecondReadFindsNothing = null;
                    hook.run();
                }
            }
            return found;
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, org.occurrent.subscription.CheckpointWriteCondition condition) {
            return delegate.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public boolean evaluatesWriteConditions() {
            return delegate.evaluatesWriteConditions();
        }

        @Override
        public java.util.OptionalLong writeVersion(String subscriptionId) {
            return delegate.writeVersion(subscriptionId);
        }

        @Override
        public void delete(String subscriptionId) {
            delegate.delete(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return delegate.exists(subscriptionId);
        }
    }

    /**
     * A feed with change-stream mechanics reduced to what this test needs: the model default means the end of what
     * has been published so far, and a subscription is registered but never delivered to.
     */
    private static final class InMemoryFeed implements CheckpointAwareSubscriptionModel {
        final Map<String, Boolean> subscriptions = new LinkedHashMap<>();
        boolean answersCurrentPosition = false;
        // Set only by the tests that need the eager, outside-the-supplier globalCheckpoint() call to answer
        // unanswerable, so recordFirstPositionOrRefuse returns null without writing anything and the dynamic
        // supplier's own retry-path branch is what asks again.
        boolean answersNullOnFirstCallOnly = false;
        String currentPosition = "this-nodes-own-position";
        // Set by the tests that stand in for a model evaluating the start position on a thread of its own, the way
        // the MongoDB models do, where a throw is logged and the evaluation is tried again rather than reaching
        // the caller of subscribe
        boolean keepsGoingWhenTheStartPositionThrows = false;
        private int globalCheckpointCalls = 0;
        final List<String> positionsStartedFrom = new ArrayList<>();
        final List<Throwable> startPositionFailures = new ArrayList<>();
        private final Map<String, StartAt> startAts = new LinkedHashMap<>();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            startAts.put(subscriptionId, startAt);
            if (startAt.isDynamic()) {
                evaluateTheStartPositionAgain(subscriptionId);
            }
            subscriptions.put(subscriptionId, true);
            return dummySubscription(subscriptionId);
        }

        void evaluateTheStartPositionAgain(String subscriptionId) {
            @Nullable StartAt resolved;
            try {
                resolved = startAts.get(subscriptionId).get(new SubscriptionModelContext(InMemoryFeed.class));
            } catch (RuntimeException e) {
                if (!keepsGoingWhenTheStartPositionThrows) {
                    throw e;
                }
                startPositionFailures.add(e);
                return;
            }
            if (resolved instanceof StartAt.StartAtCheckpoint checkpoint) {
                positionsStartedFrom.add(checkpoint.checkpoint.asString());
            }
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            globalCheckpointCalls++;
            if (answersNullOnFirstCallOnly && globalCheckpointCalls == 1) {
                return null;
            }
            return answersCurrentPosition ? new StringBasedCheckpoint(currentPosition) : null;
        }

        @Override
        public void shutdown() {
            subscriptions.clear();
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return subscriptions.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            return dummySubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            subscriptions.remove(subscriptionId);
        }

        private static Subscription dummySubscription(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }
    }
}
