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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * A subscribe of an id the wrapped model holds, but that its {@code isRunning(id)} and {@code isPaused(id)} miss,
 * gets past the check up front, stores a first position, and is then refused by the wrapped model. What the durable
 * model does with that position, and what it warns about when it can do nothing with it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelRefusedSubscribeTest {

    private static final String ID = "subscription";
    private static final Consumer<CloudEvent> NOTHING = __ -> {
    };

    @Nested
    class the_first_position_a_refused_subscribe_stored {

        @Test
        void is_deleted_so_nothing_is_stored_for_the_id() {
            CheckpointStorage storage = new InMemoryCheckpointStorage();
            DurableSubscriptionModel durable = new DurableSubscriptionModel(new RefusesWhatItCannotSee(), storage);
            durable.subscribe(ID, null, StartAt.now(), NOTHING);

            assertThatTheDuplicateIsRefused(durable);

            assertThat(storage.read(ID)).as("the first position the refused subscribe stored").isNull();
        }

        @Test
        void is_deleted_only_while_it_is_unchanged_so_a_checkpoint_the_running_subscription_wrote_meanwhile_stays() {
            CheckpointStorage storage = new InMemoryCheckpointStorage();
            RefusesWhatItCannotSee wrapped = new RefusesWhatItCannotSee();
            DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
            durable.subscribe(ID, null, StartAt.now(), NOTHING);
            wrapped.beforeRefusing = () -> wrapped.deliver("handled-by-the-running-subscription");

            assertThatTheDuplicateIsRefused(durable);

            assertThat(storage.read(ID))
                    .as("the checkpoint the running subscription wrote after the refused subscribe stored its first position")
                    .isEqualTo(new StringBasedCheckpoint("handled-by-the-running-subscription"));
        }

        @Test
        void that_cannot_be_deleted_is_suppressed_by_the_refusal_the_caller_gets() {
            RuntimeException deleteFailure = new RuntimeException("the storage failed to delete");
            CheckpointStorage storage = new InMemoryCheckpointStorage() {
                @Override
                public void deleteIfUnchanged(String subscriptionId, Checkpoint checkpoint, OptionalLong writeVersion) {
                    throw deleteFailure;
                }
            };
            RefusesWhatItCannotSee wrapped = new RefusesWhatItCannotSee();
            DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
            durable.subscribe(ID, null, StartAt.now(), NOTHING);

            DuplicateSubscriptionIdException refusal = assertThatTheDuplicateIsRefused(durable);

            assertThat(refusal).as("the refusal the wrapped model threw").isSameAs(wrapped.refusal);
            assertThat(refusal.getSuppressed())
                    .as("the failure to delete the first position the refused subscribe stored")
                    .containsExactly(deleteFailure);
        }

        @Test
        void that_cannot_be_deleted_because_the_storage_cannot_delete_only_if_unchanged_is_left_alone() {
            CheckpointStorage storage = new CannotDeleteOnlyIfUnchanged();
            RefusesWhatItCannotSee wrapped = new RefusesWhatItCannotSee();
            DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
            durable.subscribe(ID, null, StartAt.now(), NOTHING);

            DuplicateSubscriptionIdException refusal = assertThatTheDuplicateIsRefused(durable);

            assertThat(refusal).isSameAs(wrapped.refusal);
            assertThat(refusal.getSuppressed()).as("nothing was tried, so nothing failed").isEmpty();
            assertThat(storage.read(ID)).isEqualTo(new StringBasedCheckpoint(RefusesWhatItCannotSee.GLOBAL_POSITION));
        }

        @Test
        void is_deleted_when_the_refused_subscribe_was_held_paused() {
            CheckpointStorage storage = new InMemoryCheckpointStorage();
            DurableSubscriptionModel durable = new DurableSubscriptionModel(new RefusesWhatItCannotSee(), storage);
            durable.subscribe(ID, null, StartAt.now(), NOTHING);

            DuplicateSubscriptionIdException refusal = catchThrowableOfType(DuplicateSubscriptionIdException.class,
                    () -> durable.subscribePaused(ID, null, StartAt.subscriptionModelDefault(), NOTHING));

            assertThat(refusal).as("the refusal of a paused subscribe of an id the wrapped model holds").isNotNull();
            assertThat(storage.read(ID)).as("the first position the refused paused subscribe stored").isNull();
        }

        @Test
        void is_deleted_when_a_dynamic_start_answered_the_model_default() {
            CheckpointStorage storage = new InMemoryCheckpointStorage();
            DurableSubscriptionModel durable = new DurableSubscriptionModel(new RefusesWhatItCannotSee(), storage);
            durable.subscribe(ID, null, StartAt.now(), NOTHING);

            DuplicateSubscriptionIdException refusal = catchThrowableOfType(DuplicateSubscriptionIdException.class,
                    () -> durable.subscribe(ID, null, StartAt.dynamic(__ -> StartAt.subscriptionModelDefault()), NOTHING));

            assertThat(refusal).as("the refusal of a subscribe of an id the wrapped model holds").isNotNull();
            assertThat(storage.read(ID)).as("the first position the refused subscribe stored for its dynamic start").isNull();
        }
    }

    @Nested
    class a_refused_subscribe_that_stored_no_first_position {

        @Test
        void leaves_the_checkpoint_that_was_stored_before_it() {
            CheckpointStorage storage = new InMemoryCheckpointStorage();
            DurableSubscriptionModel durable = new DurableSubscriptionModel(new RefusesWhatItCannotSee(), storage);
            durable.subscribe(ID, null, StartAt.now(), NOTHING);
            storage.save(ID, new StringBasedCheckpoint("stored-before"));

            assertThatTheDuplicateIsRefused(durable);

            assertThat(storage.read(ID))
                    .as("the checkpoint stored before the refused subscribe, which it read and did not write")
                    .isEqualTo(new StringBasedCheckpoint("stored-before"));
        }

        @Test
        void leaves_the_first_position_another_node_stored_while_it_tried_to_store_its_own() {
            // Stores the same position for another node just before the ifAbsent() write of this node, which then
            // finds it stored and is refused
            CheckpointStorage storage = new InMemoryCheckpointStorage() {
                @Override
                public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                    if (condition instanceof CheckpointWriteCondition.IfAbsent) {
                        super.save(subscriptionId, checkpoint, CheckpointWriteCondition.ifAbsent());
                    }
                    return super.save(subscriptionId, checkpoint, condition);
                }
            };
            DurableSubscriptionModel durable = new DurableSubscriptionModel(new RefusesWhatItCannotSee(), storage);
            durable.subscribe(ID, null, StartAt.now(), NOTHING);

            assertThatTheDuplicateIsRefused(durable);

            assertThat(storage.read(ID))
                    .as("the first position the other node stored, which the refused subscribe adopted and did not write")
                    .isEqualTo(new StringBasedCheckpoint(RefusesWhatItCannotSee.GLOBAL_POSITION));
        }
    }

    @Nested
    class a_subscribe_of_an_id_the_wrapped_model_reports_it_holds {

        @Test
        void is_refused_without_touching_the_storage_when_the_wrapped_model_lists_the_id() {
            assertRefusedWithoutTouchingTheStorage(new ListsWhatItHolds());
        }

        @Test
        void is_refused_without_touching_the_storage_when_the_wrapped_model_answers_that_it_runs() {
            assertRefusedWithoutTouchingTheStorage(new AnswersFor(true, false));
        }

        @Test
        void is_refused_without_touching_the_storage_when_the_wrapped_model_answers_that_it_is_paused() {
            assertRefusedWithoutTouchingTheStorage(new AnswersFor(false, true));
        }

        private void assertRefusedWithoutTouchingTheStorage(RefusesWhatItCannotSee wrapped) {
            CountsCalls storage = new CountsCalls();
            DurableSubscriptionModel durable = new DurableSubscriptionModel(wrapped, storage);
            durable.subscribe(ID, null, StartAt.now(), NOTHING);
            storage.calls.clear();

            assertThatTheDuplicateIsRefused(durable);

            assertThat(storage.calls).as("the storage calls the refused subscribe made").isEmpty();
            assertThat(wrapped.refusal).as("the refusal of the wrapped model, which was never asked").isNull();
        }
    }

    @Nested
    class at_construction {

        @Test
        void warns_when_the_storage_cannot_delete_only_if_unchanged_and_the_wrapped_model_cannot_list_its_subscriptions() {
            List<String> warnings = warningsLoggedWhileCreating(new RefusesWhatItCannotSee(), new CannotDeleteOnlyIfUnchanged());

            assertThat(warnings).singleElement().satisfies(warning -> assertThat(warning)
                    .contains(CannotDeleteOnlyIfUnchanged.class.getName())
                    .contains(RefusesWhatItCannotSee.class.getName())
                    .contains("deleteIfUnchanged(String, Checkpoint, OptionalLong)")
                    .contains("IntrospectableSubscriptions"));
        }

        @Test
        void does_not_warn_when_the_storage_can_delete_only_if_unchanged() {
            assertThat(warningsLoggedWhileCreating(new RefusesWhatItCannotSee(), new InMemoryCheckpointStorage())).isEmpty();
        }

        @Test
        void does_not_warn_when_the_wrapped_model_can_list_its_subscriptions() {
            assertThat(warningsLoggedWhileCreating(new ListsItsSubscriptions(), new CannotDeleteOnlyIfUnchanged())).isEmpty();
        }

        private List<String> warningsLoggedWhileCreating(CheckpointAwareSubscriptionModel wrapped, CheckpointStorage storage) {
            Logger logger = (Logger) LoggerFactory.getLogger(DurableSubscriptionModel.class);
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            logger.addAppender(appender);
            try {
                new DurableSubscriptionModel(wrapped, storage);
            } finally {
                logger.detachAppender(appender);
            }
            return appender.list.stream()
                    .filter(event -> event.getLevel().equals(Level.WARN))
                    .map(ILoggingEvent::getFormattedMessage)
                    .filter(message -> message.contains("deleteIfUnchanged"))
                    .toList();
        }
    }

    private static DuplicateSubscriptionIdException assertThatTheDuplicateIsRefused(DurableSubscriptionModel durable) {
        DuplicateSubscriptionIdException refusal = catchThrowableOfType(DuplicateSubscriptionIdException.class, () -> durable.subscribe(ID, NOTHING));
        assertThat(refusal).as("the refusal of a subscribe of an id the wrapped model holds").isNotNull();
        return refusal;
    }

    /**
     * Holds the action of the first subscribe of an id and refuses every later one, but answers {@code false} from
     * {@code isRunning(id)} and {@code isPaused(id)}, standing in for a model whose answers can miss an id it holds.
     */
    private static class RefusesWhatItCannotSee implements CheckpointAwareSubscriptionModel {
        static final String GLOBAL_POSITION = "global";

        final Map<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();
        volatile Runnable beforeRefusing = () -> {
        };
        volatile @Nullable DuplicateSubscriptionIdException refusal;

        void deliver(String checkpoint) {
            CloudEvent cloudEvent = CloudEventBuilder.v1()
                    .withId(checkpoint)
                    .withSource(URI.create("urn:occurrent:test"))
                    .withType("Created")
                    .build();
            requireNonNull(actions.get(ID)).accept(new CheckpointAwareCloudEvent(cloudEvent, new StringBasedCheckpoint(checkpoint)));
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (actions.putIfAbsent(subscriptionId, action) != null) {
                beforeRefusing.run();
                DuplicateSubscriptionIdException refused = new DuplicateSubscriptionIdException(subscriptionId);
                refusal = refused;
                throw refused;
            }
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

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return new StringBasedCheckpoint(GLOBAL_POSITION);
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
            return false;
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return false;
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            actions.remove(subscriptionId);
        }
    }

    /**
     * Answers {@code false} from {@code isRunning(id)} and {@code isPaused(id)} but lists the ids it holds, so only
     * {@code subscriptionIds()} tells the durable model that it holds the id.
     */
    private static final class ListsWhatItHolds extends RefusesWhatItCannotSee implements IntrospectableSubscriptions {
        @Override
        public Set<String> subscriptionIds() {
            return Set.copyOf(actions.keySet());
        }
    }

    private static final class AnswersFor extends RefusesWhatItCannotSee {
        private final boolean running;
        private final boolean paused;

        AnswersFor(boolean running, boolean paused) {
            this.running = running;
            this.paused = paused;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return running && actions.containsKey(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused && actions.containsKey(subscriptionId);
        }
    }

    private static final class CountsCalls extends InMemoryCheckpointStorage {
        final List<String> calls = new CopyOnWriteArrayList<>();

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            calls.add("read");
            return super.read(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            calls.add("save");
            return super.save(subscriptionId, checkpoint, condition);
        }

        @Override
        public void deleteIfUnchanged(String subscriptionId, Checkpoint checkpoint, OptionalLong writeVersion) {
            calls.add("deleteIfUnchanged");
            super.deleteIfUnchanged(subscriptionId, checkpoint, writeVersion);
        }
    }

    private static final class ListsItsSubscriptions extends RefusesWhatItCannotSee implements IntrospectableSubscriptions {
        @Override
        public Set<String> subscriptionIds() {
            return Set.of();
        }
    }

    /**
     * Stores every checkpoint it is given, whatever the condition, and leaves {@code deleteIfUnchanged} and
     * {@code deletesIfUnchanged} at their defaults.
     */
    private static final class CannotDeleteOnlyIfUnchanged implements CheckpointStorage {
        private final Map<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            return checkpoints.get(subscriptionId);
        }

        @Override
        public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            checkpoints.put(subscriptionId, checkpoint);
            return checkpoint;
        }

        @Override
        public OptionalLong writeVersion(String subscriptionId) {
            return OptionalLong.empty();
        }

        @Override
        public void delete(String subscriptionId) {
            checkpoints.remove(subscriptionId);
        }

        @Override
        public boolean exists(String subscriptionId) {
            return checkpoints.containsKey(subscriptionId);
        }
    }
}
