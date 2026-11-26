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
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.PreparedResumeSubscriptions;
import org.occurrent.subscription.api.blocking.PreparedResumeSubscriptions.PreparedResume;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link DurableSubscriptionModel} splits a resume into {@link PreparedResumeSubscriptions#prepareResume(String)}, which reads
 * storage, and {@link PreparedResumeSubscriptions#resumeAsPrepared(String, PreparedResume)}, which reads none, so a caller can
 * read outside a lock of its own. The resume as prepared goes where {@code resumeSubscription(id)} would have gone, and
 * gives the caller an empty answer to read again when what was read moved.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class DurableSubscriptionModelPreparedResumeTest {

    private static final String ID = "subscription";
    private static final Checkpoint STORED = new StringBasedCheckpoint("stored-checkpoint");

    private final CountingStorage storage = new CountingStorage();
    private final RepositionableDelegate delegate = new RepositionableDelegate();
    private final DurableSubscriptionModel model = new DurableSubscriptionModel(delegate, storage);

    @Test
    void the_model_is_found_as_one_that_splits_a_resume() {
        assertThat(PreparedResumeSubscriptions.findIn(model)).as("the capability found in the durable model").containsSame(model);
    }

    @Test
    void a_resume_as_prepared_goes_where_the_checkpoint_read_by_the_preparation_says_and_reads_no_storage() {
        // Given a stored checkpoint and a resume prepared from it
        storage.save(ID, STORED);
        PreparedResume prepared = model.prepareResume(ID);
        int readsByThePreparation = storage.reads.get();
        assertThat(readsByThePreparation).as("the storage reads the preparation made").isPositive();
        assertThat(delegate.repositionedTo).as("where the delegate was resumed by the preparation").isNull();

        // When the resume is made as prepared
        Optional<Subscription> resumed = model.resumeAsPrepared(ID, prepared);

        // Then the delegate is repositioned to the checkpoint that was read, and no storage was read for it
        assertThat(resumed).as("the subscription resumed as prepared").isPresent();
        assertThat(delegate.repositionedTo).as("what the delegate was repositioned to").isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class, startAt -> assertThat(startAt.checkpoint).isEqualTo(STORED));
        assertThat(storage.reads.get()).as("the storage reads the resume as prepared made on top of the preparation's").isEqualTo(readsByThePreparation);
    }

    @Test
    void a_resume_as_prepared_with_no_stored_checkpoint_resumes_the_delegate_where_it_is() {
        // Given nothing stored and a resume prepared
        PreparedResume prepared = model.prepareResume(ID);

        // When the resume is made as prepared
        Optional<Subscription> resumed = model.resumeAsPrepared(ID, prepared);

        // Then the delegate is resumed by its own resume
        assertThat(resumed).as("the subscription resumed as prepared").isPresent();
        assertThat(delegate.plainResumes.get()).as("the delegate's own resumes").isEqualTo(1);
        assertThat(delegate.repositionedTo).as("what the delegate was repositioned to").isNull();
    }

    @Test
    void a_resume_as_prepared_is_empty_and_resumes_nothing_when_a_checkpoint_was_stored_since_the_preparation_and_a_new_preparation_goes_where_that_checkpoint_says() {
        // Given a subscription, a resume prepared, and an event delivered after it that stored a checkpoint
        Consumer<CloudEvent> action = event -> {
        };
        model.subscribe(ID, null, StartAt.now(), action);
        PreparedResume prepared = model.prepareResume(ID);
        delegate.deliver(checkpointed("stored-after-the-preparation"));

        // When the resume is made as prepared
        Optional<Subscription> resumed = model.resumeAsPrepared(ID, prepared);

        // Then nothing is resumed, and a resume prepared again goes to the checkpoint stored since
        assertThat(resumed).as("the subscription resumed from a preparation that was read before the checkpoint was stored").isEmpty();
        assertThat(delegate.resumes()).as("the resumes the delegate got from the stale preparation").isZero();
        Optional<Subscription> resumedAgain = model.resumeAsPrepared(ID, model.prepareResume(ID));
        assertThat(resumedAgain).as("the subscription resumed from a preparation read after the checkpoint was stored").isPresent();
        assertThat(delegate.repositionedTo).as("what the delegate was repositioned to").isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("stored-after-the-preparation"));
    }

    @Test
    void a_resume_as_prepared_is_empty_without_waiting_when_another_call_for_the_subscription_is_inside_a_storage_read() throws Exception {
        // Given a prepared resume, and a resume of the same subscription from another thread that is held inside the storage read
        storage.save(ID, STORED);
        PreparedResume prepared = model.prepareResume(ID);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        storage.holdReads(entered, release);
        CompletableFuture<Subscription> other = CompletableFuture.supplyAsync(() -> model.resumeSubscription(ID));
        try {
            assertThat(entered.await(10, TimeUnit.SECONDS)).as("whether the resume from the other thread reached the storage read").isTrue();

            // When the resume is made as prepared
            CompletableFuture<Optional<Subscription>> asPrepared = CompletableFuture.supplyAsync(() -> model.resumeAsPrepared(ID, prepared));

            // Then it answers empty at once, as the holder of the subscription's lock can be in a storage call
            assertThat(asPrepared).as("the resume as prepared made while another call held the subscription").succeedsWithin(Duration.ofSeconds(10)).satisfies(result -> assertThat(result).isEmpty());
        } finally {
            release.countDown();
        }
        assertThat(other).as("the resume that was held inside the storage read").succeedsWithin(Duration.ofSeconds(10));
    }

    @Test
    void a_resume_as_prepared_for_a_preparation_made_for_another_subscription_or_model_is_refused() {
        // Given a resume prepared for one subscription by this model, and one prepared by another model
        PreparedResume preparedForTheSubscription = model.prepareResume(ID);
        PreparedResume preparedByAnotherModel = new DurableSubscriptionModel(new RepositionableDelegate(), new InMemoryCheckpointStorage()).prepareResume(ID);

        // When they are used for another subscription, and by this model
        // Then both are refused, and nothing is resumed
        assertThatThrownBy(() -> model.resumeAsPrepared("another-subscription", preparedForTheSubscription)).as("a preparation for another subscription").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> model.resumeAsPrepared(ID, preparedByAnotherModel)).as("a preparation by another model").isInstanceOf(IllegalArgumentException.class);
        assertThat(delegate.resumes()).as("the resumes the delegate got").isZero();
    }

    private static CloudEvent checkpointed(String checkpoint) {
        CloudEvent event = CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("test.event").build();
        return new CheckpointAwareCloudEvent(event, new StringBasedCheckpoint(checkpoint));
    }

    // A storage that counts the reads and can hold the next one
    private static final class CountingStorage extends InMemoryCheckpointStorage {
        final AtomicInteger reads = new AtomicInteger();
        private volatile @Nullable CountDownLatch entered;
        private volatile @Nullable CountDownLatch release;

        void holdReads(CountDownLatch entered, CountDownLatch release) {
            this.release = release;
            this.entered = entered;
        }

        @Override
        public @Nullable Checkpoint read(String subscriptionId) {
            reads.incrementAndGet();
            CountDownLatch holding = entered;
            if (holding != null) {
                entered = null;
                holding.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            return super.read(subscriptionId);
        }
    }

    // A wrapped model that can be repositioned, and records how it was resumed
    private static final class RepositionableDelegate implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions {
        final AtomicInteger plainResumes = new AtomicInteger();
        volatile @Nullable StartAt repositionedTo;
        private volatile @Nullable Consumer<CloudEvent> action;

        void deliver(CloudEvent event) {
            action.accept(event);
        }

        int resumes() {
            return plainResumes.get() + (repositionedTo == null ? 0 : 1);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            this.action = action;
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            repositionedTo = startAt;
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            plainResumes.incrementAndGet();
            return subscription(subscriptionId);
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return null;
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
            return true;
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
        }

        private static Subscription subscription(String subscriptionId) {
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
