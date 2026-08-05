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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;

import java.time.Duration;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins where a subscription starts from when it was registered while the model was stopped. Registering reads the
 * current position and holds it, starting writes it if nothing is stored, so waiting withholds events rather than
 * losing them. Uses hand-rolled fakes rather than MongoDB, because what matters is exactly when the position is read
 * and written, which a real database makes harder to see rather than easier.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorDurableSubscriptionModelStoppedRegistrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    private static final String SUBSCRIPTION_ID = "someSubscription";

    @Test
    void registering_while_stopped_stores_nothing() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        SaveCountingCheckpointStorage storage = new SaveCountingCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThat(storage.read(SUBSCRIPTION_ID).blockOptional(TIMEOUT)).isEmpty();
        // Not just "nothing is stored": save was never invoked at all. Over a cold storage, an emptiness read alone
        // is also satisfied by a save whose returned Mono was assembled and dropped, which is its own defect.
        assertThat(storage.saves).hasValue(0);
        assertThat(delegate.startedAt).isEmpty();
        assertThat(model.isPaused(SUBSCRIPTION_ID)).isTrue();
    }

    @Test
    void starting_it_later_begins_where_it_was_registered_rather_than_where_the_feed_has_reached() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        delegate.globalCheckpoint = new StringBasedCheckpoint("much-later");
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT).asString()).isEqualTo("at-registration");
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("at-registration");
    }

    @Test
    void a_subscription_that_already_has_a_stored_position_keeps_it() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("from-a-previous-run")).block(TIMEOUT);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT).asString()).isEqualTo("from-a-previous-run");
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("from-a-previous-run");
    }

    @Test
    void a_position_that_cannot_be_read_at_registration_is_read_again_when_the_subscription_starts() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpoint = true;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        delegate.failGlobalCheckpoint = false;
        delegate.globalCheckpoint = new StringBasedCheckpoint("read-again-at-start");
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT).asString()).isEqualTo("read-again-at-start");
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();
    }

    @Test
    void a_dynamic_start_position_is_evaluated_when_the_subscription_starts_not_when_it_is_registered() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        AtomicInteger evaluations = new AtomicInteger();
        StartAt dynamic = StartAt.dynamic(() -> {
            evaluations.incrementAndGet();
            return StartAt.subscriptionModelDefault();
        });

        model.subscribe(SUBSCRIPTION_ID, null, dynamic, __ -> Mono.empty());
        assertThat(evaluations).hasValue(0);

        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(evaluations.get()).isPositive();
        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT).asString()).isEqualTo("at-registration");
    }

    @Test
    void a_dynamic_start_position_that_opts_out_still_starts_without_storing_a_position() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        SaveCountingCheckpointStorage storage = new SaveCountingCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        StartAt optOut = StartAt.dynamic(() -> null);

        model.subscribe(SUBSCRIPTION_ID, null, optOut, __ -> Mono.empty());
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.read(SUBSCRIPTION_ID).blockOptional(TIMEOUT)).isEmpty();
        assertThat(storage.saves).hasValue(0);
        assertThat(delegate.startedAt).isNotEmpty();
    }

    @Test
    void registering_while_running_behaves_as_it_did_before() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT).asString()).isEqualTo("at-registration");
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();
    }

    private static String startedAtCheckpoint(RecordingSubscriptionModel delegate) {
        assertThat(delegate.startedAt).hasSize(1);
        assertThat(delegate.startedAt.getFirst()).isInstanceOf(StartAt.StartAtCheckpoint.class);
        return ((StartAt.StartAtCheckpoint) delegate.startedAt.getFirst()).checkpoint.asString();
    }

    /**
     * Counts {@code save} invocations at the point of the call, deliberately before the returned {@code Mono} runs:
     * the guard is "the model never called save", and counting at subscription time would let an assembled-and-dropped
     * save go unnoticed, which is the defect class the count exists to catch.
     */
    private static final class SaveCountingCheckpointStorage extends InMemoryCheckpointStorage {

        final AtomicInteger saves = new AtomicInteger();

        @Override
        public Mono<org.occurrent.subscription.Checkpoint> save(String subscriptionId, org.occurrent.subscription.Checkpoint checkpoint) {
            saves.incrementAndGet();
            return super.save(subscriptionId, checkpoint);
        }
    }

}
