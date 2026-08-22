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
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.reactor.SubscriptionHandle;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins where a subscription starts from when it was registered while the model was stopped. Registering reads the
 * current position and holds it, starting writes it if nothing is stored, so waiting withholds events rather than
 * losing them. A position that could not be read at registration, whether the read failed or answered nothing, refuses
 * the subscription instead of being read again later, since a later read answers past everything written in between.
 * Uses hand-rolled fakes rather than MongoDB, because what matters is exactly when the position is read and written,
 * which a real database makes harder to see rather than easier.
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
    void a_checkpoint_deleted_and_rewritten_to_a_later_position_while_the_subscription_waited_is_resolved_by_order_not_taken_on_presence_alone() {
        // #771's second hole on this stack: this node's own position was captured at registration, long before
        // storage is read again here, at start. cancelSubscription deletes a checkpoint, so a delete followed by
        // another node's registration is reachable in that gap, and trusting storage.read() the way
        // a_subscription_that_already_has_a_stored_position_keeps_it does would take whatever it finds without
        // asking whether it is safe to. A storage able to order the two positions is what tells that apart from the
        // ordinary case the test above covers.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.globalCheckpoint = new OrderedCheckpoint(10);
        OrderAwareCheckpointStorage storage = new OrderAwareCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        // Simulates another node cancelling and re-registering this subscription, with a later position, while this
        // one waited to be started.
        storage.writeWithoutScripting(new OrderedCheckpoint(50));

        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT).asString())
                .as("this node's earlier, registration-time position replaces the later one that arrived while it waited")
                .isEqualTo("order-10");
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("order-10");
    }

    @Test
    void a_checkpoint_deleted_and_rewritten_to_an_earlier_position_while_the_subscription_waited_is_left_alone_when_the_storage_can_order_them() {
        // The mirror image of the test above: the checkpoint that appears while this subscription waits is earlier
        // than this node's own registration-time position, the ordinary "another node is already ahead" case, so it
        // is left exactly as storage.read() would already have left it, only now confirmed rather than assumed.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.globalCheckpoint = new OrderedCheckpoint(40);
        OrderAwareCheckpointStorage storage = new OrderAwareCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        storage.writeWithoutScripting(new OrderedCheckpoint(7));

        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT).asString())
                .as("the stored position is already earlier than this node's own, so it stays exactly as it was")
                .isEqualTo("order-7");
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("order-7");
    }

    @Test
    void a_resolver_failure_refuses_the_subscription_rather_than_trusting_the_later_checkpoint() {
        // A resolver outage must not read the same way as a storage saying it cannot compare the two positions.
        // The first is silence, the second is an answer, and only the second is safe to fall back to the stored
        // checkpoint from.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.globalCheckpoint = new OrderedCheckpoint(10);
        FailingResolveCheckpointStorage storage = new FailingResolveCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        // Simulates another node rewriting the checkpoint to a later position while this one waited to be started.
        storage.writeWithoutScripting(new OrderedCheckpoint(50));

        SubscriptionHandle resumed = model.resumeSubscription(SUBSCRIPTION_ID);

        assertThatThrownBy(() -> resumed.waitUntilStarted().block(TIMEOUT))
                .as("the resolver's own failure reaches the caller instead of being read as the later checkpoint being safe")
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("resolver unavailable");
        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT).asString())
                .as("the stored checkpoint is untouched by a resolution that never completed")
                .isEqualTo("order-50");
    }

    @Test
    void a_position_that_cannot_be_read_at_registration_refuses_the_subscription_rather_than_reading_it_again() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpoint = true;
        SaveCountingCheckpointStorage storage = new SaveCountingCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        // The feed moved on while the subscription waited, so a read taken now answers past everything written since
        // the registration. Starting from that is the loss, which is why the read is not taken.
        delegate.failGlobalCheckpoint = false;
        delegate.globalCheckpoint = new StringBasedCheckpoint("much-later");
        SubscriptionHandle resumed = model.resumeSubscription(SUBSCRIPTION_ID);

        assertThatThrownBy(() -> resumed.waitUntilStarted().block(TIMEOUT))
                .as("the read that failed is what reaches the caller, unwrapped")
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot read the position right now");
        assertThat(storage.read(SUBSCRIPTION_ID).blockOptional(TIMEOUT)).isEmpty();
        assertThat(storage.saves).hasValue(0);
        assertThat(delegate.startedAt).isEmpty();
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isFalse();
        assertThat(model.isPaused(SUBSCRIPTION_ID))
                .as("a refused subscription is dropped rather than left registered, so getting it back means registering again")
                .isFalse();
    }

    @Test
    void a_position_that_answers_nothing_at_registration_refuses_the_subscription_the_same_way() {
        // Answering nothing is the documented way the wrapped model reports a problem it cannot resolve, so it says
        // as little about where the feed is as a read that failed outright, and is refused for the same reason.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.globalCheckpoint = null;
        SaveCountingCheckpointStorage storage = new SaveCountingCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        delegate.globalCheckpoint = new StringBasedCheckpoint("much-later");
        SubscriptionHandle resumed = model.resumeSubscription(SUBSCRIPTION_ID);

        assertThatThrownBy(() -> resumed.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(SUBSCRIPTION_ID)
                .hasMessageContaining("answered nothing")
                .hasMessageContaining("cancelSubscription(String)")
                .hasMessageContaining("StartAt of your own");
        assertThat(storage.saves).hasValue(0);
        assertThat(delegate.startedAt).isEmpty();
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isFalse();
        assertThat(model.isPaused(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void a_position_that_could_not_be_read_is_reported_on_the_handle_the_registration_returned() {
        // Nothing forces a caller to wait for the resume, so the handle it already holds has to carry the reason too.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpoint = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, new InMemoryCheckpointStorage());
        model.stop();

        SubscriptionHandle registration = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> registration.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot read the position right now");
    }

    @Test
    void a_position_that_was_read_leaves_the_registration_handle_waiting_as_it_did_before() {
        // The handle must not start answering just because the position was read. The subscription has not started
        // and will not until it is asked to, and a caller waiting on it is waiting for delivery rather than for a read.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, new InMemoryCheckpointStorage());
        model.stop();

        SubscriptionHandle registration = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> registration.waitUntilStarted().block(Duration.ofMillis(200)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Timeout on blocking read");
        assertThat(model.isPaused(SUBSCRIPTION_ID)).isTrue();
    }

    @Test
    void a_registration_naming_its_own_position_is_not_touched_by_a_read_that_could_not_answer() {
        // Such a registration begins where it asked to, whatever the feed does while it waits, so there is nothing
        // here for a read that cannot answer to cost. Reading for it at all would only produce a refusal for a
        // subscription that goes on to start, which is also the way out the refusal names.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpoint = true;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();

        SubscriptionHandle registration = model.subscribe(SUBSCRIPTION_ID, null, StartAt.checkpoint(new StringBasedCheckpoint("replay-from-here")), __ -> Mono.empty());
        model.resumeSubscription(SUBSCRIPTION_ID);

        assertThat(delegate.globalCheckpointReads).hasValue(0);
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("replay-from-here");
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();
        assertThatThrownBy(() -> registration.waitUntilStarted().block(Duration.ofMillis(200)))
                .as("the handle from the registration keeps waiting rather than reporting a refusal that never happened")
                .hasMessageContaining("Timeout on blocking read");
    }

    @Test
    void a_read_that_could_not_answer_does_not_refuse_a_subscription_that_has_a_stored_checkpoint() {
        // The stored checkpoint is where this subscription starts and the read at registration is never consulted
        // for it, so the registration handle must not report a refusal that the start does not make.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpoint = true;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("from-a-previous-run")).block(TIMEOUT);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();

        SubscriptionHandle registration = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> registration.waitUntilStarted().block(Duration.ofMillis(200)))
                .hasMessageContaining("Timeout on blocking read");

        SubscriptionHandle resumed = model.resumeSubscription(SUBSCRIPTION_ID);

        resumed.waitUntilStarted().block(TIMEOUT);
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("from-a-previous-run");
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();
    }

    @Test
    void a_storage_that_could_not_be_read_leaves_the_registration_handle_waiting_rather_than_guessing() {
        // Whether the subscription is refused turns on what storage holds, and a read that failed says nothing about
        // that. Reading it as nothing stored would report a refusal that the start, reading the same storage a moment
        // later, does not make.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpoint = true;
        AtomicInteger reads = new AtomicInteger();
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage() {
            @Override
            public Mono<org.occurrent.subscription.Checkpoint> read(String subscriptionId) {
                return Mono.defer(() -> reads.getAndIncrement() == 0
                        ? Mono.error(new IllegalStateException("the checkpoint store is unreachable"))
                        : super.read(subscriptionId));
            }
        };
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("from-a-previous-run")).block(TIMEOUT);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();

        SubscriptionHandle registration = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> registration.waitUntilStarted().block(Duration.ofMillis(200)))
                .hasMessageContaining("Timeout on blocking read");

        SubscriptionHandle resumed = model.resumeSubscription(SUBSCRIPTION_ID);

        resumed.waitUntilStarted().block(TIMEOUT);
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("from-a-previous-run");
    }

    @Test
    void a_refused_registration_is_not_read_again_when_the_subscription_is_started() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpoint = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, new InMemoryCheckpointStorage());
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        delegate.failGlobalCheckpoint = false;
        delegate.globalCheckpoint = new StringBasedCheckpoint("much-later");
        SubscriptionHandle resumed = model.resumeSubscription(SUBSCRIPTION_ID);

        assertThatThrownBy(() -> resumed.waitUntilStarted().block(TIMEOUT)).isInstanceOf(IllegalStateException.class);
        assertThat(delegate.globalCheckpointReads)
                .as("the position is read once, at registration, and the outcome of that read is what decides this subscription")
                .hasValue(1);
    }

    @Test
    void a_read_that_would_have_succeeded_on_a_second_attempt_still_refuses_the_subscription() {
        // The read is not retried, because a second read happens after the registration and answers past whatever was
        // written in between. A model that forgot the first answer would pass everything else here and lose events.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpointTimes = 1;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        SubscriptionHandle resumed = model.resumeSubscription(SUBSCRIPTION_ID);

        assertThatThrownBy(() -> resumed.waitUntilStarted().block(TIMEOUT)).isInstanceOf(IllegalStateException.class);
        assertThat(delegate.globalCheckpointReads).hasValue(1);
        assertThat(storage.read(SUBSCRIPTION_ID).blockOptional(TIMEOUT)).isEmpty();
    }

    @Test
    void a_refused_subscription_is_started_again_by_registering_it_again_rather_than_by_resuming_it() {
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpoint = true;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        SubscriptionHandle resumed = model.resumeSubscription(SUBSCRIPTION_ID);
        assertThatThrownBy(() -> resumed.waitUntilStarted().block(TIMEOUT)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID))
                .isInstanceOf(UnknownSubscriptionException.class);

        delegate.failGlobalCheckpoint = false;
        delegate.globalCheckpoint = new StringBasedCheckpoint("much-later");
        SubscriptionHandle fresh = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        fresh.waitUntilStarted().block(TIMEOUT);
        assertThat(startedAtCheckpoint(delegate))
                .as("the interval the refused registration covered has to be replayed by whatever asks for it, since this registration reads where the feed is now")
                .isEqualTo("much-later");
        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT).asString()).isEqualTo("much-later");
    }

    @Test
    void one_registration_that_was_refused_does_not_withhold_the_others() {
        // A partially started model is the honest outcome. Withholding the healthy subscriptions over a broken one
        // would turn one unreadable position into an application that delivers nothing.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpointTimes = 1;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        SubscriptionHandle refused = model.subscribe("refused", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        model.subscribe("healthy", null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        model.start(true);

        assertThatThrownBy(() -> refused.waitUntilStarted().block(TIMEOUT)).isInstanceOf(IllegalStateException.class);
        assertThat(model.isRunning("healthy")).isTrue();
        assertThat(model.isRunning("refused")).isFalse();
        assertThat(model.isPaused("refused")).isFalse();
        assertThat(storage.read("healthy").block(TIMEOUT).asString()).isEqualTo("at-registration");
        assertThat(storage.read("refused").blockOptional(TIMEOUT)).isEmpty();
    }

    @Test
    void a_wrapped_model_that_manages_named_subscriptions_refuses_an_unreadable_position_from_subscribe_itself() {
        // That path awaits the position inside subscribe so the wrapped model is handed one, so it can refuse where
        // the caller is standing rather than on a handle.
        NamedRecordingSubscriptionModel delegate = new NamedRecordingSubscriptionModel("at-registration");
        delegate.feed.failGlobalCheckpoint = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, new InMemoryCheckpointStorage());

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot read the position right now");
        assertThat(delegate.subscribedIds).isEmpty();
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

    @Test
    void registering_while_running_reports_a_position_it_could_not_read_rather_than_starting_anyway() {
        // There is no gap to cover on this path, since the subscription starts at the moment it is registered, but a
        // position that could not be read is still nothing to start from and was already reported this way.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpoint = true;
        SaveCountingCheckpointStorage storage = new SaveCountingCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot read the position right now");
        assertThat(storage.saves).hasValue(0);
        assertThat(delegate.startedAt).isEmpty();
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void a_dynamic_start_position_that_answers_the_model_default_is_refused_when_it_starts() {
        // The read runs at registration, because the answer may be the model default, but which of the two it is only
        // becomes known at start. So the registration handle waits and the refusal comes out on the resume handle.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpoint = true;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        StartAt dynamic = StartAt.dynamic(StartAt::subscriptionModelDefault);

        SubscriptionHandle registration = model.subscribe(SUBSCRIPTION_ID, null, dynamic, __ -> Mono.empty());
        assertThatThrownBy(() -> registration.waitUntilStarted().block(Duration.ofMillis(200)))
                .hasMessageContaining("Timeout on blocking read");

        SubscriptionHandle resumed = model.resumeSubscription(SUBSCRIPTION_ID);

        assertThatThrownBy(() -> resumed.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot read the position right now");
        assertThat(delegate.globalCheckpointReads)
                .as("the answer read at registration is the one that decides it, so the resume adds no read of its own")
                .hasValue(1);
        assertThat(delegate.startedAt).isEmpty();
        assertThat(model.isPaused(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void a_dynamic_start_position_that_answers_with_one_of_its_own_starts_despite_a_read_that_could_not_answer() {
        // It begins where its own answer says, so the read at registration was never going to be consulted and its
        // failure must not stop the subscription.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpoint = true;
        SaveCountingCheckpointStorage storage = new SaveCountingCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
        model.stop();
        StartAt dynamic = StartAt.dynamic(() -> StartAt.checkpoint(new StringBasedCheckpoint("replay-from-here")));

        model.subscribe(SUBSCRIPTION_ID, null, dynamic, __ -> Mono.empty());
        SubscriptionHandle resumed = model.resumeSubscription(SUBSCRIPTION_ID);

        resumed.waitUntilStarted().block(TIMEOUT);
        assertThat(startedAtCheckpoint(delegate)).isEqualTo("replay-from-here");
        assertThat(storage.saves).hasValue(0);
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isTrue();
    }

    @Test
    void registering_while_running_is_refused_when_the_position_answers_nothing() {
        // A wrapped model applies a start position when it opens its feed, not when it is handed one, so falling back
        // to now here begins wherever the feed has reached by then rather than where this registration happened.
        // Answering nothing is the same unresolvable problem on a running model as on a stopped one, and the blocking
        // model refuses a null position whatever state it is in, so refusing here is what keeps the two the same.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.globalCheckpoint = null;
        SaveCountingCheckpointStorage storage = new SaveCountingCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        SubscriptionHandle subscription = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThatThrownBy(() -> subscription.waitUntilStarted().block(TIMEOUT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(SUBSCRIPTION_ID)
                .hasMessageContaining("answered nothing");
        assertThat(delegate.startedAt).isEmpty();
        assertThat(storage.saves).hasValue(0);
        assertThat(model.isRunning(SUBSCRIPTION_ID)).isFalse();
    }

    @Test
    void a_wrapped_model_that_manages_named_subscriptions_refuses_a_position_that_answers_nothing_while_it_is_stopped() {
        // The path every reactor model in this repository takes. The wrapped model is stopped, so it parks the
        // registration and opens its feed when it is started, and a start position of now is applied then. Handing it
        // one would lose everything written while it waited, which is the loss this whole guarantee is about.
        NamedRecordingSubscriptionModel delegate = new NamedRecordingSubscriptionModel("at-registration");
        delegate.feed.globalCheckpoint = null;
        delegate.running = false;
        SaveCountingCheckpointStorage storage = new SaveCountingCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()))
                .as("the refusal reaches the caller from subscribe itself on this path, since it awaits the position there")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(SUBSCRIPTION_ID)
                .hasMessageContaining("answered nothing");

        // The feed moves on while nothing is registered. Had the registration gone through with a start position of
        // now, this is what the subscription would have started past.
        delegate.feed.globalCheckpoint = new StringBasedCheckpoint("much-later");
        assertThat(delegate.subscribedIds).isEmpty();
        assertThat(storage.saves).hasValue(0);
    }

    @Test
    void a_wrapped_model_that_manages_named_subscriptions_refuses_a_position_that_answers_nothing_while_it_is_running() {
        NamedRecordingSubscriptionModel delegate = new NamedRecordingSubscriptionModel("at-registration");
        delegate.feed.globalCheckpoint = null;
        SaveCountingCheckpointStorage storage = new SaveCountingCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("answered nothing");
        assertThat(delegate.subscribedIds).isEmpty();
        assertThat(storage.saves).hasValue(0);
    }

    @Test
    void a_wrapped_model_that_manages_named_subscriptions_takes_a_stored_checkpoint_when_the_position_answers_nothing() {
        // The exemption still stands on this path. The stored checkpoint is where the subscription starts and the
        // position read is never consulted for it.
        NamedRecordingSubscriptionModel delegate = new NamedRecordingSubscriptionModel("at-registration");
        delegate.feed.globalCheckpoint = null;
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        storage.save(SUBSCRIPTION_ID, new StringBasedCheckpoint("from-a-previous-run")).block(TIMEOUT);
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThat(delegate.subscribedIds).containsExactly(SUBSCRIPTION_ID);
        assertThat(delegate.startedAt.getFirst())
                .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                        startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("from-a-previous-run"));
        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT).asString()).isEqualTo("from-a-previous-run");
    }

    @Test
    void a_registration_refused_on_its_own_handle_keeps_its_id_until_it_is_started_or_cancelled() {
        // Starting it is what drops it, so an id whose subscription was never started is still taken. Cancelling is
        // the other way out, and this is the difference between the two the refusal names.
        RecordingSubscriptionModel delegate = new RecordingSubscriptionModel("at-registration");
        delegate.failGlobalCheckpoint = true;
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, new InMemoryCheckpointStorage());
        model.stop();
        SubscriptionHandle registration = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        assertThatThrownBy(() -> registration.waitUntilStarted().block(TIMEOUT)).isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty()))
                .isInstanceOf(DuplicateSubscriptionIdException.class);

        model.cancelSubscription(SUBSCRIPTION_ID);
        delegate.failGlobalCheckpoint = false;

        SubscriptionHandle fresh = model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());
        assertThat(model.isPaused(SUBSCRIPTION_ID)).isTrue();
        assertThatThrownBy(() -> fresh.waitUntilStarted().block(Duration.ofMillis(200)))
                .hasMessageContaining("Timeout on blocking read");
    }

    @Test
    void a_wrapped_model_that_manages_named_subscriptions_is_handed_the_position_read_at_registration() {
        // Path D reads the position inside subscribe and hands the wrapped model a concrete one, so there is nothing
        // for it to resolve later and no second read to disagree with the first.
        NamedRecordingSubscriptionModel delegate = new NamedRecordingSubscriptionModel("at-registration");
        InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
        ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);

        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

        assertThat(delegate.subscribedIds).containsExactly(SUBSCRIPTION_ID);
        assertThat(delegate.feed.globalCheckpointReads).hasValue(1);
        assertThat(delegate.startedAt).hasSize(1);
        assertThat(delegate.startedAt.getFirst())
                .as("the position read here is what the wrapped model is handed, not something for it to resolve later")
                .isInstanceOfSatisfying(StartAt.StartAtCheckpoint.class,
                        startAt -> assertThat(startAt.checkpoint.asString()).isEqualTo("at-registration"));
        assertThat(storage.read(SUBSCRIPTION_ID).block(TIMEOUT).asString()).isEqualTo("at-registration");
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

        // The three-argument overload, because the two-argument one is defined in terms of it, so counting here
        // counts every write once whichever overload the model calls. Counting the two-argument one instead would
        // have left these tests passing after the model moved to a conditional write.
        @Override
        public Mono<org.occurrent.subscription.Checkpoint> save(String subscriptionId, org.occurrent.subscription.Checkpoint checkpoint,
                                                                org.occurrent.subscription.CheckpointWriteCondition condition) {
            saves.incrementAndGet();
            return super.save(subscriptionId, checkpoint, condition);
        }
    }

    /**
     * A real {@code resolveFirstCheckpointRace} that compares by {@link OrderedCheckpoint#order()}, standing in for
     * what the MongoDB storages do by comparing operation time. Answers empty for a candidate or a stored checkpoint
     * that is not an {@link OrderedCheckpoint}, which only real delivery, never this fixture's own writes, produces.
     */
    private static final class OrderAwareCheckpointStorage extends InMemoryCheckpointStorage {

        @Override
        public Mono<org.occurrent.subscription.Checkpoint> resolveFirstCheckpointRace(String subscriptionId, org.occurrent.subscription.Checkpoint candidate) {
            if (!(candidate instanceof OrderedCheckpoint candidateOrdered)) {
                return Mono.empty();
            }
            return super.read(subscriptionId)
                    .map(java.util.Optional::of)
                    .defaultIfEmpty(java.util.Optional.empty())
                    .flatMap(storedOptional -> {
                        if (storedOptional.isEmpty()) {
                            return super.save(subscriptionId, candidate, org.occurrent.subscription.CheckpointWriteCondition.any());
                        }
                        org.occurrent.subscription.Checkpoint stored = storedOptional.get();
                        if (!(stored instanceof OrderedCheckpoint storedOrdered)) {
                            return Mono.empty();
                        }
                        return storedOrdered.order() > candidateOrdered.order()
                                ? super.save(subscriptionId, candidate, org.occurrent.subscription.CheckpointWriteCondition.any())
                                : Mono.just(stored);
                    });
        }

        /**
         * Writes the way another node would, bypassing this class's own {@code resolveFirstCheckpointRace}.
         */
        void writeWithoutScripting(org.occurrent.subscription.Checkpoint checkpoint) {
            super.save(SUBSCRIPTION_ID, checkpoint, org.occurrent.subscription.CheckpointWriteCondition.any()).block(TIMEOUT);
        }
    }

    /**
     * A {@code resolveFirstCheckpointRace} that always errors, standing in for a resolver outage rather than a
     * storage that cannot compare the two positions. The two must not be handled the same way: the latter answers
     * empty and is safe to fall back to the stored checkpoint from, the former answered nothing at all.
     */
    private static final class FailingResolveCheckpointStorage extends InMemoryCheckpointStorage {

        @Override
        public Mono<org.occurrent.subscription.Checkpoint> resolveFirstCheckpointRace(String subscriptionId, org.occurrent.subscription.Checkpoint candidate) {
            return Mono.error(new IllegalStateException("resolver unavailable"));
        }

        /**
         * Writes the way another node would, bypassing this class's own {@code resolveFirstCheckpointRace}.
         */
        void writeWithoutScripting(org.occurrent.subscription.Checkpoint checkpoint) {
            super.save(SUBSCRIPTION_ID, checkpoint, org.occurrent.subscription.CheckpointWriteCondition.any()).block(TIMEOUT);
        }
    }

    private record OrderedCheckpoint(int order) implements org.occurrent.subscription.Checkpoint {
        @Override
        public String asString() {
            return "order-" + order;
        }
    }
}
