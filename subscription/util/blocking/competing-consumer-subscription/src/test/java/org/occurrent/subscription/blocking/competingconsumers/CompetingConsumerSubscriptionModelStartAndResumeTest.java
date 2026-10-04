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

package org.occurrent.subscription.blocking.competingconsumers;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * What starting the model, and resuming or pausing a subscription, do when the lease is not free, when the strategy
 * throws, or when the wrapped model throws on starting itself or on a subscription, competing or not. The strategy
 * tells its listeners about a grant on the thread that registers, the way the MongoDB lease strategies do, and nothing
 * here needs MongoDB. A competing consumer that a call fails for is tried again on a thread of its own, so the test
 * doubles take calls from more than one thread.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerSubscriptionModelStartAndResumeTest {

    private static final String SUBSCRIBER_ID = "subscriber";

    private final RecordingDelegate delegate = new RecordingDelegate();
    private final SynchronousLeaseStrategy strategy = new SynchronousLeaseStrategy();
    private final CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);

    // Ends the tries of a consumer that keeps failing
    @AfterEach
    void shutdownTheModel() {
        model.shutdown();
    }

    @Test
    void start_returns_once_every_consumer_had_its_turn_and_one_the_wrapped_model_threw_on_runs_once_that_model_recovers() {
        strategy.grantOnRegister = false;
        subscribe("failing-1");
        subscribe("healthy");
        subscribe("failing-2");
        model.stop();
        strategy.grantOnRegister = true;
        delegate.throwsOn.addAll(Set.of("failing-1", "failing-2"));

        Throwable thrown = catchThrowable(() -> model.start(true));

        assertThat(thrown).as("start(), whose failing consumers are tried again instead").isNull();
        assertThat(delegate.running).as("a failing consumer does not keep the others from starting").containsExactly("healthy");
        delegate.throwsOn.clear();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delegate.running).as("every consumer once the wrapped model starts them again").containsExactlyInAnyOrder("failing-1", "healthy", "failing-2"));
        assertThat(strategy.holders).containsExactlyInAnyOrder("failing-1", "healthy", "failing-2");
    }

    @Test
    void resuming_a_consumer_the_wrapped_model_throws_on_returns_and_the_consumer_runs_once_the_wrapped_model_recovers() {
        strategy.grantOnRegister = false;
        subscribe("failing");
        model.pauseSubscription("failing");
        strategy.grantOnRegister = true;
        delegate.throwsOn.add("failing");

        Throwable thrown = catchThrowable(() -> model.resumeSubscription("failing"));

        assertThat(thrown).as("resuming the consumer, whose failure is tried again instead").isNull();
        assertThat(model.isRunning("failing")).isFalse();
        delegate.throwsOn.clear();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delegate.running).as("the consumer once the wrapped model starts it again").containsExactly("failing"));
        assertThat(strategy.holders).containsExactly("failing");
    }

    @Test
    void a_consumer_start_resumes_while_another_node_holds_its_lease_is_resumed_once_this_node_wins_it() {
        strategy.grantOnRegister = true;
        subscribe("stopped");
        model.stop();
        strategy.grantOnRegister = false;
        model.start(true);
        assertThat(model.isRunning("stopped")).isFalse();

        strategy.grant("stopped");

        assertThat(model.isRunning("stopped"))
                .as("the grant resumes the subscription rather than handing the lease back as if a user had paused it")
                .isTrue();
        assertThat(strategy.holders).containsExactly("stopped");
        assertThat(strategy.calls).as("nothing gave the lease up after the grant").endsWith("grant stopped");
    }

    @Test
    void a_consumer_whose_registration_threw_on_start_resumes_once_the_lease_store_is_back() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.stop();
        strategy.registerThrows = true;
        assertThat(catchThrowable(() -> model.start(true))).as("start() while the lease store is down").isNull();
        assertThat(delegate.running).as("x while the lease store is down").isEmpty();

        strategy.registerThrows = false;

        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delegate.running).as("x once the lease store is back").containsExactly("x"));
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_user_pause_of_a_consumer_waiting_for_its_lease_keeps_it_paused_after_the_grant() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.pauseSubscription("x");
        strategy.grantOnRegister = false;
        model.resumeSubscription("x");

        Throwable thrown = catchThrowable(() -> model.pauseSubscription("x"));
        strategy.grant("x");

        assertThat(delegate.running).as("the grant does not resume a subscription the user paused").doesNotContain("x");
        assertThat(thrown).as("the pause is recorded rather than refused").isNull();
        assertThat(model.isPaused("x")).isTrue();
        assertThat(strategy.holders).as("the grant was handed back").isEmpty();
    }

    @Test
    void a_consumer_the_wrapped_model_fails_to_resume_on_start_resumes_on_the_next_grant() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.stop();
        delegate.throwsOn.add("x");
        assertThat(catchThrowable(() -> model.start(true))).isNull();
        assertThat(strategy.calls).as("x gives its lease back and stays registered, so it keeps competing for it").endsWith("release x");
        delegate.throwsOn.clear();

        strategy.grant("x");

        assertThat(delegate.running).as("x keeps competing for the lease, so the next grant resumes it, with no other node to take it over").contains("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_non_competing_subscription_the_wrapped_model_fails_to_resume_does_not_keep_a_competing_consumer_from_starting() {
        strategy.grantOnRegister = true;
        subscribe("x");
        subscribeNonCompeting("nc");
        model.stop();
        delegate.throwsOn.add("nc");

        Throwable thrown = catchThrowable(() -> model.start(true));

        assertThat(thrown).as("the caller of start learns that nc did not resume").hasMessage("The wrapped model cannot start nc right now");
        assertThat(delegate.running).as("x starts although nc fails").containsExactly("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_wrapped_model_that_fails_to_start_does_not_keep_the_subscriptions_from_their_turn() {
        strategy.grantOnRegister = true;
        subscribe("x");
        subscribeNonCompeting("nc");
        model.stop();
        delegate.startThrows = true;

        Throwable thrown = catchThrowable(() -> model.start(true));

        assertThat(thrown).as("the caller of start learns that the wrapped model did not start").hasMessage("The wrapped model cannot start right now");
        assertThat(thrown.getSuppressed()).as("nc still gets its turn and fails as well, since resuming it starts the wrapped model, while x is tried again instead").hasSize(1);
        assertThat(strategy.calls).as("x gives back the lease it won").endsWith("release x");
        delegate.startThrows = false;
        strategy.grant("x");
        assertThat(delegate.running).as("x keeps competing for the lease, so the next grant resumes it").containsExactly("x");
    }

    @Test
    void a_stop_after_a_start_that_found_the_lease_taken_keeps_the_subscription_stopped_when_this_node_is_granted_the_lease() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.stop();
        strategy.grantOnRegister = false;
        model.start(true);

        model.stop();
        strategy.grant("x");

        assertThat(delegate.running).as("nothing runs after the user stopped the model").isEmpty();
        assertThat(strategy.holders).isEmpty();
        assertThat(model.isPaused("x")).isTrue();
    }

    @Test
    void a_stop_after_a_start_that_failed_part_way_keeps_the_subscription_stopped_when_this_node_is_granted_the_lease() {
        strategy.grantOnRegister = true;
        subscribe("x");
        subscribeNonCompeting("nc");
        model.stop();
        strategy.grantOnRegister = false;
        delegate.startThrows = true;
        assertThat(catchThrowable(() -> model.start(true))).isInstanceOf(IllegalStateException.class);
        delegate.startThrows = false;

        model.stop();
        strategy.grant("x");

        assertThat(delegate.running).as("nothing runs after the user stopped the model").isEmpty();
        assertThat(strategy.holders).isEmpty();
    }

    @Test
    void a_stop_after_a_start_that_found_the_lease_taken_keeps_a_consumer_waiting_for_its_lease_from_starting_when_this_node_is_granted_it() {
        strategy.grantOnRegister = false;
        subscribe("x");
        model.stop();
        model.start(true);

        model.stop();
        strategy.grant("x");

        assertThat(delegate.running).as("nothing runs after the user stopped the model").isEmpty();
        assertThat(strategy.holders).isEmpty();
    }

    @Test
    void a_subscription_made_while_the_model_is_stopped_takes_no_lease_until_the_model_is_started() {
        strategy.grantOnRegister = true;
        model.stop();

        subscribe("x");

        assertThat(strategy.holders).as("a stopped node holds no lease, so another node can take x").isEmpty();
        assertThat(delegate.running).isEmpty();
        assertThat(model.isPaused("x")).isTrue();
        model.start(true);
        assertThat(delegate.running).as("x competes for its lease once the model is started").containsExactly("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_subscription_made_while_the_model_is_stopped_stays_paused_after_a_resume_of_another_started_the_wrapped_model() {
        strategy.grantOnRegister = true;
        subscribe("y");
        model.stop();
        model.resumeSubscription("y");

        subscribe("x");

        assertThat(delegate.running).as("x delivers nothing while the model is stopped").containsExactly("y");
        assertThat(strategy.holders).containsExactly("y");
    }

    @Test
    void a_subscription_made_while_the_model_is_stopped_and_another_node_holds_its_lease_starts_once_this_node_wins_it_after_a_start() {
        strategy.grantOnRegister = false;
        model.stop();
        subscribe("x");
        model.start(true);

        strategy.grant("x");

        assertThat(delegate.running).containsExactly("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_custom_lease_strategy_that_throws_on_one_consumer_does_not_keep_another_from_resuming() {
        strategy.grantOnRegister = true;
        subscribe("a");
        subscribe("b");
        model.stop();
        strategy.hasLockThrowsOn.add("a");

        Throwable thrown = catchThrowable(() -> model.start(true));

        assertThat(thrown).as("start(), which tries a again instead").isNull();
        assertThat(delegate.running).as("b resumes although asking about a threw").containsExactly("b");
        strategy.hasLockThrowsOn.clear();
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delegate.running).as("a once the strategy answers for it again").containsExactlyInAnyOrder("a", "b"));
    }

    // An Error ends as a RuntimeException does, apart from the caller getting it, so the consumer it was thrown for is
    // tried again instead of holding its lease with nothing delivering
    @Test
    void a_custom_lease_strategy_that_throws_an_error_on_start_has_the_consumer_tried_again() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.stop();
        strategy.hasLockErrorsOnceOn.add("x");

        Throwable thrown = catchThrowable(() -> model.start(true));

        assertThat(thrown).as("start(), whose caller gets the Error").isInstanceOf(AssertionError.class);
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delegate.running).as("[x once the strategy answers for it again]").containsExactly("x"));
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_wrapped_model_that_throws_an_error_on_being_asked_about_a_consumer_start_resumes_has_it_tried_again() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.pauseSubscription("x");
        delegate.isRunningErrorsOnceOn.add("x");

        Throwable thrown = catchThrowable(() -> model.start(true));

        assertThat(thrown).as("start(), whose caller gets the Error").isInstanceOf(AssertionError.class);
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delegate.running).as("[x once the wrapped model answers for it again]").containsExactly("x"));
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_wrapped_model_that_throws_an_error_on_being_asked_about_a_granted_consumer_has_it_tried_again() {
        strategy.grantOnRegister = true;
        subscribe("x");
        strategy.loseTheLease("x");
        delegate.isRunningErrorsOnceOn.add("x");

        Throwable thrown = catchThrowable(() -> strategy.grant("x"));

        assertThat(thrown).as("the grant, whose caller gets the Error").isInstanceOf(AssertionError.class);
        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delegate.running).as("[x once the wrapped model answers for it again]").containsExactly("x"));
        assertThat(strategy.holders).containsExactly("x");
    }

    // The user paused both, so start(false) resumes neither, whether it competes for a lease or not
    @Test
    void a_start_without_resuming_keeps_a_subscription_the_user_paused_paused_whether_it_competes_or_not() {
        strategy.grantOnRegister = true;
        subscribe("x");
        subscribeNonCompeting("nc");
        model.pauseSubscription("x");
        model.pauseSubscription("nc");
        model.stop();

        model.start(false);

        assertThat(delegate.running).as("[subscriptions the user paused that start(false) resumed]").isEmpty();
        assertThat(model.isPaused("x")).isTrue();
        assertThat(model.isPaused("nc")).isTrue();
    }

    @Test
    void a_subscription_that_wins_its_lease_while_the_wrapped_model_is_stopped_is_delivered_once_after_a_start() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.stop();
        strategy.grantOnRegister = false;
        model.start(true);
        assertThat(delegate.isRunning()).as("a start that won no lease does not start the wrapped model").isFalse();
        strategy.grantOnRegister = true;

        subscribe("y");
        strategy.grantOnRegister = false;
        model.start(true);

        assertThat(delegate.running).as("y is subscribed once, in a wrapped model that was started first").containsExactly("y");
    }

    @Test
    void a_subscription_that_wins_its_lease_while_the_wrapped_model_fails_to_start_gives_the_lease_back_and_keeps_nothing() {
        strategy.grantOnRegister = true;
        subscribe("y");
        model.stop();
        model.start(false);
        delegate.startThrows = true;

        Throwable thrown = catchThrowable(() -> subscribe("x"));

        assertThat(thrown).as("the caller of subscribe learns that the wrapped model did not start").hasMessage("The wrapped model cannot start right now");
        assertThat(strategy.holders).as("the lease nothing on this node serves is given back").isEmpty();
        assertThat(model.subscriptionIds()).as("the failed subscription is not kept").containsExactly("y");
    }

    @Test
    void a_subscription_that_lost_its_lease_before_a_stop_competes_again_after_start_false_and_runs_once_this_node_wins_it() {
        strategy.grantOnRegister = true;
        subscribe("x");
        strategy.loseTheLease("x");
        assertThat(model.isRunning("x")).as("x after another node took its lease").isFalse();
        model.stop();

        model.start(false);
        strategy.grant("x");

        assertThat(strategy.calls).as("calls to the strategy, since nothing but the lease paused x").endsWith("register x", "grant x");
        assertThat(delegate.running).as("x once this node wins its lease back").containsExactly("x");
    }

    @Test
    void a_subscription_the_user_resumes_while_the_model_is_stopped_runs_once_this_node_wins_its_lease_later() {
        strategy.grantOnRegister = false;
        subscribe("x");
        model.stop();
        model.resumeSubscription("x");

        strategy.grant("x");

        assertThat(delegate.running).as("a resume runs x whether the lease is won straight away or later").containsExactly("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_stop_that_the_wrapped_model_fails_still_gives_every_lease_back() {
        strategy.grantOnRegister = true;
        subscribe("x");
        delegate.stopThrows = true;

        Throwable thrown = catchThrowable(model::stop);

        assertThat(thrown).as("the caller of stop learns that the wrapped model did not stop").hasRootCauseMessage("The wrapped model cannot stop right now");
        assertThat(strategy.holders).as("a stopped node holds no lease, although the wrapped model failed to stop").isEmpty();
        assertThat(model.isPaused("x")).isTrue();
    }

    @Test
    void a_stop_that_the_wrapped_model_fails_names_the_subscriptions_it_paused_and_how_to_resume_them() {
        strategy.grantOnRegister = true;
        subscribe("x");
        delegate.stopThrows = true;

        Throwable thrown = catchThrowable(model::stop);

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasMessage("Stopping the wrapped subscription model failed. "
                + "This model is stopped anyway, and subscriptions [x] are paused in the wrapped model and gave up their lease. "
                + "start(true) resumes them, while start(false) keeps them paused until each one is resumed.");
    }

    @Test
    void a_grant_for_a_lease_this_node_no_longer_holds_starts_nothing() {
        strategy.grantOnRegister = false;
        subscribe("x");

        strategy.grantWithoutTheLease("x");

        assertThat(delegate.running).as("x waits for a lease this node holds").isEmpty();
        assertThat(model.isRunning("x")).isFalse();
    }

    @Test
    void a_subscription_made_while_the_model_is_stopped_is_held_paused_by_the_wrapped_model_from_the_subscribe() {
        strategy.grantOnRegister = true;
        model.stop();

        subscribe("x");

        assertThat(delegate.isPaused("x")).as("the wrapped model has x, and records where it starts, from the subscribe").isTrue();
        assertThat(model.isRunning("x")).isFalse();
        model.start(true);
        assertThat(delegate.running).as("winning the lease resumes x once").containsExactly("x");
    }

    @Test
    void a_subscription_made_while_the_model_is_stopped_is_held_paused_by_a_wrapped_model_that_a_resume_started_again() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.stop();
        model.resumeSubscription("x");
        assertThat(delegate.isRunning()).as("resuming x started the wrapped model again").isTrue();

        subscribe("y");

        assertThat(delegate.isPaused("y")).as("the wrapped model has y, and records where it starts, from the subscribe").isTrue();
        assertThat(delegate.running).as("y delivers nothing before this node wins its lease").containsExactly("x");
        model.start(true);
        assertThat(delegate.running).as("winning the lease resumes y once").containsExactly("x", "y");
    }

    @Test
    void a_subscription_made_after_a_stop_the_wrapped_model_failed_is_held_paused_by_it() {
        strategy.grantOnRegister = true;
        subscribe("x");
        delegate.stopThrows = true;
        assertThat(catchThrowable(model::stop)).isInstanceOf(IllegalStateException.class);
        assertThat(delegate.isRunning()).as("the wrapped model still runs").isTrue();
        assertThat(delegate.running).as("stop() paused x in the wrapped model that still runs").doesNotContain("x");

        subscribe("y");

        assertThat(delegate.isPaused("y")).as("the wrapped model has y, and records where it starts, from the subscribe").isTrue();
        assertThat(delegate.running).as("y delivers nothing before this node wins its lease").doesNotContain("y");
        model.start(false);
        assertThat(delegate.running).as("winning the lease resumes y once, and x stays paused without a resume").containsExactly("y");
    }

    @Test
    void a_start_without_resuming_makes_a_subscription_made_while_the_model_was_stopped_compete_for_its_lease() {
        strategy.grantOnRegister = true;
        model.stop();
        subscribe("x");

        model.start(false);

        assertThat(delegate.running).as("x was never paused, so it competes for its lease and runs once it wins it").containsExactly("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_start_without_resuming_makes_a_subscription_waiting_for_its_lease_at_the_stop_compete_again() {
        strategy.grantOnRegister = false;
        subscribe("x");
        model.stop();

        model.start(false);
        strategy.grant("x");

        assertThat(delegate.running).as("x was waiting, not paused, so it competes again and runs once it wins its lease").containsExactly("x");
    }

    @Test
    void a_consumer_recorded_as_running_that_the_wrapped_model_fails_to_resume_is_resumed_by_a_later_grant() {
        strategy.grantOnRegister = true;
        // Nothing pauses x when it gives its lease back, so only what this model records for it decides the later grant
        strategy.tellsTheListenersAboutARelease = false;
        subscribe("x");
        // The wrapped model has lost x, as it does for a catch-up replay that failed, while x is recorded as running here
        delegate.cancelSubscription("x");
        delegate.throwsOn.add("x");
        assertThat(catchThrowable(() -> model.resumeSubscription("x"))).isNull();
        assertThat(strategy.calls).as("x gives its lease back and stays registered").endsWith("release x");
        delegate.throwsOn.clear();

        strategy.grant("x");

        assertThat(delegate.running).as("the later grant tries x again").containsExactly("x");
        assertThat(strategy.holders).containsExactly("x");
    }

    @Test
    void a_started_model_is_running_while_this_node_holds_no_lease() {
        strategy.grantOnRegister = false;
        subscribe("x");
        model.stop();

        model.start(true);

        assertThat(delegate.isRunning()).as("a start that won no lease does not start the wrapped model").isFalse();
        assertThat(model.isRunning()).as("the model after start(), with no lease held").isTrue();
    }

    @Test
    void a_stopped_model_is_not_running_once_a_resume_wins_a_lease_and_starts_the_wrapped_model() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.stop();

        model.resumeSubscription("x");

        assertThat(delegate.isRunning()).as("the resume that won the lease started the wrapped model").isTrue();
        assertThat(model.isRunning()).as("the model after stop(), with x resumed").isFalse();
    }

    @Test
    void a_shut_down_model_is_not_running_although_its_wrapped_model_says_it_is() {
        strategy.grantOnRegister = true;
        subscribe("x");

        model.shutdown();

        assertThat(delegate.isRunning()).as("the wrapped model, whose shutdown() does nothing").isTrue();
        assertThat(model.isRunning()).as("the model after shutdown()").isFalse();
    }

    @Test
    void a_new_model_over_a_wrapped_model_never_started_is_running() {
        delegate.started = false;

        assertThat(model.isRunning()).as("the new model").isTrue();
    }

    // isRunning() returns true here, so a caller that starts the model only while it returns false never resumes these
    @Test
    void documents_that_a_subscription_that_does_not_compete_made_on_a_new_model_whose_wrapped_model_was_never_started_stays_paused_until_start_or_a_resume() {
        delegate.started = false;
        subscribeNonCompeting("nc1");
        subscribeNonCompeting("nc2");

        model.start(false);
        assertThat(model.isPaused("nc1")).as("nc1, after start(false)").isTrue();
        assertThat(model.isPaused("nc2")).as("nc2, after start(false)").isTrue();

        model.resumeSubscription("nc1");
        assertThat(model.isRunning("nc1")).as("nc1, after resumeSubscription(nc1)").isTrue();
        assertThat(model.isPaused("nc2")).as("nc2, after resumeSubscription(nc1)").isTrue();

        model.start();
        assertThat(model.isRunning("nc2")).as("nc2, after start()").isTrue();
    }

    @Test
    void a_subscription_that_does_not_compete_made_while_the_model_is_stopped_stays_paused() {
        delegate.started = false;
        model.stop();

        subscribeNonCompeting("nc");

        assertThat(delegate.isRunning()).as("the wrapped model, while this model is stopped").isFalse();
        assertThat(model.isPaused("nc")).as("nc, made while this model is stopped").isTrue();
    }

    private void subscribe(String subscriptionId) {
        model.subscribe(SUBSCRIBER_ID, subscriptionId, null, StartAt.subscriptionModelDefault(), __ -> {
        });
    }

    // A start position that resolves to null here makes the model hand the subscription straight to the wrapped model
    private void subscribeNonCompeting(String subscriptionId) {
        model.subscribe(SUBSCRIBER_ID, subscriptionId, null, StartAt.dynamic(__ -> null), __ -> {
        });
    }

    /**
     * Keeps track of which subscriptions deliver and which are paused, and throws when starting any subscription in
     * {@link #throwsOn}. Asked whether a subscription in {@link #isRunningErrorsOnceOn} runs, it throws an Error, once.
     * Like {@code SpringMongoSubscriptionModel}, it holds a subscription made while it is stopped
     * paused, as well as one made with {@code subscribePaused}, and starts itself to resume a subscription. A subscription delivered twice is listed twice in
     * {@link #running}.
     */
    private static final class RecordingDelegate implements SubscriptionModel {
        private final Set<String> throwsOn = ConcurrentHashMap.newKeySet();
        private final List<String> running = new CopyOnWriteArrayList<>();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();
        private volatile boolean started = true;
        private volatile boolean startThrows;
        private volatile boolean stopThrows;
        private final Set<String> isRunningErrorsOnceOn = ConcurrentHashMap.newKeySet();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            throwIfRefused(subscriptionId);
            if (started) {
                running.add(subscriptionId);
            } else {
                paused.add(subscriptionId);
            }
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            throwIfRefused(subscriptionId);
            paused.add(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            running.removeIf(subscriptionId::equals);
            paused.remove(subscriptionId);
        }

        @Override
        public void stop() {
            if (stopThrows) {
                throw new IllegalStateException("The wrapped model cannot stop right now");
            }
            started = false;
            paused.addAll(running);
            running.clear();
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            if (startThrows) {
                throw new IllegalStateException("The wrapped model cannot start right now");
            }
            started = true;
        }

        @Override
        public boolean isRunning() {
            return started;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            if (isRunningErrorsOnceOn.remove(subscriptionId)) {
                throw new AssertionError("The wrapped model failed with an Error on being asked about " + subscriptionId);
            }
            return running.contains(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throwIfRefused(subscriptionId);
            if (!started) {
                start(false);
            }
            paused.remove(subscriptionId);
            running.add(subscriptionId);
            return new FakeSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            running.removeIf(subscriptionId::equals);
            paused.add(subscriptionId);
        }

        private void throwIfRefused(String subscriptionId) {
            if (throwsOn.contains(subscriptionId)) {
                throw new IllegalStateException("The wrapped model cannot start " + subscriptionId + " right now");
            }
        }
    }

    private record FakeSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    /**
     * Grants the lease on register when {@link #grantOnRegister} is set, and tells the listeners on the registering
     * thread, as a lease strategy does for a lease that changed hands. {@link #grant(String)} plays a refresh round
     * granting a lease that another node gave up, which, as with the MongoDB lease strategies, only a registered
     * consumer can win. Releasing a lease keeps the consumer registered, unregistering does not. Both tell the listeners
     * when the consumer held the lease, a release only while {@link #tellsTheListenersAboutARelease} is set. Asked
     * whether this node holds the lease of a subscription in {@link #hasLockErrorsOnceOn}, it throws an Error, once.
     */
    private static final class SynchronousLeaseStrategy implements CompetingConsumerStrategy {
        private final List<String> calls = new CopyOnWriteArrayList<>();
        private final Set<String> registered = ConcurrentHashMap.newKeySet();
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final Set<String> hasLockThrowsOn = ConcurrentHashMap.newKeySet();
        private final Set<String> hasLockErrorsOnceOn = ConcurrentHashMap.newKeySet();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        private volatile boolean grantOnRegister;
        private volatile boolean registerThrows;
        private volatile boolean tellsTheListenersAboutARelease = true;

        /**
         * A grant the strategy decided before the lease moved on, which reaches the listeners once this node no longer
         * holds it.
         */
        void grantWithoutTheLease(String subscriptionId) {
            calls.add("stale grant " + subscriptionId);
            listeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, SUBSCRIBER_ID));
        }

        void loseTheLease(String subscriptionId) {
            calls.add("lost " + subscriptionId);
            holders.remove(subscriptionId);
            listeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, SUBSCRIBER_ID));
        }

        void grant(String subscriptionId) {
            if (!registered.contains(subscriptionId)) {
                calls.add("no grant for unregistered " + subscriptionId);
                return;
            }
            calls.add("grant " + subscriptionId);
            holders.add(subscriptionId);
            listeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, SUBSCRIBER_ID));
        }

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("register " + subscriptionId);
            if (registerThrows) {
                throw new IllegalStateException("The lease store cannot be reached");
            }
            registered.add(subscriptionId);
            if (grantOnRegister && holders.add(subscriptionId)) {
                listeners.forEach(listener -> listener.onConsumeGranted(subscriptionId, subscriberId));
            }
            return holders.contains(subscriptionId);
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("unregister " + subscriptionId);
            registered.remove(subscriptionId);
            if (holders.remove(subscriptionId)) {
                listeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
            }
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("release " + subscriptionId);
            if (holders.remove(subscriptionId) && tellsTheListenersAboutARelease) {
                listeners.forEach(listener -> listener.onConsumeProhibited(subscriptionId, subscriberId));
            }
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            if (hasLockErrorsOnceOn.remove(subscriptionId)) {
                throw new AssertionError("A custom lease strategy failed with an Error on " + subscriptionId);
            }
            if (hasLockThrowsOn.contains(subscriptionId)) {
                throw new IllegalStateException("A custom lease strategy failed to answer for " + subscriptionId);
            }
            return holders.contains(subscriptionId);
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
            listeners.add(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            listeners.remove(listenerConsumer);
        }
    }
}
