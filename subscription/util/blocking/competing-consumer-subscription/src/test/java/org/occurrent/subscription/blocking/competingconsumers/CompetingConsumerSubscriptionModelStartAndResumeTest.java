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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
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
    // Not running as the model over it is built, as a wrapped model built with autoStartup(false) is
    private final RecordingDelegate delegateNotRunning = notRunning();
    private final SynchronousLeaseStrategy strategyOfTheModelBuiltStopped = new SynchronousLeaseStrategy();
    private final CompetingConsumerSubscriptionModel modelBuiltStopped = new CompetingConsumerSubscriptionModel(delegateNotRunning, strategyOfTheModelBuiltStopped);

    // Ends the tries of a consumer that keeps failing
    @AfterEach
    void shutdownTheModel() {
        model.shutdown();
        modelBuiltStopped.shutdown();
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

    // The Error is thrown once every subscription has had its turn, as one that fails to start does
    @Test
    void a_wrapped_model_that_throws_an_error_on_being_started_does_not_keep_the_subscriptions_from_their_turn() {
        strategy.grantOnRegister = true;
        subscribe("x");
        model.stop();
        delegate.startErrorsOnce.set(true);

        Throwable thrown = catchThrowable(() -> model.start(true));

        assertThat(thrown).as("start(), whose caller gets the Error").isInstanceOf(AssertionError.class).hasMessage("The wrapped model failed with an Error on being started");
        assertThat(delegate.running).as("x, which wins its lease in that start(true) and starts the wrapped model").containsExactly("x");
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
        assertThat(delegate.isRunning()).as("the wrapped model, after a start that won no lease").isTrue();
        // Stopped by a call to the wrapped model itself, so y wins its lease while that model is stopped
        delegate.stop();
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
        // Stopped by a call to the wrapped model itself, since start(false) started it
        delegate.stop();
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

        assertThat(delegate.isRunning()).as("the wrapped model, after a start that won no lease").isTrue();
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
    void a_new_model_is_started_when_its_wrapped_model_runs_and_stopped_when_it_does_not() {
        assertThat(model.isRunning()).as("a new model over a wrapped model that runs").isTrue();
        assertThat(modelBuiltStopped.isRunning()).as("a new model over a wrapped model that is not running").isFalse();
    }

    // The last row is how a caller that reads isRunning() as whether the wrapped model runs starts a model
    @ParameterizedTest(name = "started by: {0}")
    @ValueSource(strings = {"start()", "start(false)", "start() only when isRunning() returns false"})
    void subscriptions_made_on_a_model_built_over_a_wrapped_model_that_is_not_running_run_once_that_model_is_started(String startedBy) {
        strategyOfTheModelBuiltStopped.grantOnRegister = true;
        subscribeNonCompeting(modelBuiltStopped, "nc1");
        subscribeNonCompeting(modelBuiltStopped, "nc2");
        subscribe(modelBuiltStopped, "x");
        assertThat(delegateNotRunning.running).as("subscriptions running in the wrapped model before the start").isEmpty();
        assertThat(strategyOfTheModelBuiltStopped.holders).as("leases held before the start").isEmpty();

        switch (startedBy) {
            case "start()" -> modelBuiltStopped.start();
            case "start(false)" -> modelBuiltStopped.start(false);
            default -> {
                if (!modelBuiltStopped.isRunning()) {
                    modelBuiltStopped.start();
                }
            }
        }

        assertThat(delegateNotRunning.isRunning()).as("the wrapped model, after %s", startedBy).isTrue();
        assertThat(modelBuiltStopped.isRunning("nc1")).as("nc1, after %s", startedBy).isTrue();
        assertThat(modelBuiltStopped.isRunning("nc2")).as("nc2, after %s", startedBy).isTrue();
        assertThat(modelBuiltStopped.isRunning("x")).as("x, after %s", startedBy).isTrue();
    }

    // InMemorySubscriptionModel drops what it is given while it is not running
    @Test
    void nothing_is_delivered_on_a_model_built_over_a_wrapped_model_that_is_not_running_until_that_model_is_started() {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        wrapped.stop();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        List<String> deliveredToNc = new CopyOnWriteArrayList<>();
        List<String> deliveredToX = new CopyOnWriteArrayList<>();
        try {
            subscribeNonCompeting(overInMemory, "nc", deliveredToNc);
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), cloudEvent -> deliveredToX.add(cloudEvent.getId()));
            wrapped.accept(List.of(event("before")));

            overInMemory.start();
            wrapped.accept(List.of(event("after")));

            await().atMost(5, SECONDS).untilAsserted(() -> {
                assertThat(deliveredToNc).as("events delivered to nc").contains("after");
                assertThat(deliveredToX).as("events delivered to x").contains("after");
            });
            assertThat(deliveredToNc).as("events delivered to nc").containsExactly("after");
            assertThat(deliveredToX).as("events delivered to x").containsExactly("after");
        } finally {
            overInMemory.shutdown();
        }
    }

    // The first start finds no subscription to start the wrapped model for
    @Test
    void a_subscription_that_does_not_compete_made_on_a_model_started_before_it_had_any_subscription_runs() {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        wrapped.stop();
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, new SynchronousLeaseStrategy());
        List<String> delivered = new CopyOnWriteArrayList<>();
        try {
            if (!overInMemory.isRunning()) {
                overInMemory.start();
            }
            subscribeNonCompeting(overInMemory, "nc", delivered);
            if (!overInMemory.isRunning()) {
                overInMemory.start();
            }

            wrapped.accept(List.of(event("e1")));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delivered).as("events delivered to nc").containsExactly("e1"));
        } finally {
            overInMemory.shutdown();
        }
    }

    @Test
    void a_subscription_that_does_not_compete_made_after_a_stop_and_a_start_with_no_subscription_runs() {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, new SynchronousLeaseStrategy());
        List<String> delivered = new CopyOnWriteArrayList<>();
        try {
            overInMemory.stop();
            overInMemory.start();
            subscribeNonCompeting(overInMemory, "nc", delivered);

            wrapped.accept(List.of(event("e1")));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delivered).as("events delivered to nc").containsExactly("e1"));
        } finally {
            overInMemory.shutdown();
        }
    }

    // A start that starts the wrapped model resumes nothing it holds paused
    @Test
    void a_start_without_resuming_starts_the_wrapped_model_and_leaves_a_subscription_the_user_paused_paused() {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, new SynchronousLeaseStrategy());
        List<String> deliveredToTheOnePaused = new CopyOnWriteArrayList<>();
        List<String> deliveredToTheOneMade = new CopyOnWriteArrayList<>();
        try {
            subscribeNonCompeting(overInMemory, "paused", deliveredToTheOnePaused);
            overInMemory.pauseSubscription("paused");
            // Stopped by a call to the wrapped model itself, so this model is still started
            wrapped.stop();

            overInMemory.start(false);
            subscribeNonCompeting(overInMemory, "made", deliveredToTheOneMade);

            assertThat(overInMemory.isRunning("made")).as("the subscription made after start(false)").isTrue();
            assertThat(overInMemory.isPaused("paused")).as("the subscription the user paused, after start(false)").isTrue();
            wrapped.accept(List.of(event("e1")));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(deliveredToTheOneMade).as("events delivered to the subscription made").containsExactly("e1"));
            assertThat(deliveredToTheOnePaused).as("events delivered to the subscription the user paused").isEmpty();
        } finally {
            overInMemory.shutdown();
        }
    }

    @Test
    void a_start_that_fails_to_start_the_wrapped_model_throws_and_a_later_start_runs_a_subscription_that_does_not_compete() {
        subscribeNonCompeting(modelBuiltStopped, "nc");
        delegateNotRunning.startThrows = true;

        Throwable thrown = catchThrowable(modelBuiltStopped::start);
        delegateNotRunning.startThrows = false;
        modelBuiltStopped.start();

        assertThat(thrown).as("start(), while the wrapped model fails to start").hasMessage("The wrapped model cannot start right now");
        assertThat(modelBuiltStopped.isRunning("nc")).as("nc, after a later start()").isTrue();
    }

    // Started by a call to the wrapped model itself, which this model knows nothing of, so x never competes for its lease
    @Test
    void a_competing_subscription_whose_events_are_held_since_its_model_was_never_started_logs_one_warning() {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        wrapped.stop();
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, new SynchronousLeaseStrategy());
        List<String> deliveredToX = new CopyOnWriteArrayList<>();
        List<String> warnings = new CopyOnWriteArrayList<>();
        List<String> held = new CopyOnWriteArrayList<>();
        AppenderBase<ILoggingEvent> appender = recording(warnings, held);
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), cloudEvent -> deliveredToX.add(cloudEvent.getId()));
            wrapped.start();
            wrapped.accept(List.of(event("e1"), event("e2")));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(held).as("events of x held").hasSize(1));

            // Lets e1 through, after which e2 is held
            overInMemory.stop();

            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(held).as("events of x held").hasSize(2));
            assertThat(deliveredToX).as("events delivered to x").containsExactly("e1");
            assertThat(warnings).as("warnings").singleElement().asString()
                    .contains("Call start() on the CompetingConsumerSubscriptionModel, not on the subscription model it wraps")
                    .contains("subscriptionId=x");
        } finally {
            detach(appender);
            overInMemory.shutdown();
        }
    }

    @ParameterizedTest(name = "ended by: {0}")
    @ValueSource(strings = {"stop()", "shutdown()"})
    void an_event_held_since_its_model_was_never_started_is_delivered_once_the_model_is(String endedBy) {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        wrapped.stop();
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, new SynchronousLeaseStrategy());
        List<String> deliveredToX = new CopyOnWriteArrayList<>();
        List<String> held = new CopyOnWriteArrayList<>();
        AppenderBase<ILoggingEvent> appender = recording(new CopyOnWriteArrayList<>(), held);
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), cloudEvent -> deliveredToX.add(cloudEvent.getId()));
            wrapped.start();
            wrapped.accept(List.of(event("e1")));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(held).as("events of x held").hasSize(1));

            Runnable end = endedBy.equals("stop()") ? overInMemory::stop : overInMemory::shutdown;
            assertThat(CompletableFuture.runAsync(end)).as(endedBy).succeedsWithin(Duration.ofSeconds(5));

            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(deliveredToX).as("events delivered to x after %s", endedBy).containsExactly("e1"));
        } finally {
            detach(appender);
            overInMemory.shutdown();
        }
    }

    @Test
    void a_subscription_that_does_not_compete_made_while_the_model_is_stopped_stays_paused() {
        delegate.started = false;
        model.stop();

        subscribeNonCompeting("nc");

        assertThat(delegate.isRunning()).as("the wrapped model, while this model is stopped").isFalse();
        assertThat(model.isPaused("nc")).as("nc, made while this model is stopped").isTrue();
    }

    // InMemorySubscriptionModel pauses every subscription on stop(), and start(false) keeps the pauses
    @ParameterizedTest(name = "paused after: {0}")
    @ValueSource(strings = {"resumed and paused", "paused once the wrapped model was started directly"})
    void a_subscription_that_does_not_compete_the_user_paused_before_the_first_start_stays_paused_after_start_false(String pausedAfter) {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        wrapped.stop();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        List<String> deliveredToThePaused = new CopyOnWriteArrayList<>();
        List<String> deliveredToTheUntouched = new CopyOnWriteArrayList<>();
        List<String> deliveredToX = new CopyOnWriteArrayList<>();
        try {
            subscribeNonCompeting(overInMemory, "paused", deliveredToThePaused);
            subscribeNonCompeting(overInMemory, "untouched", deliveredToTheUntouched);
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), cloudEvent -> deliveredToX.add(cloudEvent.getId()));
            if (pausedAfter.equals("resumed and paused")) {
                overInMemory.resumeSubscription("paused");
            } else {
                wrapped.start();
            }
            overInMemory.pauseSubscription("paused");

            overInMemory.start(false);
            wrapped.accept(List.of(event("e1")));

            await().atMost(5, SECONDS).untilAsserted(() -> {
                assertThat(deliveredToX).as("events delivered to x").contains("e1");
                assertThat(deliveredToTheUntouched).as("events delivered to the subscription nobody touched").contains("e1");
            });
            assertThat(overInMemory.isPaused("paused")).as("the subscription the user paused before the first start, after start(false)").isTrue();
            assertThat(deliveredToThePaused).as("events delivered to the subscription the user paused, %s", pausedAfter).isEmpty();
        } finally {
            overInMemory.shutdown();
        }
    }

    @ParameterizedTest(name = "first start: {0}")
    @ValueSource(strings = {"start()", "start(false)"})
    void a_first_start_that_throws_does_not_count_as_the_first_so_a_later_start_without_resuming_runs_a_subscription_that_does_not_compete(String firstStart) {
        subscribeNonCompeting(modelBuiltStopped, "nc");
        delegateNotRunning.startThrows = true;

        Throwable thrown = catchThrowable(() -> {
            if (firstStart.equals("start()")) {
                modelBuiltStopped.start();
            } else {
                modelBuiltStopped.start(false);
            }
        });
        delegateNotRunning.startThrows = false;
        modelBuiltStopped.start(false);

        assertThat(thrown).as("the first %s, while the wrapped model fails to start", firstStart).isNotNull();
        assertThat(modelBuiltStopped.isRunning("nc")).as("nc, after a first %s that threw and a start(false)", firstStart).isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"built stopped", "stopped on the wrapped model itself"})
    void a_start_that_throws_leaves_the_model_not_running_so_a_start_only_when_it_is_not_running_tries_again(String stoppedHow) {
        CompetingConsumerSubscriptionModel stopped;
        RecordingDelegate wrapped;
        if (stoppedHow.equals("built stopped")) {
            stopped = modelBuiltStopped;
            wrapped = delegateNotRunning;
            subscribeNonCompeting(stopped, "nc");
        } else {
            stopped = model;
            wrapped = delegate;
            subscribeNonCompeting(stopped, "nc");
            wrapped.stop();
        }
        wrapped.startThrows = true;

        // isRunning() does not ask the wrapped model, so a model whose wrapped model was stopped directly still reports
        // that it runs until a start(..) throws
        Throwable thrown = catchThrowable(stopped::start);

        assertThat(thrown).as("the start while the wrapped model fails to start, %s", stoppedHow).isNotNull();
        assertThat(stopped.isRunning()).as("the model after a start that threw, %s", stoppedHow).isFalse();
        wrapped.startThrows = false;
        if (!stopped.isRunning()) {
            stopped.start();
        }
        assertThat(stopped.isRunning()).as("the model after a start only when it is not running, %s", stoppedHow).isTrue();
        assertThat(stopped.isRunning("nc")).as("nc, after a start only when the model is not running, %s", stoppedHow).isTrue();
    }

    // The lease strategy does not refresh on its own, so a grant reaches the model only as the test makes it
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"wrapped model, then this model", "this model, then the wrapped model, then the grant", "stop(), wrapped model, then this model"})
    void a_competing_subscription_runs_under_its_lease_whichever_of_the_two_models_is_started_first(String startedInTheOrder) {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        boolean builtRunning = startedInTheOrder.startsWith("stop()");
        if (!builtRunning) {
            wrapped.stop();
        }
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        List<String> deliveredToX = new CopyOnWriteArrayList<>();
        List<String> deliveredWithoutTheLease = new CopyOnWriteArrayList<>();
        Consumer<CloudEvent> actionOfX = cloudEvent -> {
            deliveredToX.add(cloudEvent.getId());
            if (!grants.hasLock("x", SUBSCRIBER_ID)) {
                deliveredWithoutTheLease.add(cloudEvent.getId());
            }
        };
        try {
            switch (startedInTheOrder) {
                case "wrapped model, then this model" -> {
                    grants.grantOnRegister = true;
                    overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), actionOfX);
                    wrapped.start();
                    overInMemory.start();
                }
                case "this model, then the wrapped model, then the grant" -> {
                    grants.grantOnRegister = false;
                    overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), actionOfX);
                    overInMemory.start();
                    wrapped.start();
                    assertThat(catchThrowable(() -> grants.grant("x"))).as("the grant of x, %s", startedInTheOrder).isNull();
                }
                default -> {
                    grants.grantOnRegister = true;
                    overInMemory.stop();
                    overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), actionOfX);
                    wrapped.start();
                    overInMemory.start();
                }
            }

            wrapped.accept(List.of(event("e1")));

            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(deliveredToX).as("events delivered to x, %s", startedInTheOrder).contains("e1"));
            assertThat(overInMemory.isRunning("x")).as("x, %s", startedInTheOrder).isTrue();
            assertThat(deliveredWithoutTheLease).as("events delivered to x while this node did not hold its lease, %s", startedInTheOrder).isEmpty();
        } finally {
            overInMemory.shutdown();
        }
    }

    @ParameterizedTest(name = "restarted by: {0}")
    @ValueSource(strings = {"start()", "start(false)"})
    void a_start_runs_a_competing_subscription_this_node_holds_the_lease_for_once_the_wrapped_model_was_stopped_directly(String how) {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        List<String> deliveredToX = new CopyOnWriteArrayList<>();
        List<String> deliveredWithoutTheLease = new CopyOnWriteArrayList<>();
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), cloudEvent -> {
                deliveredToX.add(cloudEvent.getId());
                if (!grants.hasLock("x", SUBSCRIBER_ID)) {
                    deliveredWithoutTheLease.add(cloudEvent.getId());
                }
            });
            assertThat(overInMemory.isRunning("x")).as("x, before the wrapped model is stopped").isTrue();
            wrapped.stop();

            if (how.equals("start()")) {
                overInMemory.start();
            } else {
                overInMemory.start(false);
            }
            wrapped.accept(List.of(event("s1")));

            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(deliveredToX).as("events delivered to x after the wrapped model was stopped directly and %s", how).contains("s1"));
            assertThat(deliveredWithoutTheLease).as("events delivered to x while this node did not hold its lease, after %s", how).isEmpty();
            assertThat(grants.holders).as("the subscriptions this node holds the lease for, after %s", how).contains("x");
        } finally {
            overInMemory.shutdown();
        }
    }

    @Test
    void a_start_without_resuming_after_a_start_that_threw_and_a_stop_runs_a_competing_subscription_and_keeps_the_one_the_user_paused_paused() {
        FailingInMemory wrapped = new FailingInMemory();
        wrapped.stop();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        List<String> deliveredToX = new CopyOnWriteArrayList<>();
        List<String> deliveredToY = new CopyOnWriteArrayList<>();
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), cloudEvent -> deliveredToX.add(cloudEvent.getId()));
            overInMemory.subscribe(SUBSCRIBER_ID, "y", null, StartAt.now(), cloudEvent -> deliveredToY.add(cloudEvent.getId()));
            overInMemory.pauseSubscription("y");
            wrapped.startThrowsOnce.set(true);
            assertThat(catchThrowable(() -> overInMemory.start(false))).as("the first start(false), while the wrapped model fails to start").isNotNull();
            overInMemory.stop();

            overInMemory.start(false);
            wrapped.accept(List.of(event("e1")));
            wrapped.waitUntilAllEventsProcessed(Duration.ofSeconds(5));

            assertThat(grants.holders).as("the subscriptions this node holds the lease for, after a start that threw, stop() and start(false)").containsExactly("x");
            assertThat(overInMemory.isRunning("x")).as("x, after a start that threw, stop() and start(false)").isTrue();
            assertThat(deliveredToX).as("events delivered to x, after a start that threw, stop() and start(false)").containsExactly("e1");
            assertThat(overInMemory.isPaused("y")).as("y, which the user paused, after a start that threw, stop() and start(false)").isTrue();
            assertThat(deliveredToY).as("events delivered to y, which the user paused").isEmpty();
        } finally {
            overInMemory.shutdown();
        }
    }

    @Test
    void pausing_a_competing_subscription_waiting_for_its_lease_that_a_direct_start_of_the_wrapped_model_runs_pauses_it_in_the_wrapped_model_too() {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        wrapped.stop();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), __ -> {
            });
            wrapped.start();
            assertThat(wrapped.isRunning("x")).as("x in the wrapped model, which a direct start runs while this node waits for the lease").isTrue();

            overInMemory.pauseSubscription("x");
            Throwable thrownOnResume = catchThrowable(() -> overInMemory.resumeSubscription("x"));

            assertThat(wrapped.isPaused("x")).as("x in the wrapped model, after the user paused it while it waited for the lease").isTrue();
            assertThat(thrownOnResume).as("resuming x, which the user paused").isNull();
        } finally {
            overInMemory.shutdown();
        }
    }

    @Test
    void a_start_that_resumes_what_the_user_paused_clears_the_pause_of_each_it_resumed_although_it_throws_for_another_so_a_later_start_without_resuming_runs_them() {
        FailingInMemory wrapped = new FailingInMemory();
        wrapped.stop();
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, new SynchronousLeaseStrategy());
        List<String> delivered = new CopyOnWriteArrayList<>();
        try {
            subscribeNonCompeting(overInMemory, "nc1", delivered);
            subscribeNonCompeting(overInMemory, "nc2", delivered);
            subscribeNonCompeting(overInMemory, "nc3", delivered);
            wrapped.start();
            overInMemory.pauseSubscription("nc1");
            overInMemory.pauseSubscription("nc2");
            wrapped.resumeThrowsOn.add("nc2");
            assertThat(catchThrowable(() -> overInMemory.start(true))).as("start(true), while the wrapped model fails to resume nc2").isNotNull();
            wrapped.resumeThrowsOn.clear();
            overInMemory.stop();

            overInMemory.start(false);

            assertThat(overInMemory.isPaused("nc3")).as("nc3, which nobody paused, after start(true), stop() and start(false)").isFalse();
            assertThat(overInMemory.isPaused("nc1")).as("nc1, whose pause start(true) undid, after start(true), stop() and start(false)").isFalse();
        } finally {
            overInMemory.shutdown();
        }
    }

    @Test
    void a_start_that_resumes_what_the_user_paused_clears_the_pause_of_a_competing_subscription_although_it_throws_for_another_so_a_later_start_without_resuming_runs_it() {
        FailingInMemory wrapped = new FailingInMemory();
        wrapped.stop();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), __ -> {
            });
            subscribeNonCompeting(overInMemory, "nc2", new CopyOnWriteArrayList<>());
            wrapped.start();
            overInMemory.pauseSubscription("x");
            overInMemory.pauseSubscription("nc2");
            wrapped.resumeThrowsOn.add("nc2");
            assertThat(catchThrowable(() -> overInMemory.start(true))).as("start(true), while the wrapped model fails to resume nc2").isNotNull();
            wrapped.resumeThrowsOn.clear();
            overInMemory.stop();

            overInMemory.start(false);

            assertThat(grants.holders).as("the subscriptions this node holds the lease for, after start(true), stop() and start(false)").containsExactly("x");
            assertThat(overInMemory.isRunning("x")).as("x, whose pause start(true) undid, after start(true), stop() and start(false)").isTrue();
        } finally {
            overInMemory.shutdown();
        }
    }

    @Test
    void a_start_that_resumes_a_competing_subscription_the_user_paused_while_it_waited_for_its_lease_and_a_direct_start_of_the_wrapped_model_ran_logs_no_failure_and_runs_it() {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        wrapped.stop();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        List<String> warnings = new CopyOnWriteArrayList<>();
        AppenderBase<ILoggingEvent> appender = recording(warnings, new CopyOnWriteArrayList<>());
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), __ -> {
            });
            wrapped.start();
            overInMemory.pauseSubscription("x");

            overInMemory.start(true);

            assertThat(warnings).as("warnings logged by start(true)").noneMatch(warning -> warning.startsWith("A call for CompetingConsumer failed, so it is tried again"));
            assertThat(grants.holders).as("the subscriptions this node holds the lease for, after start(true)").containsExactly("x");
            assertThat(overInMemory.isRunning("x")).as("x, which the user paused, after start(true)").isTrue();
        } finally {
            detach(appender);
            overInMemory.shutdown();
        }
    }

    @Test
    void a_competing_subscription_this_node_holds_the_lease_for_runs_after_a_start_of_the_wrapped_model_that_took_effect_and_then_threw() {
        FailingInMemory wrapped = new FailingInMemory();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        List<String> deliveredToX = new CopyOnWriteArrayList<>();
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), cloudEvent -> deliveredToX.add(cloudEvent.getId()));
            await().atMost(5, SECONDS).until(() -> overInMemory.isRunning("x"));
            wrapped.stop();
            wrapped.startTakesEffectThenThrowsOnce.set(true);

            Throwable thrownOnStart = catchThrowable(() -> overInMemory.start(false));
            wrapped.accept(List.of(event("e1")));

            assertThat(thrownOnStart).as("start(false), whose start of the wrapped model took effect and then threw").isNotNull();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(deliveredToX).as("events delivered to x, which holds its lease, after a start of the wrapped model that took effect and then threw").containsExactly("e1"));
            assertThat(grants.holders).as("the subscriptions this node holds the lease for, after that start").containsExactly("x");
        } finally {
            overInMemory.shutdown();
        }
    }

    @ParameterizedTest(name = "started again by {0}, then {1}")
    @CsvSource({"the grant of y, start(false)", "the grant of y, start()", "a resume of nc, start(false)", "a resume of nc, start()"})
    void a_competing_subscription_this_node_holds_the_lease_for_runs_once_another_call_started_the_wrapped_model_again_after_a_direct_stop(String startedAgainBy, String start) {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        List<String> deliveredToX = new CopyOnWriteArrayList<>();
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), cloudEvent -> deliveredToX.add(cloudEvent.getId()));
            subscribeNonCompeting(overInMemory, "nc", new CopyOnWriteArrayList<>());
            grants.grantOnRegister = false;
            overInMemory.subscribe(SUBSCRIBER_ID, "y", null, StartAt.now(), __ -> {
            });
            await().atMost(5, SECONDS).until(() -> overInMemory.isRunning("x"));
            wrapped.stop();

            if (startedAgainBy.equals("the grant of y")) {
                grants.grant("y");
            } else {
                overInMemory.resumeSubscription("nc");
            }
            if (start.equals("start()")) {
                overInMemory.start();
            } else {
                overInMemory.start(false);
            }
            wrapped.accept(List.of(event("e1")));

            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(deliveredToX).as("events delivered to x, which holds its lease, after a direct stop of the wrapped model, %s and %s", startedAgainBy, start).containsExactly("e1"));
            assertThat(grants.holders).as("the subscriptions this node holds the lease for, after %s and %s", startedAgainBy, start).contains("x");
        } finally {
            overInMemory.shutdown();
        }
    }

    @Test
    void pausing_a_competing_subscription_that_lost_its_lease_and_that_a_direct_restart_of_the_wrapped_model_runs_pauses_it_there_and_it_can_be_resumed() {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), __ -> {
            });
            await().atMost(5, SECONDS).until(() -> overInMemory.isRunning("x"));
            grants.grantOnRegister = false;
            grants.loseTheLease("x");
            wrapped.stop();
            wrapped.start();
            assertThat(wrapped.isRunning("x")).as("x in the wrapped model, which a direct restart runs after x lost its lease").isTrue();

            overInMemory.pauseSubscription("x");
            boolean pausedInTheWrappedModel = wrapped.isPaused("x");
            Throwable thrownOnResume = catchThrowable(() -> overInMemory.resumeSubscription("x"));

            assertThat(pausedInTheWrappedModel).as("x in the wrapped model, after the user paused it once it had lost its lease").isTrue();
            assertThat(thrownOnResume).as("resuming x, which the user paused").isNull();
            assertThat(grants.registered).as("the subscriptions registered for their lease, after the resume of x").contains("x");
        } finally {
            overInMemory.shutdown();
        }
    }

    @Test
    void a_competing_subscription_the_user_paused_that_a_direct_restart_of_the_wrapped_model_runs_can_be_resumed_and_competes_for_its_lease() {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), __ -> {
            });
            await().atMost(5, SECONDS).until(() -> overInMemory.isRunning("x"));
            overInMemory.pauseSubscription("x");
            wrapped.stop();
            wrapped.start();
            assertThat(wrapped.isRunning("x")).as("x in the wrapped model, which a direct restart runs after the user paused it").isTrue();

            Throwable thrownOnResume = catchThrowable(() -> overInMemory.resumeSubscription("x"));
            overInMemory.start(true);

            assertThat(thrownOnResume).as("resuming x, which the user paused").isNull();
            assertThat(grants.registered).as("the subscriptions registered for their lease, after the resume of x and start(true)").contains("x");
        } finally {
            overInMemory.shutdown();
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"competing, built running", "competing, built stopped", "not competing, built running", "not competing, built stopped"})
    void a_user_pause_that_took_effect_in_the_wrapped_model_and_then_threw_is_kept_by_a_start_without_resuming(String how) {
        boolean builtStopped = how.endsWith("built stopped");
        RecordingDelegate wrapped = builtStopped ? notRunning() : new RecordingDelegate();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overRecording = new CompetingConsumerSubscriptionModel(wrapped, grants);
        String subscriptionId = how.startsWith("competing") ? "x" : "nc";
        try {
            if (subscriptionId.equals("x")) {
                subscribe(overRecording, "x");
            } else {
                subscribeNonCompeting(overRecording, "nc");
            }
            if (builtStopped) {
                // Runs it while this model is stopped, so that the pause reaches the wrapped model
                overRecording.resumeSubscription(subscriptionId);
            }
            assertThat(overRecording.isRunning(subscriptionId)).as("%s before the pause, %s", subscriptionId, how).isTrue();
            wrapped.pauseTakesEffectThenThrows = true;
            assertThat(catchThrowable(() -> overRecording.pauseSubscription(subscriptionId))).as("the pause of %s, which took effect and then threw, %s", subscriptionId, how).isNotNull();
            wrapped.pauseTakesEffectThenThrows = false;
            if (!builtStopped) {
                overRecording.stop();
            }

            overRecording.start(false);

            await().during(1, SECONDS).atMost(3, SECONDS).untilAsserted(() -> assertThat(overRecording.isRunning(subscriptionId)).as("%s, which the user paused, after start(false), %s", subscriptionId, how).isFalse());
        } finally {
            overRecording.shutdown();
        }
    }

    @ParameterizedTest(name = "{0}, then {1}")
    @CsvSource({"an exception, start(true)", "an exception, resumeSubscription(nc)", "an exception, cancelSubscription(nc)", "an Error, start(true)"})
    void the_subscribe_of_a_subscription_that_does_not_compete_throws_when_resuming_it_for_a_start_that_came_while_it_was_made_fails(String failure, String then) throws Exception {
        RecordingDelegate wrapped = new RecordingDelegate();
        CompetingConsumerSubscriptionModel overRecording = new CompetingConsumerSubscriptionModel(wrapped, new SynchronousLeaseStrategy());
        CompletableFuture<@Nullable Void> subscribeMayGoOn = new CompletableFuture<>();
        try {
            wrapped.stop();
            wrapped.startThrows = true;
            wrapped.subscribeWaitsFor = subscribeMayGoOn;
            CompletableFuture<@Nullable Throwable> subscribed = CompletableFuture.supplyAsync(() -> catchThrowable(() -> subscribeNonCompeting(overRecording, "nc")));
            await().atMost(5, SECONDS).until(wrapped.subscribeEntered::isDone);
            assertThat(catchThrowable(() -> overRecording.start(true))).as("start(true), while the wrapped model makes nc and cannot start").isNotNull();
            if (failure.equals("an Error")) {
                wrapped.startThrows = false;
                wrapped.startErrorsOnce.set(true);
            }
            subscribeMayGoOn.complete(null);
            Throwable thrownOnSubscribe = subscribed.get(5, SECONDS);
            wrapped.startThrows = false;

            assertThat(thrownOnSubscribe).as("the subscribe of nc, whose resume for start(true) failed with %s once the wrapped model had made it", failure)
                    .isInstanceOf(failure.equals("an Error") ? AssertionError.class : IllegalStateException.class);
            assertThat(overRecording.isPaused("nc")).as("nc, after its subscribe threw").isTrue();
            switch (then) {
                case "start(true)" -> overRecording.start(true);
                case "resumeSubscription(nc)" -> overRecording.resumeSubscription("nc");
                default -> overRecording.cancelSubscription("nc");
            }
            if (then.startsWith("cancel")) {
                assertThat(catchThrowable(() -> subscribeNonCompeting(overRecording, "nc"))).as("subscribing nc again, after %s", then).isNull();
            } else {
                assertThat(overRecording.isRunning("nc")).as("nc, after %s", then).isTrue();
            }
        } finally {
            subscribeMayGoOn.complete(null);
            overRecording.shutdown();
        }
    }

    @ParameterizedTest(name = "paused by {0}, then {1}")
    @CsvSource({
            "a pause on the wrapped model, start(false)",
            "a pause on the wrapped model, start(true)",
            "stop() and start(false) on the wrapped model, start(false)",
            "stop() and start(false) on the wrapped model, start(true)",
            "stop() and start() on the wrapped model and then a pause there, start(false)",
            "stop() and start() on the wrapped model and then a pause there, start(true)",
            "a pause on the wrapped model and then stop() there, start(false)",
            "a pause on the wrapped model and then stop() there, start(true)"})
    void a_start_with_either_flag_runs_a_competing_subscription_this_node_holds_the_lease_for_that_the_wrapped_model_holds_paused(String pausedBy, String start) {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        List<String> deliveredToX = new CopyOnWriteArrayList<>();
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), cloudEvent -> deliveredToX.add(cloudEvent.getId()));
            await().atMost(5, SECONDS).until(() -> overInMemory.isRunning("x"));
            switch (pausedBy) {
                case "a pause on the wrapped model" -> wrapped.pauseSubscription("x");
                case "stop() and start(false) on the wrapped model" -> {
                    wrapped.stop();
                    wrapped.start(false);
                }
                case "stop() and start() on the wrapped model and then a pause there" -> {
                    wrapped.stop();
                    wrapped.start();
                    wrapped.pauseSubscription("x");
                }
                case "a pause on the wrapped model and then stop() there" -> {
                    wrapped.pauseSubscription("x");
                    wrapped.stop();
                }
                default -> throw new IllegalArgumentException(pausedBy);
            }

            overInMemory.start(start.equals("start(true)"));
            wrapped.accept(List.of(event("e1")));

            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(deliveredToX).as("events delivered to x, which holds its lease, after %s and then %s", pausedBy, start).containsExactly("e1"));
            assertThat(grants.holders).as("the subscriptions this node holds the lease for, after %s", start).containsExactly("x");
        } finally {
            overInMemory.shutdown();
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"a grant of its lease", "a resume of it"})
    void a_grant_of_its_lease_or_a_resume_of_it_runs_a_competing_subscription_the_user_paused_directly_on_the_wrapped_model(String then) {
        InMemorySubscriptionModel wrapped = new InMemorySubscriptionModel();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        List<String> deliveredToX = new CopyOnWriteArrayList<>();
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), cloudEvent -> deliveredToX.add(cloudEvent.getId()));
            await().atMost(5, SECONDS).until(() -> overInMemory.isRunning("x"));
            wrapped.pauseSubscription("x");

            if (then.equals("a grant of its lease")) {
                grants.grant("x");
            } else {
                overInMemory.resumeSubscription("x");
            }
            wrapped.accept(List.of(event("e1")));

            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(deliveredToX).as("events delivered to x, which holds its lease, after a pause on the wrapped model and then %s", then).containsExactly("e1"));
            assertThat(grants.holders).as("the subscriptions this node holds the lease for, after %s", then).containsExactly("x");
        } finally {
            overInMemory.shutdown();
        }
    }

    @Test
    void a_start_whose_start_of_the_wrapped_model_took_effect_and_then_threw_returns_once_a_competing_subscription_this_node_holds_the_lease_for_runs() {
        FailingInMemory wrapped = new FailingInMemory();
        SynchronousLeaseStrategy grants = new SynchronousLeaseStrategy();
        grants.grantOnRegister = true;
        CompetingConsumerSubscriptionModel overInMemory = new CompetingConsumerSubscriptionModel(wrapped, grants);
        try {
            overInMemory.subscribe(SUBSCRIBER_ID, "x", null, StartAt.now(), __ -> {
            });
            await().atMost(5, SECONDS).until(() -> overInMemory.isRunning("x"));
            wrapped.stop();
            wrapped.startTakesEffectThenThrowsOnce.set(true);
            // A try that asks about x holds the lock of x a while, so a start(..) that left x to a try finds it taken
            wrapped.isRunningIsSlowOnceOnATry.set(true);

            Throwable thrownOnStart = catchThrowable(() -> overInMemory.start(false));

            assertThat(thrownOnStart).as("start(false), whose start of the wrapped model took effect and then threw").isNotNull();
            assertThat(wrapped.isRunning("x")).as("x, which holds its lease, as start(false) returns after its start of the wrapped model took effect and then threw").isTrue();
        } finally {
            overInMemory.shutdown();
        }
    }

    private void subscribe(String subscriptionId) {
        subscribe(model, subscriptionId);
    }

    private static void subscribe(CompetingConsumerSubscriptionModel model, String subscriptionId) {
        model.subscribe(SUBSCRIBER_ID, subscriptionId, null, StartAt.subscriptionModelDefault(), __ -> {
        });
    }

    // A start position that resolves to null here makes the model hand the subscription straight to the wrapped model
    private void subscribeNonCompeting(String subscriptionId) {
        subscribeNonCompeting(model, subscriptionId);
    }

    private static void subscribeNonCompeting(CompetingConsumerSubscriptionModel model, String subscriptionId) {
        model.subscribe(SUBSCRIBER_ID, subscriptionId, null, StartAt.dynamic(__ -> null), __ -> {
        });
    }

    // InMemorySubscriptionModel refuses a start position that resolves to null, so it gets StartAt.now() instead
    private static void subscribeNonCompeting(CompetingConsumerSubscriptionModel model, String subscriptionId, List<String> delivered) {
        StartAt doesNotCompete = StartAt.dynamic(context -> context.subscriptionModelType() == CompetingConsumerSubscriptionModel.class ? null : StartAt.now());
        model.subscribe(SUBSCRIBER_ID, subscriptionId, null, doesNotCompete, cloudEvent -> delivered.add(cloudEvent.getId()));
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").build();
    }

    private static RecordingDelegate notRunning() {
        RecordingDelegate delegate = new RecordingDelegate();
        delegate.started = false;
        return delegate;
    }

    // Records each warning this model logs, and each event of x it holds for the lease
    private static AppenderBase<ILoggingEvent> recording(List<String> warnings, List<String> held) {
        AppenderBase<ILoggingEvent> appender = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                String message = event.getFormattedMessage();
                if (event.getLevel() == Level.WARN) {
                    warnings.add(message);
                } else if (message.startsWith("Holding an event until this node may deliver it") && message.contains("subscriptionId=x")) {
                    held.add(message);
                }
            }
        };
        appender.start();
        ((Logger) LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class)).addAppender(appender);
        return appender;
    }

    private static void detach(AppenderBase<ILoggingEvent> appender) {
        ((Logger) LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class)).detachAppender(appender);
    }

    /**
     * Keeps track of which subscriptions deliver and which are paused, and throws when starting any subscription in
     * {@link #throwsOn}. Asked whether a subscription in {@link #isRunningErrorsOnceOn} runs, it throws an Error, once,
     * and so does a start while {@link #startErrorsOnce} is set. A pause while {@link #pauseTakesEffectThenThrows} is
     * set takes effect and then throws, and a subscribe waits for {@link #subscribeWaitsFor} once it is set. Like {@code SpringMongoSubscriptionModel}, it holds a subscription made while it is stopped
     * paused, as well as one made with {@code subscribePaused}, and starts itself to resume a subscription. A subscription delivered twice is listed twice in
     * {@link #running}.
     */
    private static final class RecordingDelegate implements SubscriptionModel {
        private final Set<String> throwsOn = ConcurrentHashMap.newKeySet();
        private final List<String> running = new CopyOnWriteArrayList<>();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();
        private volatile boolean started = true;
        private volatile boolean startThrows;
        private final AtomicBoolean startErrorsOnce = new AtomicBoolean();
        private volatile boolean stopThrows;
        private final Set<String> isRunningErrorsOnceOn = ConcurrentHashMap.newKeySet();
        private volatile boolean pauseTakesEffectThenThrows;
        private volatile @Nullable CompletableFuture<@Nullable Void> subscribeWaitsFor;
        private final CompletableFuture<@Nullable Void> subscribeEntered = new CompletableFuture<>();

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            CompletableFuture<@Nullable Void> waitsFor = subscribeWaitsFor;
            if (waitsFor != null) {
                subscribeEntered.complete(null);
                waitsFor.join();
            }
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
            if (startErrorsOnce.compareAndSet(true, false)) {
                throw new AssertionError("The wrapped model failed with an Error on being started");
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
            if (pauseTakesEffectThenThrows) {
                throw new IllegalStateException("The wrapped model paused " + subscriptionId + " and then threw");
            }
        }

        private void throwIfRefused(String subscriptionId) {
            if (throwsOn.contains(subscriptionId)) {
                throw new IllegalStateException("The wrapped model cannot start " + subscriptionId + " right now");
            }
        }
    }

    /**
     * An {@link InMemorySubscriptionModel} that throws on the next {@code start(..)} when {@link #startThrowsOnce} is
     * set, starts and then throws on the next one when {@link #startTakesEffectThenThrowsOnce} is set, and throws on
     * resuming a subscription in {@link #resumeThrowsOn}. While {@link #isRunningIsSlowOnceOnATry} is set, the first
     * question on the thread of a try whether a subscription runs takes 700 ms.
     */
    private static final class FailingInMemory extends InMemorySubscriptionModel {
        private final AtomicBoolean startThrowsOnce = new AtomicBoolean();
        private final AtomicBoolean startTakesEffectThenThrowsOnce = new AtomicBoolean();
        private final Set<String> resumeThrowsOn = ConcurrentHashMap.newKeySet();
        private final AtomicBoolean isRunningIsSlowOnceOnATry = new AtomicBoolean();

        @Override
        public boolean isRunning(String subscriptionId) {
            if (Thread.currentThread().getName().startsWith("occurrent-competing-consumer-reconcile-") && isRunningIsSlowOnceOnATry.compareAndSet(true, false)) {
                try {
                    Thread.sleep(700);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.isRunning(subscriptionId);
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            if (startThrowsOnce.compareAndSet(true, false)) {
                throw new IllegalStateException("The wrapped model cannot start right now");
            }
            super.start(resumeSubscriptionsAutomatically);
            if (startTakesEffectThenThrowsOnce.compareAndSet(true, false)) {
                throw new IllegalStateException("The wrapped model started and then threw");
            }
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            if (resumeThrowsOn.contains(subscriptionId)) {
                throw new IllegalStateException("The wrapped model cannot resume " + subscriptionId + " right now");
            }
            return super.resumeSubscription(subscriptionId);
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
