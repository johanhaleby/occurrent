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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.IntrospectableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * {@code subscribe(..)} registers with the lease strategy and subscribes in the wrapped model without holding the
 * model's monitor, while lifecycle calls and lease callbacks run on other threads. Whatever runs in between, a
 * subscription delivers only while this node holds its lease, a failure that goes away does not keep it from running,
 * and only the user's cancel cancels it in the wrapped model.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CompetingConsumerSubscribeConcurrencyTest {

    // Longer than the 200 ms an event waits at most between two looks at the lease
    private static final Duration HELD_FOR = Duration.ofMillis(500);

    @Test
    void a_resume_that_starts_the_wrapped_model_while_a_stopped_model_subscribes_delivers_nothing_without_the_lease() throws Exception {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        strategy.heldElsewhere.add("s1");
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s0", null, StartAt.subscriptionModelDefault(), __ -> {});
        model.stop();

        // Another thread resumes s0, which starts the wrapped model, when the wrapped model is about to subscribe s1
        List<Thread> resuming = new CopyOnWriteArrayList<>();
        delegate.beforeSubscribe = id -> {
            if (id.equals("s1")) {
                delegate.beforeSubscribe = __ -> {};
                resuming.add(runOnAnotherThreadUntilDoneOrBlocked(() -> model.resumeSubscription("s0")));
            }
        };
        List<String> s1Received = new CopyOnWriteArrayList<>();
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId()));
        delegate.write("e1");
        model.start(false);
        delegate.write("e2");
        strategy.heldElsewhere.remove("s1");
        strategy.holders.add("s1");
        Throwable grantFailure = catchThrowable(() -> strategy.listeners.forEach(l -> l.onConsumeGranted("s1", "node")));
        for (Thread thread : resuming) {
            thread.join(SECONDS.toMillis(5));
        }

        assertThat(s1Received).as("events s1 received on a node that never held its lease").isEmpty();
        assertThat(grantFailure).as("the grant once this node wins the lease of s1").isNull();
        assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model once this node holds its lease").isTrue();
    }

    @Test
    void a_subscribe_that_a_stop_overtook_does_not_start_the_wrapped_model_after_stop_returned() throws Exception {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CountDownLatch release = new CountDownLatch(1);
        strategy.blockRegister.put("s1", release);
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        List<String> s1Received = new CopyOnWriteArrayList<>();
        CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId())));
        assertThat(strategy.registerEntered.await(5, SECONDS)).as("the registration of s1 began").isTrue();

        model.stop();
        delegate.afterSubscribe = id -> {
            if (id.equals("s1")) {
                delegate.afterSubscribe = __ -> {};
                delegate.write("written-after-stop-returned");
            }
        };
        release.countDown();
        assertThat(subscribing).succeedsWithin(Duration.ofSeconds(5));
        List<String> nonCompetingReceived = new CopyOnWriteArrayList<>();
        model.subscribe("n1", null, StartAt.dynamic(ctx -> ctx.hasSubscriptionModelType(CompetingConsumerSubscriptionModel.class) ? null : StartAt.subscriptionModelDefault()), e -> nonCompetingReceived.add(e.getId()));
        delegate.write("written-while-stopped");

        assertThat(model.isRunning()).as("model.isRunning() after stop() and the subscribe it overtook returned").isFalse();
        assertThat(s1Received).as("events s1 received after stop() returned").isEmpty();
        assertThat(nonCompetingReceived).as("events a non-competing subscription made while stopped received").isEmpty();
    }

    @Test
    void a_subscribe_that_a_stop_overtook_over_a_model_that_cannot_pause_delivers_nothing_without_its_lease() throws Exception {
        UserWrittenModel delegate = new UserWrittenModel(true);
        Strategy strategy = new Strategy();
        CountDownLatch release = new CountDownLatch(1);
        strategy.blockRegister.put("s1", release);
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        List<String> s1Received = new CopyOnWriteArrayList<>();
        CompletableFuture<Subscription> subscribing = CompletableFuture.supplyAsync(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId())));
        assertThat(strategy.registerEntered.await(5, SECONDS)).as("the registration of s1 began").isTrue();

        model.stop();
        release.countDown();
        Throwable failure = catchThrowable(() -> subscribing.get(5, SECONDS));
        delegate.write("e1");

        assertThat(failure).as("the subscribe of s1").isNull();
        assertThat(delegate.isRunning("s1") && !strategy.hasLock("s1", "node")).as("s1 runs in the wrapped model without its lease, received=" + s1Received).isFalse();
        assertThat(s1Received).as("events s1 received without its lease").isEmpty();
    }

    @Test
    void a_subscription_that_a_model_which_cannot_pause_runs_when_a_stop_overtakes_the_subscribe_keeps_its_lease() {
        UserWrittenModel delegate = new UserWrittenModel(true);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // Another thread stops the model once the wrapped model runs s1
        delegate.afterSubscribe = id -> {
            if (id.equals("s1")) {
                delegate.afterSubscribe = __ -> {};
                runOnAnotherThreadUntilDoneOrBlocked(model::stop);
            }
        };
        List<String> s1Received = new CopyOnWriteArrayList<>();

        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId()));
        delegate.write("e1");

        assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model that cannot pause it").isTrue();
        assertThat(strategy.hasLock("s1", "node")).as("lease held while the wrapped model delivers s1, received=" + s1Received).isTrue();
        assertThat(model.isRunning("s1")).as("recorded as running").isTrue();
    }

    @Test
    void a_resume_that_fails_once_after_a_start_overtook_the_subscribe_keeps_the_subscription_for_the_next_grant() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.stop();
        // Another thread starts the model once the wrapped model holds s1 paused
        delegate.afterSubscribe = id -> {
            delegate.afterSubscribe = __ -> {};
            runOnAnotherThreadUntilDoneOrBlocked(() -> model.start(false));
        };
        delegate.resumeFailsOnce = true;

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}));
        boolean heldPaused = delegate.isPaused("s1");
        boolean leaseHeldWhileWaiting = strategy.hasLock("s1", "node");
        grant(strategy, "s1");

        assertThat(failure).as("the subscribe of s1, whose resume failed once").isNull();
        assertThat(leaseHeldWhileWaiting).as("lease of s1 held while it waits for a grant, which it would never get").isFalse();
        assertThat(delegate.cancelled).as("subscriptions cancelled in the wrapped model").isEmpty();
        assertThat(heldPaused).as("s1 held paused in the wrapped model until the next grant").isTrue();
        assertThat(delegate.isRunning("s1")).as("s1 runs once this node wins its lease again").isTrue();
    }

    @Test
    void a_shutdown_that_overtakes_a_subscribe_cancels_nothing_in_the_wrapped_model() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // Another thread shuts the model down once the wrapped model has s1
        delegate.afterSubscribe = id -> {
            delegate.afterSubscribe = __ -> {};
            runOnAnotherThreadUntilDoneOrBlocked(model::shutdown);
        };

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}));

        assertThat(delegate.cancelled).as("subscriptions cancelled in the wrapped model").isEmpty();
        assertThat(failure).as("the subscribe of s1 that shutdown() overtook").isInstanceOf(IllegalStateException.class);
        assertThat(strategy.registered).as("registrations after shutdown").isEmpty();
    }

    @Test
    void a_subscription_whose_lease_goes_to_another_node_while_the_wrapped_model_makes_it_delivers_nothing_until_a_grant() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        delegate.implementsSubscribePaused = true;
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // The lease goes to another node, and an event is written, once the wrapped model has s1
        delegate.afterSubscribe = id -> {
            delegate.afterSubscribe = __ -> {};
            strategy.holders.remove("s1");
            strategy.heldElsewhere.add("s1");
            delegate.write("e1");
        };
        List<String> s1Received = new CopyOnWriteArrayList<>();

        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId()));
        List<String> receivedWithoutTheLease = List.copyOf(s1Received);
        boolean leaseHeldWhileWaiting = strategy.hasLock("s1", "node");
        strategy.heldElsewhere.remove("s1");
        grant(strategy, "s1");

        assertThat(leaseHeldWhileWaiting).as("lease of s1 held while it waits for a grant").isFalse();
        assertThat(receivedWithoutTheLease).as("events s1 received while another node held its lease").isEmpty();
        assertThat(s1Received).as("events s1 received once this node won its lease").containsExactly("e1");
    }

    @Test
    void a_pause_that_fails_after_pausing_a_subscription_whose_lease_went_elsewhere_during_the_subscribe_waits_for_the_next_grant() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // The lease goes to another node once the wrapped model runs s1, and pausing it there throws once it is paused
        delegate.afterSubscribe = id -> {
            delegate.afterSubscribe = __ -> {};
            strategy.holders.remove("s1");
            strategy.heldElsewhere.add("s1");
            delegate.pauseFailsOnce = true;
            delegate.pauseFailsAfterPausing = true;
        };

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}));
        boolean runningWithoutTheLease = delegate.isRunning("s1");
        boolean leaseHeldWhileWaiting = strategy.hasLock("s1", "node");
        strategy.heldElsewhere.remove("s1");
        grant(strategy, "s1");

        assertThat(leaseHeldWhileWaiting).as("lease of s1 held while it waits for a grant").isFalse();
        assertThat(delegate.isRunning("s1")).as("s1 runs once this node wins its lease again, subscribe failure=" + failure).isTrue();
        assertThat(runningWithoutTheLease).as("s1 ran while another node held its lease").isFalse();
        assertThat(failure).as("the subscribe of s1").isNull();
    }

    @Test
    void a_pause_that_fails_once_the_lease_went_elsewhere_during_the_subscribe_is_tried_again_until_the_wrapped_model_stops_the_subscription() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // The lease goes to another node once the wrapped model runs s1, and pausing it there throws before pausing it
        delegate.afterSubscribe = id -> {
            delegate.afterSubscribe = __ -> {};
            strategy.holders.remove("s1");
            strategy.heldElsewhere.add("s1");
            delegate.pauseFailsOnce = true;
        };
        List<String> s1Received = new CopyOnWriteArrayList<>();

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId())));

        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model while another node holds its lease, subscribe failure=" + failure).isFalse());
        delegate.write("e1");
        assertThat(s1Received).as("events s1 received while another node held its lease").isEmpty();
        assertThat(failure).as("the subscribe of s1").isNull();
        assertThat(strategy.registered).as("registrations competing for a lease").containsExactly("s1");
    }

    @Test
    void a_pause_that_fails_once_this_node_lost_the_lease_of_a_running_subscription_is_tried_again_until_the_wrapped_model_stops_it() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        List<String> s1Received = new CopyOnWriteArrayList<>();
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId()));
        delegate.write("e1");
        strategy.holders.remove("s1");
        strategy.heldElsewhere.add("s1");
        delegate.pauseFailsOnce = true;

        Throwable callbackFailure = catchThrowable(() -> strategy.listeners.forEach(l -> l.onConsumeProhibited("s1", "node")));

        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model while another node holds its lease, callback failure=" + callbackFailure).isFalse());
        delegate.write("e2");
        assertThat(s1Received).as("events s1 received").containsExactly("e1");
        assertThat(callbackFailure).as("the lease-loss callback").isNull();
    }

    @Test
    void a_subscription_whose_registration_fails_once_a_start_overtook_the_subscribe_is_recorded_and_registers_and_runs() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        delegate.implementsSubscribePaused = true;
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.stop();
        // Another thread starts the model once the wrapped model holds s1 paused, and the registration that follows fails
        delegate.afterSubscribe = id -> {
            delegate.afterSubscribe = __ -> {};
            runOnAnotherThreadUntilDoneOrBlocked(() -> model.start(false));
        };
        strategy.registerFailsOnce.add("s1");
        List<String> s1Received = new CopyOnWriteArrayList<>();

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId())));
        Set<String> recordedAfterTheFailure = model.subscriptionIds();

        try {
            assertThat(failure).as("the subscribe of s1, whose registration failed").isNull();
            assertThat(recordedAfterTheFailure).as("subscriptions recorded once the subscribe had returned").containsExactly("s1");
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model once registered, registered=" + strategy.registered).isTrue());
            delegate.write("e1");
            assertThat(delegate.cancelled).as("subscriptions cancelled in the wrapped model").isEmpty();
            assertThat(s1Received).as("events s1 received").containsExactly("e1");
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_subscription_that_cannot_give_the_lease_back_after_a_failed_resume_is_recorded_and_runs_once_tried_again() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.stop();
        // Another thread starts the model once the wrapped model holds s1 paused, resuming it fails once, and so does
        // giving the lease back after that
        delegate.afterSubscribe = id -> {
            delegate.afterSubscribe = __ -> {};
            runOnAnotherThreadUntilDoneOrBlocked(() -> model.start(false));
        };
        delegate.resumeFailsOnce = true;
        strategy.releaseFailsOnce = true;

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}));

        try {
            assertThat(failure).as("the subscribe of s1, which could not give the lease back").isNull();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model while this node holds its lease, holders=" + strategy.holders).isTrue());
            assertThat(strategy.hasLock("s1", "node")).as("lease of s1 held while it runs").isTrue();
            assertThat(delegate.cancelled).as("subscriptions cancelled in the wrapped model").isEmpty();
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_subscription_that_a_stop_and_then_a_start_overtook_while_the_wrapped_model_made_it_paused_registers_again_and_runs() {
        aStopAndThenAStartOvertakeTheSubscribeWhileTheWrappedModelMakesTheSubscription(true);
    }

    @Test
    void a_subscription_that_a_stop_and_then_a_start_overtook_while_a_model_refusing_subscribePaused_made_it_registers_again_and_runs() {
        aStopAndThenAStartOvertakeTheSubscribeWhileTheWrappedModelMakesTheSubscription(false);
    }

    private static void aStopAndThenAStartOvertakeTheSubscribeWhileTheWrappedModelMakesTheSubscription(boolean implementsSubscribePaused) {
        UserWrittenModel delegate = new UserWrittenModel(false);
        delegate.implementsSubscribePaused = implementsSubscribePaused;
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // Other threads stop the model and then start it, once this node holds the lease and the wrapped model has s1
        delegate.afterSubscribe = id -> {
            delegate.afterSubscribe = __ -> {};
            join(runOnAnotherThreadUntilDoneOrBlocked(model::stop));
            join(runOnAnotherThreadUntilDoneOrBlocked(() -> model.start(true)));
        };
        List<String> s1Received = new CopyOnWriteArrayList<>();

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId())));
        delegate.write("e1");

        assertThat(failure).as("the subscribe of s1").isNull();
        assertThat(strategy.registered).as("registrations once start() and the subscribe of s1 had returned, holders=" + strategy.holders + ", paused in the wrapped model=" + delegate.isPaused("s1")).containsExactly("s1");
        assertThat(s1Received).as("events s1 received once the model was started again").containsExactly("e1");
    }

    @Test
    void a_subscription_that_a_model_which_cannot_pause_runs_after_a_stop_gave_up_its_registration_registers_again() {
        UserWrittenModel delegate = new UserWrittenModel(true);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // Another thread stops the model when the wrapped model is about to make s1, and the wrapped model is started
        // again before it does, as a resume of another subscription starts it
        delegate.beforeSubscribe = id -> {
            delegate.beforeSubscribe = __ -> {};
            join(runOnAnotherThreadUntilDoneOrBlocked(model::stop));
            delegate.start(false);
        };

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}));

        assertThat(failure).as("the subscribe of s1").isNull();
        assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model that cannot pause it").isTrue();
        assertThat(strategy.registered).as("registrations while the wrapped model delivers s1").containsExactly("s1");
        assertThat(strategy.hasLock("s1", "node")).as("lease held while the wrapped model delivers s1").isTrue();
    }

    @Test
    void a_subscribe_that_no_longer_holds_the_lease_after_giving_it_back_failed_returns_and_waits_for_the_next_grant() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.stop();
        // Another thread starts the model once the wrapped model holds s1 paused, resuming it fails once, and giving the
        // lease back after that throws once the strategy has given it up, as the MongoDB strategies do
        delegate.afterSubscribe = id -> {
            delegate.afterSubscribe = __ -> {};
            runOnAnotherThreadUntilDoneOrBlocked(() -> model.start(false));
        };
        delegate.resumeFailsOnce = true;
        strategy.releaseFailsOnceAfterGivingUpTheLease = true;

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}));
        boolean leaseHeldWhileWaiting = strategy.hasLock("s1", "node");
        Set<String> registeredWhileWaiting = Set.copyOf(strategy.registered);
        grant(strategy, "s1");

        assertThat(failure).as("the subscribe of s1, which no longer holds the lease").isNull();
        assertThat(leaseHeldWhileWaiting).as("lease of s1 held while it waits for a grant").isFalse();
        assertThat(registeredWhileWaiting).as("registrations while s1 waits for a grant").containsExactly("s1");
        assertThat(delegate.isRunning("s1")).as("s1 runs once this node wins its lease again").isTrue();
    }

    @Test
    void a_pause_that_keeps_failing_once_this_node_lost_the_lease_is_logged_as_a_warning_again_while_it_is_tried() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});
        List<String> warnings = new CopyOnWriteArrayList<>();
        AppenderBase<ILoggingEvent> appender = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getLevel() == Level.WARN) {
                    warnings.add(event.getFormattedMessage());
                }
            }
        };
        Logger logger = (Logger) LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class);
        appender.start();
        logger.addAppender(appender);
        try {
            strategy.holders.remove("s1");
            strategy.heldElsewhere.add("s1");
            delegate.pauseKeepsFailing = true;

            strategy.listeners.forEach(l -> l.onConsumeProhibited("s1", "node"));

            await().atMost(8, SECONDS).untilAsserted(() -> assertThat(warnings).as("warnings while the pause of s1 keeps failing")
                    .anySatisfy(warning -> assertThat(warning).contains("Still could not").contains("after 5 tries").contains("subscriptionId=s1")));
        } finally {
            logger.detachAppender(appender);
            model.shutdown();
        }
    }

    @Test
    void a_subscription_whose_registration_a_stop_could_not_give_up_cleanly_registers_again_once_a_start_overtook_the_subscribe() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        delegate.implementsSubscribePaused = true;
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // Other threads stop the model and then start it, once this node holds the lease and the wrapped model has s1.
        // The unregister in stop() throws once the strategy has forgotten the consumer, as the MongoDB strategies do.
        List<Throwable> stopFailures = new CopyOnWriteArrayList<>();
        delegate.afterSubscribe = id -> {
            delegate.afterSubscribe = __ -> {};
            strategy.unregisterFailsOnceAfterForgettingTheConsumer = true;
            join(runOnAnotherThreadUntilDoneOrBlocked(() -> stopFailures.add(catchThrowable(model::stop))));
            join(runOnAnotherThreadUntilDoneOrBlocked(() -> model.start(true)));
        };
        List<String> s1Received = new CopyOnWriteArrayList<>();

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId())));
        delegate.write("e1");

        assertThat(failure).as("the subscribe of s1").isNull();
        assertThat(stopFailures).as("what stop() threw").singleElement().satisfies(e -> assertThat(e).hasMessage("transient unregister failure"));
        assertThat(strategy.registered).as("registrations once start() and the subscribe of s1 had returned, holders=" + strategy.holders + ", paused in the wrapped model=" + delegate.isPaused("s1")).containsExactly("s1");
        assertThat(s1Received).as("events s1 received once the model was started again").containsExactly("e1");
    }

    @Test
    void a_subscription_that_the_wrapped_model_still_runs_when_its_subscribe_fails_is_recorded_as_running_and_the_subscribe_returns() {
        UserWrittenModel delegate = new UserWrittenModel(true);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // Another thread stops the model when the wrapped model is about to make s1, the wrapped model is started again
        // before it does, as a resume of another subscription starts it, and registering s1 again fails
        delegate.beforeSubscribe = id -> {
            delegate.beforeSubscribe = __ -> {};
            join(runOnAnotherThreadUntilDoneOrBlocked(model::stop));
            delegate.start(false);
            strategy.registerFailsOnce.add("s1");
        };
        List<String> s1Received = new CopyOnWriteArrayList<>();

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId())));
        Set<String> ids = model.subscriptionIds();
        Throwable pause = catchThrowable(() -> model.pauseSubscription("s1"));

        assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model that cannot pause it").isTrue();
        assertThat(ids).as("subscriptionIds() while the wrapped model runs s1, subscribe failure=" + failure + ", received=" + s1Received).containsExactly("s1");
        assertThat(pause instanceof UnknownSubscriptionException).as("pausing s1, which the wrapped model runs, answers as for an unknown id, pause=" + pause).isFalse();
        assertThat(failure).as("the subscribe of s1, which the wrapped model runs").isNull();
    }

    @Test
    void an_event_this_node_may_not_deliver_does_not_hold_up_a_pause_on_a_platform_thread_of_a_model_that_delivers_under_its_lock() {
        aPauseWhileAModelThatDeliversUnderItsLockHoldsAnEvent(Thread.ofPlatform());
    }

    @Test
    void an_event_this_node_may_not_deliver_does_not_hold_up_a_pause_on_a_virtual_thread_of_a_model_that_delivers_under_its_lock() {
        aPauseWhileAModelThatDeliversUnderItsLockHoldsAnEvent(Thread.ofVirtual());
    }

    // The lease of s1 could have expired, and the lease strategy says so only when asked. The wrapped model takes its
    // lock to pause s1, so the pause lets the event through.
    private static void aPauseWhileAModelThatDeliversUnderItsLockHoldsAnEvent(Thread.Builder pausing) {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        try {
            List<String> s1Received = new CopyOnWriteArrayList<>();
            model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId()));
            strategy.holders.remove("s1");
            CompletableFuture<Throwable> write = writeOnAnotherThreadUntilItWaits(delegate, "e1");

            CompletableFuture<Void> pause = new CompletableFuture<>();
            pausing.start(() -> {
                try {
                    model.pauseSubscription("s1");
                    pause.complete(null);
                } catch (Throwable e) {
                    pause.completeExceptionally(e);
                }
            });

            assertThat(pause).as("[the pause of s1 returned while its event waited for the lease]").succeedsWithin(Duration.ofSeconds(5));
            assertThat(write).as("the write").succeedsWithin(Duration.ofSeconds(5));
            assertThat(write.join()).as("[what the write threw]").isNull();
            assertThat(s1Received).as("[events s1 received, delivered as the pause let them through]").containsExactly("e1");
            assertThat(delegate.isPaused("s1")).as("s1 paused in the wrapped model").isTrue();
        } finally {
            strategy.holders.add("s1");
            model.shutdown();
        }
    }

    @Test
    void a_loss_of_the_lease_told_on_a_virtual_thread_pauses_a_model_that_delivers_under_its_lock_while_an_event_waits() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        try {
            List<String> s1Received = new CopyOnWriteArrayList<>();
            model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId()));
            strategy.holders.remove("s1");
            strategy.heldElsewhere.add("s1");
            CompletableFuture<Throwable> write = writeOnAnotherThreadUntilItWaits(delegate, "e1");

            CompletableFuture<Void> told = new CompletableFuture<>();
            Thread.ofVirtual().start(() -> {
                try {
                    strategy.listeners.forEach(listener -> listener.onConsumeProhibited("s1", "node"));
                    told.complete(null);
                } catch (Throwable e) {
                    told.completeExceptionally(e);
                }
            });

            assertThat(told).as("[the loss of the lease of s1, told on a virtual thread, returned]").succeedsWithin(Duration.ofSeconds(5));
            assertThat(write).as("the write").succeedsWithin(Duration.ofSeconds(5));
            assertThat(write.join()).as("[what the write threw]").isNull();
            assertThat(delegate.isPaused("s1")).as("[s1 paused in the wrapped model once this node lost its lease]").isTrue();
        } finally {
            strategy.heldElsewhere.remove("s1");
            strategy.holders.add("s1");
            model.shutdown();
        }
    }

    @Test
    void a_platform_thread_of_the_application_waiting_for_a_lock_the_delivering_thread_holds_changes_nothing_about_an_event_that_waits_for_the_lease() {
        anApplicationThreadWaitsForALockTheDeliveringThreadHolds(Thread.ofPlatform());
    }

    @Test
    void a_virtual_thread_of_the_application_waiting_for_a_lock_the_delivering_thread_holds_changes_nothing_about_an_event_that_waits_for_the_lease() {
        anApplicationThreadWaitsForALockTheDeliveringThreadHolds(Thread.ofVirtual());
    }

    // The writing thread holds a lock of the application's own while the lease of s1 could have expired, and another
    // thread of the application waits for that lock. The event waits for the lease all the same.
    private static void anApplicationThreadWaitsForALockTheDeliveringThreadHolds(Thread.Builder waiting) {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        Object applicationLock = new Object();
        try {
            List<String> s1Received = new CopyOnWriteArrayList<>();
            model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId()));
            strategy.holders.remove("s1");
            CompletableFuture<Throwable> write = new CompletableFuture<>();
            Thread writer = Thread.ofPlatform().daemon().start(() -> {
                synchronized (applicationLock) {
                    write.complete(catchThrowable(() -> delegate.write("e1")));
                }
            });
            await().atMost(Duration.ofSeconds(5)).until(() -> writer.getState() == Thread.State.TIMED_WAITING);
            Thread waiter = waiting.start(() -> {
                synchronized (applicationLock) {
                    applicationLock.notifyAll();
                }
            });
            // A write that gives the event up lets go of the lock, so the waiter never stays blocked
            await().atMost(Duration.ofSeconds(5)).until(() -> waiter.getState() == Thread.State.BLOCKED || write.isDone());

            await().during(HELD_FOR).atMost(HELD_FOR.multipliedBy(2)).untilAsserted(() ->
                    assertThat(write).as("[the write of an event this node may not deliver, while another thread waits for a lock its thread holds]").isNotDone());
            assertThat(s1Received).as("events s1 received without its lease").isEmpty();
            strategy.holders.add("s1");

            assertThat(write).as("the write once this node holds the lease again").succeedsWithin(Duration.ofSeconds(5));
            assertThat(write.join()).as("[what the write threw]").isNull();
            assertThat(s1Received).as("[events s1 received once this node held the lease again]").containsExactly("e1");
        } finally {
            strategy.holders.add("s1");
            model.shutdown();
        }
    }

    @Test
    void an_event_for_a_subscription_without_its_lease_that_a_model_which_cannot_pause_runs_while_this_model_is_stopped_waits_for_start() {
        UserWrittenModel delegate = new UserWrittenModel(true);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // A stop() overtakes the subscribe, the wrapped model is started again by a resume of another subscription, and
        // registering s1 again fails
        delegate.beforeSubscribe = id -> {
            delegate.beforeSubscribe = __ -> {};
            join(runOnAnotherThreadUntilDoneOrBlocked(model::stop));
            delegate.start(false);
            strategy.registerFailsOnce.add("s1");
        };
        try {
            List<String> s1Received = new CopyOnWriteArrayList<>();
            catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId())));
            await().atMost(Duration.ofSeconds(5)).until(() -> Thread.getAllStackTraces().keySet().stream().noneMatch(thread -> thread.getName().startsWith("occurrent-competing-consumer-reconcile-")));

            CompletableFuture<Throwable> write = writeOnAnotherThreadUntilItWaits(delegate, "e1");

            await().during(HELD_FOR).atMost(HELD_FOR.multipliedBy(2)).untilAsserted(() ->
                    assertThat(write).as("[the write while this model is stopped and s1 has no lease]").isNotDone());
            assertThat(s1Received).as("events s1 received while this model is stopped and s1 has no lease").isEmpty();
            model.start(false);

            assertThat(write).as("the write once this model was started").succeedsWithin(Duration.ofSeconds(5));
            assertThat(write.join()).as("[what the write threw]").isNull();
            assertThat(s1Received).as("[events s1 received once this model was started]").containsExactly("e1");
            assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model that cannot pause it").isTrue();
        } finally {
            model.shutdown();
        }
    }

    // The user paused s1, which gave its lease up, and the wrapped model cannot pause it, so it still delivers to s1
    // while holding the lock its isRunning takes
    @Test
    void a_resume_of_a_subscription_that_a_model_which_cannot_pause_runs_returns_while_its_event_waits_for_the_lease() {
        UserWrittenModel delegate = new UserWrittenModel(true);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        try {
            List<String> s1Received = new CopyOnWriteArrayList<>();
            model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId()));
            catchThrowable(() -> model.pauseSubscription("s1"));
            strategy.holders.remove("s1");
            CompletableFuture<Throwable> write = writeOnAnotherThreadUntilItWaits(delegate, "e1");
            assertThat(write).as("the write of an event for s1 without its lease").isNotDone();

            CompletableFuture<Throwable> resume = new CompletableFuture<>();
            Thread.ofPlatform().daemon().start(() -> resume.complete(catchThrowable(() -> model.resumeSubscription("s1"))));

            assertThat(resume).as("[the resume of s1 returned while its event waited for the lease]").succeedsWithin(Duration.ofSeconds(5));
            assertThat(write).as("the write").succeedsWithin(Duration.ofSeconds(5));
            assertThat(write.join()).as("[what the write threw]").isNull();
            assertThat(s1Received).as("[events s1 received, delivered as the resume let them through]").containsExactly("e1");
        } finally {
            model.shutdown();
        }
    }

    // Returns once the write waits, with what the write throws once it returns
    private static CompletableFuture<Throwable> writeOnAnotherThreadUntilItWaits(UserWrittenModel delegate, String eventId) {
        CompletableFuture<Throwable> write = new CompletableFuture<>();
        Thread writer = Thread.ofPlatform().daemon().start(() -> write.complete(catchThrowable(() -> delegate.write(eventId))));
        await().atMost(Duration.ofSeconds(5)).until(() -> writer.getState() == Thread.State.TIMED_WAITING || write.isDone());
        return write;
    }

    @Test
    void a_waiting_subscription_whose_registration_throws_in_start_is_registered_once_tried_again() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        strategy.heldElsewhere.add("s1");
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});
        model.stop();
        strategy.registerFailsOnce.add("s1");

        Throwable start = catchThrowable(() -> model.start(true));

        try {
            assertThat(start).as("the start() whose registration of s1 threw").isNull();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(strategy.registered).as("registrations once the registration of s1 is tried again").containsExactly("s1"));
            assertThat(delegate.isRunning("s1")).as("s1 while another node holds its lease").isFalse();
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_subscription_the_wrapped_model_runs_when_its_registration_fails_after_a_stop_and_a_start_registers_and_runs_with_the_lease() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // Other threads stop the model and then start it when the wrapped model is about to make s1, the wrapped model is
        // started again before it does, and registering s1 again fails once, and so does pausing it
        delegate.beforeSubscribe = id -> {
            delegate.beforeSubscribe = __ -> {};
            join(runOnAnotherThreadUntilDoneOrBlocked(model::stop));
            join(runOnAnotherThreadUntilDoneOrBlocked(() -> model.start(true)));
            delegate.start(false);
            strategy.registerFailsOnce.add("s1");
            delegate.pauseFailsOnce = true;
        };
        List<String> s1Received = new CopyOnWriteArrayList<>();

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId())));

        try {
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(strategy.registered).as("registrations once the subscribe of s1 had returned, subscribe failure=" + failure
                    + ", holders=" + strategy.holders + ", paused in the wrapped model=" + delegate.isPaused("s1") + ", paused here=" + model.isPaused("s1")).containsExactly("s1"));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model once this node holds its lease, holders=" + strategy.holders).isTrue());
            delegate.write("e1");
            assertThat(s1Received).as("events s1 received").containsExactly("e1");
            assertThat(strategy.hasLock("s1", "node")).as("lease of s1 held while it runs").isTrue();
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_subscription_that_runs_without_the_lease_when_its_registration_fails_registers_and_stops_being_tried_once_this_node_wins_the_lease() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // As above, but another node holds the lease by then and pausing s1 keeps failing
        delegate.beforeSubscribe = id -> {
            delegate.beforeSubscribe = __ -> {};
            join(runOnAnotherThreadUntilDoneOrBlocked(model::stop));
            join(runOnAnotherThreadUntilDoneOrBlocked(() -> model.start(true)));
            delegate.start(false);
            strategy.registerFailsOnce.add("s1");
            strategy.heldElsewhere.add("s1");
            delegate.pauseKeepsFailing = true;
        };

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}));

        try {
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(strategy.registered).as("registrations while another node holds the lease of s1, subscribe failure=" + failure).containsExactly("s1"));
            strategy.heldElsewhere.remove("s1");
            grant(strategy, "s1");
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(threadsTryingAgain("s1")).as("threads trying s1 again once this node holds its lease").isEmpty());
            assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model once this node holds its lease").isTrue();
        } finally {
            delegate.pauseKeepsFailing = false;
            model.shutdown();
        }
    }

    @Test
    void a_registration_that_a_failed_subscribe_could_not_give_up_while_holding_the_lease_is_given_up_afterwards() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        delegate.implementsSubscribePaused = true;
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // Making s1 in the wrapped model fails, and so does the unregister after it, before the strategy forgets s1
        delegate.beforeSubscribe = id -> {
            strategy.unregisterFailsOnceBeforeForgettingTheConsumer = true;
            throw new IllegalStateException("transient subscribe failure");
        };

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}));

        try {
            assertThat(failure).as("the subscribe of s1").hasMessage("transient subscribe failure");
            assertThat(model.subscriptionIds()).as("subscriptions recorded once the subscribe of s1 had thrown").isEmpty();
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(strategy.registered).as("registrations once the subscribe of s1 had thrown, holders=" + strategy.holders).isEmpty());
            assertThat(strategy.holders).as("leases held once the subscribe of s1 had thrown").isEmpty();
        } finally {
            model.shutdown();
        }
    }

    @Test
    void stop_gives_up_a_lease_that_a_subscribe_it_overtook_has_already_won() throws Exception {
        UserWrittenModel delegate = new UserWrittenModel(false);
        delegate.implementsSubscribePaused = true;
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        // Another thread stops the model once this node holds the lease of s1 and the wrapped model is about to make it
        List<Boolean> leaseHeldOnceStopReturned = new CopyOnWriteArrayList<>();
        delegate.beforeSubscribe = id -> {
            delegate.beforeSubscribe = __ -> {};
            Thread stopping = runOnAnotherThreadUntilDoneOrBlocked(model::stop);
            try {
                stopping.join(SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            leaseHeldOnceStopReturned.add(strategy.hasLock("s1", "node"));
        };

        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});

        assertThat(leaseHeldOnceStopReturned).as("lease of s1 held once stop() had returned").containsExactly(false);
        assertThat(strategy.registered).as("registrations competing for a lease while stopped").isEmpty();
    }

    @Test
    void a_subscribe_after_shutdown_throws_and_neither_registers_nor_subscribes() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.shutdown();

        Throwable failure = catchThrowable(() -> model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {}));

        assertThat(failure).isInstanceOf(IllegalStateException.class);
        assertThat(strategy.registered).as("registrations after shutdown").isEmpty();
        assertThat(delegate.isRunning("s1") || delegate.isPaused("s1")).as("s1 in the wrapped model after shutdown").isFalse();
    }

    @Test
    void shutdown_returns_while_start_retries_a_registration_under_the_monitor() throws Exception {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        strategy.heldElsewhere.add("s1");
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});
        model.stop();
        // Registering on start() waits until the strategy is shut down, as a registration retrying through an outage does
        strategy.blockRegister.put("s1", strategy.shutDown);
        CompletableFuture<Void> starting = CompletableFuture.runAsync(() -> model.start(false));
        try {
            assertThat(strategy.registerEntered.await(5, SECONDS)).as("start() began registering s1").isTrue();

            CompletableFuture<Void> shuttingDown = CompletableFuture.runAsync(model::shutdown);

            assertThat(shuttingDown).as("shutdown() while start() retries a registration").succeedsWithin(Duration.ofSeconds(5));
        } finally {
            strategy.shutDown.countDown();
        }
        assertThat(starting).succeedsWithin(Duration.ofSeconds(5));
    }

    @Test
    void a_pause_that_takes_effect_and_then_throws_once_this_node_lost_the_lease_is_resumed_by_the_next_grant() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        List<String> s1Received = new CopyOnWriteArrayList<>();
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId()));
        delegate.write("e1");
        strategy.holders.remove("s1");
        strategy.heldElsewhere.add("s1");
        delegate.pauseFailsOnce = true;
        delegate.pauseFailsAfterPausing = true;

        try {
            Throwable callbackFailure = catchThrowable(() -> strategy.listeners.forEach(l -> l.onConsumeProhibited("s1", "node")));
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(threadsTryingAgain("s1")).as("threads trying s1 again once this node lost its lease").isEmpty());
            strategy.heldElsewhere.remove("s1");
            grant(strategy, "s1");

            assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model once this node won its lease again, callback failure=" + callbackFailure
                    + ", paused in the wrapped model=" + delegate.isPaused("s1") + ", holders=" + strategy.holders).isTrue();
            delegate.write("e2");
            assertThat(s1Received).as("events s1 received").containsExactly("e1", "e2");
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_stop_whose_pause_takes_effect_and_then_throws_over_a_model_that_keeps_running_is_resumed_by_start() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        delegate.keepsSubscriptionsRunningWhenStopped = true;
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        List<String> s1Received = new CopyOnWriteArrayList<>();
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId()));
        delegate.pauseFailsOnce = true;
        delegate.pauseFailsAfterPausing = true;

        try {
            Throwable stopFailure = catchThrowable(model::stop);
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(strategy.registered).as("registrations once stop() had thrown, stop failure=" + stopFailure).isEmpty());
            model.start(true);

            assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model once started, registered=" + strategy.registered + ", holders=" + strategy.holders
                    + ", paused in the wrapped model=" + delegate.isPaused("s1")).isTrue();
            assertThat(strategy.hasLock("s1", "node")).as("lease of s1 held while it runs").isTrue();
            delegate.write("e1");
            assertThat(s1Received).as("events s1 received").containsExactly("e1");
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_user_pause_that_takes_effect_and_then_throws_gives_up_the_registration_and_the_lease() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});
        delegate.pauseFailsOnce = true;
        delegate.pauseFailsAfterPausing = true;

        try {
            Throwable failure = catchThrowable(() -> model.pauseSubscription("s1"));

            assertThat(failure).as("the pause of s1").hasMessage("transient pause failure");
            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(strategy.registered).as("registrations once the pause of s1 had thrown, holders=" + strategy.holders
                    + ", threads trying again=" + threadsTryingAgain("s1")).isEmpty());
            assertThat(strategy.holders).as("leases held once the pause of s1 had thrown").isEmpty();
            assertThat(model.isPaused("s1")).as("s1 paused").isTrue();
            model.resumeSubscription("s1");
            assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model once resumed").isTrue();
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_grant_whose_lease_check_throws_is_tried_again_until_the_subscription_runs() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        strategy.heldElsewhere.add("s1");
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});
        strategy.heldElsewhere.remove("s1");
        strategy.hasLockFailsOnce = true;

        try {
            Throwable callbackFailure = catchThrowable(() -> grant(strategy, "s1"));

            await().atMost(5, SECONDS).untilAsserted(() -> assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model once this node holds its lease, callback failure=" + callbackFailure
                    + ", threads trying again=" + threadsTryingAgain("s1")).isTrue());
            assertThat(callbackFailure).as("the grant callback").isNull();
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_cancel_that_takes_effect_and_then_throws_forgets_the_subscription_and_gives_up_its_lease() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});
        delegate.cancelFailsAfterCancelling = true;

        try {
            Throwable failure = catchThrowable(() -> model.cancelSubscription("s1"));

            assertThat(failure).as("the cancel of s1").hasMessage("transient cancel failure");
            assertThat(model.subscriptionIds()).as("subscriptions once the cancel of s1 had thrown, registered=" + strategy.registered + ", holders=" + strategy.holders).isEmpty();
            assertThat(strategy.registered).as("registrations once the cancel of s1 had thrown").isEmpty();
            assertThat(strategy.holders).as("leases held once the cancel of s1 had thrown").isEmpty();
            model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), __ -> {});
            assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model once subscribed again").isTrue();
        } finally {
            model.shutdown();
        }
    }

    @Test
    void a_subscription_the_wrapped_model_runs_after_its_subscribe_on_a_grant_threw_keeps_its_lease() {
        UserWrittenModel delegate = new UserWrittenModel(false);
        Strategy strategy = new Strategy();
        strategy.heldElsewhere.add("s1");
        CompetingConsumerSubscriptionModel model = new CompetingConsumerSubscriptionModel(delegate, strategy);
        List<String> s1Received = new CopyOnWriteArrayList<>();
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> s1Received.add(e.getId()));
        strategy.heldElsewhere.remove("s1");
        // The wrapped model runs s1 when this node wins its lease, and throws after that
        delegate.afterSubscribe = id -> {
            delegate.afterSubscribe = __ -> {};
            throw new IllegalStateException("transient subscribe failure");
        };

        try {
            Throwable callbackFailure = catchThrowable(() -> grant(strategy, "s1"));

            assertThat(delegate.isRunning("s1")).as("s1 runs in the wrapped model, callback failure=" + callbackFailure).isTrue();
            assertThat(strategy.hasLock("s1", "node")).as("lease of s1 held while it runs").isTrue();
            delegate.write("e1");
            assertThat(s1Received).as("events s1 received").containsExactly("e1");
        } finally {
            model.shutdown();
        }
    }

    // Threads of the model that try something for the subscription again
    private static List<String> threadsTryingAgain(String subscriptionId) {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(name -> name.startsWith("occurrent-competing-consumer-") && name.endsWith("-" + subscriptionId))
                .toList();
    }

    // Gives this node the lease, as a refresh that wins it does
    private static void grant(Strategy strategy, String subscriptionId) {
        strategy.holders.add(subscriptionId);
        strategy.listeners.forEach(l -> l.onConsumeGranted(subscriptionId, "node"));
    }

    private static void join(Thread thread) {
        try {
            thread.join(SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // Runs the call on a new thread and returns once it has returned, or once it waits for a monitor that the calling
    // thread holds, so a hook the wrapped model calls with the monitor held does not wait for good
    private static Thread runOnAnotherThreadUntilDoneOrBlocked(Runnable call) {
        Thread thread = new Thread(call);
        thread.start();
        long deadline = System.nanoTime() + SECONDS.toNanos(5);
        while (thread.isAlive() && thread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        return thread;
    }

    // Grants a lease unless another node holds it, makes a registration wait on a latch when told to, and fails a
    // registration or a release once when told to, the release either before or after it has given up the lease. It can
    // also fail an unregister once, either before or after it has forgotten the consumer, and a lease check once.
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Set<String> holders = ConcurrentHashMap.newKeySet();
        private final Set<String> heldElsewhere = ConcurrentHashMap.newKeySet();
        private final Set<String> registered = ConcurrentHashMap.newKeySet();
        private final Map<String, CountDownLatch> blockRegister = new ConcurrentHashMap<>();
        private final Set<String> registerFailsOnce = ConcurrentHashMap.newKeySet();
        private volatile boolean releaseFailsOnce;
        private volatile boolean releaseFailsOnceAfterGivingUpTheLease;
        private volatile boolean unregisterFailsOnceAfterForgettingTheConsumer;
        private volatile boolean unregisterFailsOnceBeforeForgettingTheConsumer;
        private volatile boolean hasLockFailsOnce;
        private final CountDownLatch registerEntered = new CountDownLatch(1);
        private final CountDownLatch shutDown = new CountDownLatch(1);
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            CountDownLatch latch = blockRegister.remove(subscriptionId);
            if (latch != null) {
                registerEntered.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (registerFailsOnce.remove(subscriptionId)) {
                throw new IllegalStateException("transient register failure");
            }
            registered.add(subscriptionId);
            if (heldElsewhere.contains(subscriptionId)) {
                return false;
            }
            holders.add(subscriptionId);
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            if (unregisterFailsOnceBeforeForgettingTheConsumer) {
                unregisterFailsOnceBeforeForgettingTheConsumer = false;
                throw new IllegalStateException("transient unregister failure");
            }
            registered.remove(subscriptionId);
            holders.remove(subscriptionId);
            if (unregisterFailsOnceAfterForgettingTheConsumer) {
                unregisterFailsOnceAfterForgettingTheConsumer = false;
                throw new IllegalStateException("transient unregister failure");
            }
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            if (releaseFailsOnce) {
                releaseFailsOnce = false;
                throw new IllegalStateException("transient release failure");
            }
            holders.remove(subscriptionId);
            if (releaseFailsOnceAfterGivingUpTheLease) {
                releaseFailsOnceAfterGivingUpTheLease = false;
                throw new IllegalStateException("transient release failure");
            }
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            if (hasLockFailsOnce) {
                hasLockFailsOnce = false;
                throw new IllegalStateException("transient hasLock failure");
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

        @Override
        public void shutdown() {
            shutDown.countDown();
        }
    }

    // A model of a user's own, which does not implement subscribePaused unless told to, holds a subscription made while
    // it is stopped paused, and starts each subscription at the end of its log. One that cannot pause ignores
    // pauseSubscription(..) and keeps its subscriptions running when it is stopped. It can also be told to keep them
    // running when it is stopped while pausing them when asked to, and to fail a cancel once after cancelling.
    private static final class UserWrittenModel implements SubscriptionModel, IntrospectableSubscriptions {
        private final boolean cannotPause;
        private volatile Consumer<String> beforeSubscribe = __ -> {};
        private volatile Consumer<String> afterSubscribe = __ -> {};
        private volatile boolean resumeFailsOnce;
        private volatile boolean implementsSubscribePaused;
        private volatile boolean pauseFailsOnce;
        private volatile boolean pauseFailsAfterPausing;
        private volatile boolean pauseKeepsFailing;
        private volatile boolean keepsSubscriptionsRunningWhenStopped;
        private volatile boolean cancelFailsAfterCancelling;
        private final List<String> cancelled = new CopyOnWriteArrayList<>();
        private boolean running = true;
        private final Set<String> runningIds = new HashSet<>();
        private final Set<String> pausedIds = new HashSet<>();
        private final List<String> log = new ArrayList<>();
        private final Map<String, Consumer<CloudEvent>> actions = new HashMap<>();
        private final Map<String, Integer> positions = new HashMap<>();

        private UserWrittenModel(boolean cannotPause) {
            this.cannotPause = cannotPause;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return make(subscriptionId, filter, action, false);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (!implementsSubscribePaused) {
                return SubscriptionModel.super.subscribePaused(subscriptionId, filter, startAt, action);
            }
            return make(subscriptionId, filter, action, true);
        }

        private Subscription make(String subscriptionId, @Nullable SubscriptionFilter filter, Consumer<CloudEvent> action, boolean paused) {
            beforeSubscribe.accept(subscriptionId);
            synchronized (this) {
                if (runningIds.contains(subscriptionId) || pausedIds.contains(subscriptionId)) {
                    throw new IllegalArgumentException("Subscription " + subscriptionId + " is already defined.");
                }
                (running && !paused ? runningIds : pausedIds).add(subscriptionId);
                actions.put(subscriptionId, action);
                positions.put(subscriptionId, log.size());
            }
            afterSubscribe.accept(subscriptionId);
            return new UserWrittenSubscription(subscriptionId);
        }

        private synchronized void write(String eventId) {
            log.add(eventId);
            List.copyOf(runningIds).forEach(this::deliver);
        }

        private void deliver(String subscriptionId) {
            for (int position = positions.get(subscriptionId); position < log.size(); position++) {
                actions.get(subscriptionId).accept(CloudEventBuilder.v1().withId(log.get(position)).withSource(URI.create("urn:user-written")).withType("written").build());
                positions.put(subscriptionId, position + 1);
            }
        }

        @Override
        public synchronized Set<String> subscriptionIds() {
            Set<String> ids = new HashSet<>(runningIds);
            ids.addAll(pausedIds);
            return ids;
        }

        @Override
        public synchronized void cancelSubscription(String subscriptionId) {
            cancelled.add(subscriptionId);
            runningIds.remove(subscriptionId);
            pausedIds.remove(subscriptionId);
            if (cancelFailsAfterCancelling) {
                cancelFailsAfterCancelling = false;
                throw new IllegalStateException("transient cancel failure");
            }
        }

        @Override
        public synchronized void stop() {
            running = false;
            if (!cannotPause && !keepsSubscriptionsRunningWhenStopped) {
                pausedIds.addAll(runningIds);
                runningIds.clear();
            }
        }

        @Override
        public synchronized void start(boolean resumeSubscriptionsAutomatically) {
            running = true;
            if (resumeSubscriptionsAutomatically) {
                runningIds.addAll(pausedIds);
                pausedIds.clear();
                List.copyOf(runningIds).forEach(this::deliver);
            }
        }

        @Override
        public synchronized boolean isRunning() {
            return running;
        }

        @Override
        public synchronized boolean isRunning(String subscriptionId) {
            return runningIds.contains(subscriptionId);
        }

        @Override
        public synchronized boolean isPaused(String subscriptionId) {
            return pausedIds.contains(subscriptionId);
        }

        @Override
        public synchronized Subscription resumeSubscription(String subscriptionId) {
            if (resumeFailsOnce) {
                resumeFailsOnce = false;
                throw new IllegalStateException("transient resume failure");
            }
            if (!pausedIds.remove(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
            }
            runningIds.add(subscriptionId);
            deliver(subscriptionId);
            return new UserWrittenSubscription(subscriptionId);
        }

        @Override
        public synchronized void pauseSubscription(String subscriptionId) {
            boolean fails = pauseFailsOnce || pauseKeepsFailing;
            pauseFailsOnce = false;
            if (fails && !pauseFailsAfterPausing) {
                throw new IllegalStateException("transient pause failure");
            }
            if (!cannotPause && runningIds.remove(subscriptionId)) {
                pausedIds.add(subscriptionId);
            }
            if (fails) {
                throw new IllegalStateException("transient pause failure");
            }
        }
    }

    private record UserWrittenSubscription(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
