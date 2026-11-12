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
import org.junit.jupiter.api.Timeout;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * A wrapped model or a lease strategy written in Kotlin can throw a checked exception without declaring it. What this
 * model does after such a failure is the same as after a {@link RuntimeException}. A start(..) still gives every
 * subscription its turn and then throws the failure as it was thrown, a stop() still has a consumer it could not pause
 * tried again, and a subscribe still gives up its registration before the wrapped model has made the subscription, and
 * records it once it has.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class CompetingConsumerUndeclaredCheckedExceptionTest {

    private static final String SUBSCRIBER = "node";
    private static final Duration EVENTUALLY = Duration.ofSeconds(10);
    // Resolves to null for this model, which hands the subscription straight to the wrapped model
    private static final StartAt DOES_NOT_COMPETE = StartAt.dynamic(context -> context.subscriptionModelType() == CompetingConsumerSubscriptionModel.class ? null : StartAt.now());

    private final Strategy strategy = new Strategy();
    private @Nullable CompetingConsumerSubscriptionModel model;

    @AfterEach
    void shutdownTheModel() {
        if (model != null) {
            model.shutdown();
        }
    }

    @Test
    void a_start_whose_wrapped_model_throws_a_checked_exception_on_starting_still_resumes_a_subscription_that_does_not_compete_and_throws_it() {
        // Given
        Wrapped wrapped = new Wrapped(false);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        model.subscribe(SUBSCRIBER, "nc", null, DOES_NOT_COMPETE, __ -> {
        });
        IOException cannotStart = new IOException("The wrapped model cannot start right now");
        wrapped.failures.next("start", cannotStart);

        // When
        Throwable thrown = catchThrowable(() -> model.start(true));

        // Then
        assertThat(wrapped.running).as("the subscriptions the wrapped model runs once start(true) threw").containsExactly("nc");
        assertThat(thrown).as("what start(true) threw").isSameAs(cannotStart);
    }

    @Test
    void a_start_whose_wrapped_model_throws_a_checked_exception_again_as_a_subscription_that_does_not_compete_gets_its_turn_still_resumes_it() {
        // Given
        Wrapped wrapped = new Wrapped(false);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        model.subscribe(SUBSCRIBER, "nc", null, DOES_NOT_COMPETE, __ -> {
        });
        IOException cannotStart = new IOException("The wrapped model cannot start right now");
        IOException cannotStartAgain = new IOException("The wrapped model still cannot start");
        wrapped.failures.next("start", cannotStart);
        wrapped.failures.next("start", cannotStartAgain);

        // When
        Throwable thrown = catchThrowable(() -> model.start(true));

        // Then
        assertThat(wrapped.running).as("the subscriptions the wrapped model runs once start(true) threw").containsExactly("nc");
        assertThat(thrown).as("what start(true) threw").isSameAs(cannotStart);
    }

    @Test
    void a_start_whose_wrapped_model_throws_a_checked_exception_on_resuming_a_subscription_that_does_not_compete_throws_it_and_leaves_the_model_not_running() {
        // Given
        Wrapped wrapped = new Wrapped(true);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        model.subscribe(SUBSCRIBER, "nc", null, DOES_NOT_COMPETE, __ -> {
        });
        model.pauseSubscription("nc");
        IOException cannotResume = new IOException("The wrapped model cannot resume nc right now");
        wrapped.failures.next("resume nc", cannotResume);

        // When
        Throwable thrown = catchThrowable(() -> model.start(true));

        // Then
        assertThat(thrown).as("what start(true) threw").isSameAs(cannotResume);
        assertThat(model.isRunning()).as("the model is running once start(true) threw").isFalse();
    }

    @Test
    void a_stop_whose_wrapped_model_throws_a_checked_exception_on_pausing_a_competing_subscription_has_it_paused_and_its_lease_given_up_by_its_try() {
        // Given
        Wrapped wrapped = new Wrapped(true);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        model.subscribe(SUBSCRIBER, "cc", null, StartAt.now(), __ -> {
        });
        assertThat(wrapped.running).as("the subscriptions the wrapped model runs once cc was made").containsExactly("cc");
        IOException cannotPause = new IOException("The wrapped model cannot pause cc right now");
        wrapped.failures.next("pause cc", cannotPause);

        // When
        Throwable thrown = catchThrowable(model::stop);

        // Then
        await().atMost(EVENTUALLY).untilAsserted(() -> {
            assertThat(wrapped.running).as("the subscriptions the wrapped model runs after stop() threw").isEmpty();
            assertThat(strategy.holders).as("the leases this node holds after stop() threw").isEmpty();
        });
        assertThat(thrown).as("what stop() threw").isSameAs(cannotPause);
    }

    @Test
    void a_subscribe_whose_lease_strategy_throws_a_checked_exception_before_the_wrapped_model_made_the_subscription_gives_up_its_registration_and_throws_it() {
        // Given
        Wrapped wrapped = new Wrapped(true);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        IOException unreachable = new IOException("The lease store cannot be reached right now");
        strategy.failures.next("hasLock cc", unreachable);

        // When
        Throwable thrown = catchThrowable(() -> model.subscribe(SUBSCRIBER, "cc", null, StartAt.now(), __ -> {
        }));

        // Then
        assertThat(strategy.calls).as("the calls to the lease strategy").containsExactly("register cc", "unregister cc");
        assertThat(thrown).as("what subscribe threw").isSameAs(unreachable);
    }

    @Test
    void a_subscribe_whose_wrapped_model_throws_a_checked_exception_once_it_made_the_subscription_returns_and_gives_the_lease_back() {
        // Given
        Wrapped wrapped = new Wrapped(true);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        wrapped.onceMade = () -> wrapped.failures.next("isPaused cc", new IOException("The wrapped model cannot say whether it holds cc paused right now"));

        // When
        Throwable thrown = catchThrowable(() -> model.subscribe(SUBSCRIBER, "cc", null, StartAt.now(), __ -> {
        }));

        // Then
        assertThat(thrown).as("what subscribe threw").isNull();
        assertThat(strategy.calls).as("the calls to the lease strategy").containsExactly("register cc", "release cc");
    }

    @Test
    void a_subscribe_that_lost_its_lease_and_whose_wrapped_model_throws_a_checked_exception_on_being_asked_whether_it_runs_the_subscription_returns() {
        // Given
        Wrapped wrapped = new Wrapped(true);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        wrapped.onceMade = () -> {
            strategy.holders.remove("cc");
            wrapped.failures.next("isRunning cc", new IOException("The wrapped model cannot say whether it runs cc right now"));
        };

        // When
        Throwable thrown = catchThrowable(() -> model.subscribe(SUBSCRIBER, "cc", null, StartAt.now(), __ -> {
        }));

        // Then
        assertThat(thrown).as("what subscribe threw").isNull();
        assertThat(wrapped.paused).as("the subscriptions the wrapped model holds paused").containsExactly("cc");
    }

    @Test
    void a_subscribe_that_lost_its_lease_and_whose_wrapped_model_throws_a_checked_exception_each_time_it_is_asked_whether_it_runs_the_subscription_returns() {
        // Given
        Wrapped wrapped = new Wrapped(true);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        wrapped.onceMade = () -> {
            strategy.holders.remove("cc");
            wrapped.failures.next("isRunning cc", new IOException("The wrapped model cannot say whether it runs cc right now"));
            wrapped.failures.next("isRunning cc", new IOException("The wrapped model still cannot say whether it runs cc"));
        };

        // When
        Throwable thrown = catchThrowable(() -> model.subscribe(SUBSCRIBER, "cc", null, StartAt.now(), __ -> {
        }));

        // Then
        assertThat(thrown).as("what subscribe threw").isNull();
        assertThat(wrapped.paused).as("the subscriptions the wrapped model holds paused").containsExactly("cc");
    }

    @Test
    void a_subscribe_that_a_stop_overtook_returns_when_the_wrapped_model_that_started_itself_to_make_it_throws_a_checked_exception_on_being_asked_whether_it_runs() throws Exception {
        // Given
        Wrapped wrapped = new Wrapped(true);
        wrapped.startsItselfOnSubscribe = true;
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        Gate subscribeInTheWrappedModel = new Gate();
        wrapped.nextSubscribe = subscribeInTheWrappedModel;
        wrapped.onceMade = () -> wrapped.failures.nextOnThisThread("isRunning", new IOException("The wrapped model cannot say whether it runs right now"));
        CompletableFuture<@Nullable Throwable> subscribing = CompletableFuture.supplyAsync(() -> catchThrowable(() -> model.subscribe(SUBSCRIBER, "cc", null, StartAt.now(), __ -> {
        })));
        assertThat(subscribeInTheWrappedModel.awaitEntered()).as("the wrapped model is making cc").isTrue();

        // When
        model.stop();
        subscribeInTheWrappedModel.open();
        @Nullable Throwable thrown = subscribing.get(EVENTUALLY.toMillis(), TimeUnit.MILLISECONDS);

        // Then
        assertThat(thrown).as("what subscribe threw").isNull();
        assertThat(model.isPaused("cc")).as("cc is paused once the subscribe returned").isTrue();
    }

    @Test
    void a_start_handed_over_for_a_subscription_that_does_not_compete_is_tried_again_after_its_wrapped_model_throws_an_InterruptedException() throws Exception {
        // Given
        Wrapped wrapped = new Wrapped(false);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        model.subscribe(SUBSCRIBER, "nc", null, DOES_NOT_COMPETE, __ -> {
        });
        Gate isPausedInTheWrappedModel = new Gate();
        wrapped.nextIsPaused = isPausedInTheWrappedModel;
        CompletableFuture<@Nullable Throwable> pausing = CompletableFuture.supplyAsync(() -> catchThrowable(() -> model.pauseSubscription("nc")));
        assertThat(isPausedInTheWrappedModel.awaitEntered()).as("the pause holds the lock of nc").isTrue();
        wrapped.failures.next("start", new IOException("The wrapped model cannot start right now"));
        wrapped.failures.next("start", new InterruptedException("The wrapped model was interrupted while starting"));

        // When
        Throwable thrown = catchThrowable(() -> model.start(true));
        isPausedInTheWrappedModel.open();
        pausing.get(EVENTUALLY.toMillis(), TimeUnit.MILLISECONDS);

        // Then
        assertThat(thrown).as("what start(true) threw").isInstanceOf(IOException.class);
        await().atMost(EVENTUALLY).untilAsserted(() ->
                assertThat(wrapped.started).as("the wrapped model is started once the thread the start(true) was handed to tried again").isTrue());
    }

    @Test
    void a_cancel_whose_wrapped_model_throws_a_checked_exception_after_it_took_effect_and_whose_lease_strategy_throws_one_on_unregistering_throws_the_first_with_the_second_suppressed() {
        // Given
        Wrapped wrapped = new Wrapped(true);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        model.subscribe(SUBSCRIBER, "cc", null, StartAt.now(), __ -> {
        });
        IOException cannotCancel = new IOException("The wrapped model cannot cancel cc right now");
        IOException cannotUnregister = new IOException("The lease store cannot be reached right now");
        wrapped.failures.next("cancelled cc", cannotCancel);
        strategy.failures.next("unregister cc", cannotUnregister);

        // When
        Throwable thrown = catchThrowable(() -> model.cancelSubscription("cc"));

        // Then
        assertThat(thrown).as("what cancelSubscription threw").isSameAs(cannotCancel);
        assertThat(thrown.getSuppressed()).as("what was suppressed in it").containsExactly(cannotUnregister);
    }

    @Test
    void a_start_true_for_a_subscription_that_does_not_compete_whose_resume_throws_a_checked_exception_is_not_resumed_again_by_the_thread_an_earlier_start_was_handed_to() throws Exception {
        // Given
        Wrapped wrapped = new Wrapped(false);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        model.subscribe(SUBSCRIBER, "nc", null, DOES_NOT_COMPETE, __ -> {
        });
        Gate isPausedInTheWrappedModel = new Gate();
        wrapped.nextIsPaused = isPausedInTheWrappedModel;
        CompletableFuture<@Nullable Throwable> pausing = CompletableFuture.supplyAsync(() -> catchThrowable(() -> model.pauseSubscription("nc")));
        assertThat(isPausedInTheWrappedModel.awaitEntered()).as("the pause holds the lock of nc").isTrue();
        Gate handedOverThreadFailed = new Gate();
        model.runBeforeAHandedOverThreadBacksOff(handedOverThreadFailed::pass);
        wrapped.failures.next("start", new IOException("The wrapped model cannot start right now"));
        wrapped.failures.next("start", new IOException("The wrapped model still cannot start"));
        catchThrowable(() -> model.start(false));
        isPausedInTheWrappedModel.open();
        pausing.get(EVENTUALLY.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(handedOverThreadFailed.awaitEntered()).as("the thread the first start was handed to failed to apply it").isTrue();
        IOException cannotResume = new IOException("The wrapped model cannot resume nc right now");
        wrapped.failures.next("resume nc", cannotResume);

        // When
        Throwable thrown = catchThrowable(() -> model.start(true));
        handedOverThreadFailed.open();
        await().atMost(EVENTUALLY).until(() -> !aThreadAppliesWhatWasHandedOverFor("nc"));

        // Then
        assertThat(thrown).as("what start(true) threw").isSameAs(cannotResume);
        assertThat(wrapped.running).as("the subscriptions the wrapped model runs once the thread the first start was handed to is done").isEmpty();
    }

    @Test
    void a_stop_whose_pause_of_a_subscription_the_wrapped_model_throws_a_checked_exception_for_applied_by_a_later_pause_before_it_stopped_the_wrapped_model_returns_and_gives_the_lease_up() throws Exception {
        // Given
        Wrapped wrapped = new Wrapped(true);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        model.subscribe(SUBSCRIBER, "cc", null, StartAt.now(), __ -> {
        });
        model.subscribe(SUBSCRIBER, "nc", null, DOES_NOT_COMPETE, __ -> {
        });
        model.pauseSubscription("nc");
        Gate resumeInTheWrappedModel = new Gate();
        wrapped.nextResume = resumeInTheWrappedModel;
        CompletableFuture<@Nullable Throwable> resuming = CompletableFuture.supplyAsync(() -> catchThrowable(() -> model.resumeSubscription("nc")));
        assertThat(resumeInTheWrappedModel.awaitEntered()).as("the resume of nc is under way in the wrapped model").isTrue();
        CompletableFuture<@Nullable Throwable> stopping = new CompletableFuture<>();
        Thread stoppingThread = new Thread(() -> stopping.complete(catchThrowable(model::stop)), "test-stopping-thread");
        stoppingThread.setDaemon(true);
        stoppingThread.start();
        await().atMost(EVENTUALLY).until(() -> stopping.isDone() || waitsOnAMonitor(stoppingThread));
        assertThat(stopping).as("stop() waits for the resume of nc").isNotDone();
        wrapped.failures.next("pause cc", new IOException("The wrapped model cannot pause cc right now"));

        // When
        model.pauseSubscription("cc");
        resumeInTheWrappedModel.open();
        resuming.get(EVENTUALLY.toMillis(), TimeUnit.MILLISECONDS);
        @Nullable Throwable thrown = stopping.get(EVENTUALLY.toMillis(), TimeUnit.MILLISECONDS);

        // Then
        assertThat(thrown).as("what stop() threw").isNull();
        assertThat(strategy.holders).as("the leases this node holds once stop() returned").isEmpty();
    }

    @Test
    void a_resume_that_a_stop_did_not_wait_for_and_whose_pause_afterwards_throws_a_checked_exception_after_it_took_effect_gives_the_lease_up_only_by_unregistering() throws Exception {
        // Given
        Wrapped wrapped = new Wrapped(true);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        model.subscribe(SUBSCRIBER, "cc", null, StartAt.now(), __ -> {
        });
        model.pauseSubscription("cc");
        Gate resumeInTheWrappedModel = new Gate();
        wrapped.nextResume = resumeInTheWrappedModel;
        wrapped.failures.next("paused cc", new IOException("The wrapped model cannot pause cc right now"));
        CompletableFuture<@Nullable Throwable> resuming = CompletableFuture.supplyAsync(() -> catchThrowable(() -> model.resumeSubscription("cc")));
        assertThat(resumeInTheWrappedModel.awaitEntered()).as("the resume of cc is under way in the wrapped model").isTrue();
        model.stop();

        // When
        resumeInTheWrappedModel.open();
        @Nullable Throwable thrown = resuming.get(EVENTUALLY.toMillis(), TimeUnit.MILLISECONDS);

        // Then
        assertThat(thrown).as("what resumeSubscription threw").isNull();
        // cc gives its lease up once the stop() is applied to it, which can be after the resume returned
        await().atMost(EVENTUALLY).untilAsserted(() ->
                assertThat(strategy.calls).as("the calls to the lease strategy").containsExactly("register cc", "unregister cc", "register cc", "unregister cc"));
    }

    @Test
    void a_resume_of_a_subscription_that_does_not_compete_resumes_it_when_the_wrapped_model_throws_a_checked_exception_on_being_asked_whether_it_runs() {
        // Given
        Wrapped wrapped = new Wrapped(true);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        model.subscribe(SUBSCRIBER, "nc", null, DOES_NOT_COMPETE, __ -> {
        });
        model.pauseSubscription("nc");
        wrapped.failures.next("isRunning", new IOException("The wrapped model cannot say whether it runs right now"));

        // When
        Throwable thrown = catchThrowable(() -> model.resumeSubscription("nc"));

        // Then
        assertThat(thrown).as("what resumeSubscription threw").isNull();
        assertThat(wrapped.running).as("the subscriptions the wrapped model runs").containsExactly("nc");
    }

    @Test
    void a_subscribe_whose_wrapped_model_throws_a_checked_exception_after_pausing_the_subscription_gives_the_lease_back() {
        // Given
        Wrapped wrapped = new Wrapped(true);
        wrapped.startsItselfOnSubscribe = true;
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        wrapped.onceMade = () -> {
            wrapped.failures.next("isPaused cc", new IOException("The wrapped model cannot say whether it holds cc paused right now"));
            wrapped.failures.next("paused cc", new IOException("The wrapped model cannot pause cc right now"));
        };

        // When
        Throwable thrown = catchThrowable(() -> model.subscribe(SUBSCRIBER, "cc", null, StartAt.now(), __ -> {
        }));

        // Then
        assertThat(thrown).as("what subscribe threw").isNull();
        assertThat(strategy.calls).as("the calls to the lease strategy").containsExactly("register cc", "release cc");
        assertThat(wrapped.running).as("the subscriptions the wrapped model runs").isEmpty();
    }

    @Test
    void a_start_runs_a_subscription_waiting_for_its_lease_when_the_wrapped_model_throws_a_checked_exception_on_being_asked_whether_it_runs_it_before_the_start() throws Exception {
        // Given
        Wrapped wrapped = new Wrapped(true);
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        wrapped.onceMade = () -> strategy.holders.remove("cc");
        model.subscribe(SUBSCRIBER, "cc", null, StartAt.now(), __ -> {
        });
        wrapped.failures.next("isRunning cc", new IOException("The wrapped model cannot say whether it runs cc right now"));

        // When
        Throwable thrown = catchThrowable(() -> model.start(true));

        // Then
        assertThat(thrown).as("what start(true) threw").isNull();
        assertThat(strategy.calls).as("the calls to the lease strategy").containsExactly("register cc", "register cc");
        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(wrapped.running).as("the subscriptions the wrapped model runs").containsExactly("cc"));
    }

    @Test
    void a_stop_applied_by_a_later_cancel_to_a_subscribe_under_way_throws_the_checked_exception_the_wrapped_model_threw_on_being_asked_whether_it_runs_the_subscription() throws Exception {
        // Given
        Wrapped wrapped = new Wrapped(true);
        wrapped.startsItselfOnSubscribe = true;
        CompetingConsumerSubscriptionModel model = modelOver(wrapped);
        model.subscribe(SUBSCRIBER, "nc", null, DOES_NOT_COMPETE, __ -> {
        });
        model.pauseSubscription("nc");
        Gate subscribeInTheWrappedModel = new Gate();
        wrapped.nextSubscribe = subscribeInTheWrappedModel;
        CompletableFuture<@Nullable Throwable> subscribing = CompletableFuture.supplyAsync(() -> catchThrowable(() -> model.subscribe(SUBSCRIBER, "cc", null, StartAt.now(), __ -> {
        })));
        assertThat(subscribeInTheWrappedModel.awaitEntered()).as("the wrapped model is making cc").isTrue();
        Gate resumeInTheWrappedModel = new Gate();
        wrapped.nextResume = resumeInTheWrappedModel;
        CompletableFuture<@Nullable Throwable> resuming = CompletableFuture.supplyAsync(() -> catchThrowable(() -> model.resumeSubscription("nc")));
        assertThat(resumeInTheWrappedModel.awaitEntered()).as("the resume of nc is under way in the wrapped model").isTrue();
        CompletableFuture<@Nullable Throwable> stopping = new CompletableFuture<>();
        Thread stoppingThread = new Thread(() -> stopping.complete(catchThrowable(model::stop)), "test-stopping-thread");
        stoppingThread.setDaemon(true);
        stoppingThread.start();
        await().atMost(EVENTUALLY).until(() -> stopping.isDone() || waitsOnAMonitor(stoppingThread));
        assertThat(stopping).as("stop() waits for the resume of nc").isNotDone();
        IOException cannotSay = new IOException("The wrapped model cannot say whether it runs cc right now");
        wrapped.failures.next("isRunning cc", cannotSay);

        // When
        model.cancelSubscription("cc");
        resumeInTheWrappedModel.open();
        subscribeInTheWrappedModel.open();
        resuming.get(EVENTUALLY.toMillis(), TimeUnit.MILLISECONDS);
        subscribing.get(EVENTUALLY.toMillis(), TimeUnit.MILLISECONDS);
        @Nullable Throwable thrown = stopping.get(EVENTUALLY.toMillis(), TimeUnit.MILLISECONDS);

        // Then
        assertThat(thrown).as("what stop() threw").isSameAs(cannotSay);
    }

    private static boolean aThreadAppliesWhatWasHandedOverFor(String subscriptionId) {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.isAlive() && thread.getName().equals("occurrent-competing-consumer-lifecycle-" + subscriptionId));
    }

    private static boolean waitsOnAMonitor(Thread thread) {
        return thread.getState() == Thread.State.WAITING && Stream.of(thread.getStackTrace())
                .anyMatch(frame -> frame.getClassName().equals(Object.class.getName()) && frame.getMethodName().startsWith("wait"));
    }

    private CompetingConsumerSubscriptionModel modelOver(Wrapped wrapped) {
        model = new CompetingConsumerSubscriptionModel(wrapped, strategy);
        return model;
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> RuntimeException sneaky(Throwable t) throws E {
        throw (E) t;
    }

    // The failures armed for each call, each thrown once without being declared, as Kotlin code throws a checked exception
    private static final class Failures {
        private final Map<String, Queue<Throwable>> armed = new ConcurrentHashMap<>();

        private void next(String call, Throwable failure) {
            armed.computeIfAbsent(call, __ -> new ConcurrentLinkedQueue<>()).add(failure);
        }

        private void nextOnThisThread(String call, Throwable failure) {
            next(onThisThread(call), failure);
        }

        private void throwIfArmed(String call) {
            @Nullable Throwable failure = take(onThisThread(call));
            if (failure == null) {
                failure = take(call);
            }
            if (failure != null) {
                throw sneaky(failure);
            }
        }

        private @Nullable Throwable take(String call) {
            @Nullable Queue<Throwable> failures = armed.get(call);
            return failures == null ? null : failures.poll();
        }

        private static String onThisThread(String call) {
            return call + " on thread " + Thread.currentThread().threadId();
        }
    }

    // Grants every lease on register, and tells no listener, so a lease given back stays free
    private static final class Strategy implements CompetingConsumerStrategy {
        private final Failures failures = new Failures();
        private final List<String> calls = new CopyOnWriteArrayList<>();
        private final Set<String> holders = ConcurrentHashMap.newKeySet();

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("register " + subscriptionId);
            failures.throwIfArmed("register " + subscriptionId);
            holders.add(subscriptionId);
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("unregister " + subscriptionId);
            holders.remove(subscriptionId);
            failures.throwIfArmed("unregister " + subscriptionId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            calls.add("release " + subscriptionId);
            holders.remove(subscriptionId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            failures.throwIfArmed("hasLock " + subscriptionId);
            return holders.contains(subscriptionId);
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }
    }

    // A model of a user's own that keeps what it runs running across its own stop(), and holds a subscription made while
    // it is stopped, or through subscribePaused, paused. One that starts itself on a subscribe refuses subscribePaused.
    // The next subscribe waits at nextSubscribe once it is set, and the next isPaused at nextIsPaused, and onceMade runs
    // once after each subscribe. The next resume waits at nextResume. A call armed as "cancelled" or "paused" throws
    // once it has taken effect.
    private static final class Wrapped implements SubscriptionModel {
        private final Failures failures = new Failures();
        private final Set<String> running = ConcurrentHashMap.newKeySet();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();
        private volatile boolean started;
        private volatile boolean startsItselfOnSubscribe;
        private volatile @Nullable Gate nextSubscribe;
        private volatile @Nullable Gate nextIsPaused;
        private volatile @Nullable Gate nextResume;
        private volatile Runnable onceMade = () -> {
        };

        private Wrapped(boolean started) {
            this.started = started;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            @Nullable Gate gate = nextSubscribe;
            nextSubscribe = null;
            if (gate != null) {
                gate.pass();
            }
            failures.throwIfArmed("subscribe " + subscriptionId);
            if (startsItselfOnSubscribe) {
                started = true;
            }
            (started ? running : paused).add(subscriptionId);
            made();
            return new Made(subscriptionId);
        }

        @Override
        public Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            if (startsItselfOnSubscribe) {
                throw new UnsupportedOperationException("subscribePaused");
            }
            failures.throwIfArmed("subscribePaused " + subscriptionId);
            paused.add(subscriptionId);
            made();
            return new Made(subscriptionId);
        }

        private void made() {
            Runnable once = onceMade;
            onceMade = () -> {
            };
            once.run();
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            running.remove(subscriptionId);
            paused.remove(subscriptionId);
            failures.throwIfArmed("cancelled " + subscriptionId);
        }

        @Override
        public void stop() {
            failures.throwIfArmed("stop");
            started = false;
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            failures.throwIfArmed("start");
            started = true;
        }

        @Override
        public boolean isRunning() {
            failures.throwIfArmed("isRunning");
            return started;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            failures.throwIfArmed("isRunning " + subscriptionId);
            return running.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            @Nullable Gate gate = nextIsPaused;
            nextIsPaused = null;
            if (gate != null) {
                gate.pass();
            }
            failures.throwIfArmed("isPaused " + subscriptionId);
            return paused.contains(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            @Nullable Gate gate = nextResume;
            nextResume = null;
            if (gate != null) {
                gate.pass();
            }
            failures.throwIfArmed("resume " + subscriptionId);
            paused.remove(subscriptionId);
            running.add(subscriptionId);
            return new Made(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            failures.throwIfArmed("pause " + subscriptionId);
            if (running.remove(subscriptionId)) {
                paused.add(subscriptionId);
            }
            failures.throwIfArmed("paused " + subscriptionId);
        }
    }

    private record Made(String id) implements Subscription {
        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }

    // A point that a thread waits at until the test opens it, which tells the test that a thread got there
    private static final class Gate {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch open = new CountDownLatch(1);

        private void pass() {
            entered.countDown();
            try {
                open.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private boolean awaitEntered() throws InterruptedException {
            return entered.await(EVENTUALLY.toMillis(), TimeUnit.MILLISECONDS);
        }

        private void open() {
            open.countDown();
        }
    }
}
