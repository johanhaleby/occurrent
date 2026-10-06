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
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.retry.RetryStrategy;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * An event that waits for the lease is delivered once the lease is back, or once this model pauses, stops or starts
 * the subscription, and is never lost, also over an {@code InMemorySubscriptionModel} that does not retry an action
 * that throws, and also when the lease strategy throws while the event waits, which logs a warning, or logging anything
 * for a waiting event throws. Logging for a waiting event that clears the interrupt flag doesn't take an interrupt set
 * as the event came.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class CompetingConsumerLosesNoHeldEventTest {

    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    // Longer than the 200 ms an event waits at most between two looks at the lease
    private static final Duration AWAY_FOR = Duration.ofMillis(500);

    private final FenceStrategy strategy = new FenceStrategy();
    private final CountDownLatch inE1 = new CountDownLatch(1);
    private final CountDownLatch releaseE1 = new CountDownLatch(1);
    private final CountDownLatch releaseTheStrategysShutdown = new CountDownLatch(1);
    private final List<String> received = new CopyOnWriteArrayList<>();
    private @Nullable CompetingConsumerSubscriptionModel model;
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final Logger modelLog = (Logger) LoggerFactory.getLogger(CompetingConsumerSubscriptionModel.class);
    private final List<AppenderBase<ILoggingEvent>> addedAppenders = new ArrayList<>();

    @BeforeEach
    void captureTheLog() {
        logged.start();
        modelLog.addAppender(logged);
    }

    @AfterEach
    void shutdown() {
        releaseE1.countDown();
        releaseTheStrategysShutdown.countDown();
        if (model != null) {
            model.shutdown();
        }
        modelLog.detachAppender(logged);
        addedAppenders.forEach(modelLog::detachAppender);
        modelLog.setLevel(null);
    }

    @Test
    void an_event_waiting_for_the_lease_when_it_moves_to_another_node_and_back_is_delivered_by_a_model_that_does_not_retry() throws Exception {
        theLeaseMovesAwayAndBackWhileAnEventWaits(RetryStrategy.none());
    }

    @Test
    void an_event_waiting_for_the_lease_when_it_moves_to_another_node_and_back_is_delivered_by_a_model_that_retries_only_other_exceptions() throws Exception {
        theLeaseMovesAwayAndBackWhileAnEventWaits(RetryStrategy.fixed(200).retryIf(e -> !(e instanceof IllegalStateException)));
    }

    @Test
    void an_event_waiting_for_the_lease_while_this_model_is_stopped_is_delivered_once_it_starts_by_a_model_that_does_not_retry() throws Exception {
        InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(RetryStrategy.none());
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        model.stop();
        releaseE1.countDown();
        assertThat(strategy.askedWithoutTheLease.await(5, SECONDS)).as("e2 waits for the lease while this model is stopped").isTrue();
        model.start(true);
        await().atMost(EVENTUALLY).until(() -> model.isRunning("s1"));
        inMemory.accept(List.of(event("e3")));

        await().atMost(EVENTUALLY).untilAsserted(() -> assertThat(received).as("[events s1 received once this model was started again]").containsExactly("e1", "e2", "e3"));
    }

    @Test
    void an_event_for_which_the_lease_strategy_throws_an_error_waits_and_is_delivered_once_it_answers_by_a_model_that_does_not_retry() throws Exception {
        InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(RetryStrategy.none());
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        strategy.failureOnTheDeliveringThread = new Error("hasLock failed");
        releaseE1.countDown();
        assertThat(strategy.threw.await(5, SECONDS)).as("hasLock throws an Error for e2").isTrue();
        // Long enough for e2 to ask more than once
        await().pollDelay(AWAY_FOR).atMost(AWAY_FOR.multipliedBy(2)).dontCatchUncaughtExceptions().until(() -> true);
        assertThat(received).as("[events s1 received while hasLock throws an Error for e2]").containsExactly("e1");
        strategy.failureOnTheDeliveringThread = null;
        inMemory.accept(List.of(event("e3")));

        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().untilAsserted(() -> assertThat(received).as("[events s1 received after hasLock threw an Error for e2]").containsExactly("e1", "e2", "e3"));
    }

    @Test
    void an_event_for_which_the_lease_strategy_keeps_throwing_a_runtime_exception_logs_a_warning_while_it_waits_and_is_delivered_once() throws Exception {
        theLeaseStrategyKeepsThrowingWhileAnEventWaits(new IllegalStateException("hasLock failed"));
    }

    @Test
    void an_event_for_which_the_lease_strategy_keeps_throwing_an_error_logs_a_warning_while_it_waits_and_is_delivered_once() throws Exception {
        theLeaseStrategyKeepsThrowingWhileAnEventWaits(new Error("hasLock failed"));
    }

    // Seven failed looks take e2 past its second warning, and the warning of a look is logged before the next look.
    // Delivering e2 ends the wait too, so a hasLock that throws and lets e2 through fails on what s1 received. One whose
    // failure ends the wait, and the thread with it, fails on the looks counted.
    private void theLeaseStrategyKeepsThrowingWhileAnEventWaits(Throwable failure) throws Exception {
        InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(RetryStrategy.none());
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        strategy.failureOnTheDeliveringThread = failure;
        releaseE1.countDown();
        waitAtMostEventuallyUntil(() -> strategy.failedLooks.get() >= 7 || received.size() > 1);
        assertThat(received).as("[events s1 received while hasLock throws %s for e2]", failure).containsExactly("e1");
        assertThat(strategy.failedLooks.get()).as("[looks hasLock threw %s for while e2 waited]", failure).isGreaterThanOrEqualTo(7);
        assertThat(warnings()).as("[warnings logged while hasLock throws %s for e2]", failure).isNotEmpty();
        strategy.failureOnTheDeliveringThread = null;
        inMemory.accept(List.of(event("e3")));

        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().untilAsserted(() -> assertThat(received).as("[events s1 received after hasLock threw %s for e2]", failure).containsExactly("e1", "e2", "e3"));
        int failedLooks = strategy.failedLooks.get();
        assertThat(warnings()).as("[warnings for %s failed looks, one for the first and one for every fifth after it]", failedLooks).hasSize((failedLooks + 4) / 5)
                .allSatisfy(warning -> {
                    assertThat(warning.getFormattedMessage()).as("[what the warning names]").contains("subscriberId=node", "subscriptionId=s1");
                    assertThat(warning.getThrowableProxy()).as("[the failure logged with the warning]").isNotNull();
                    assertThat(warning.getThrowableProxy().getClassName()).isEqualTo(failure.getClass().getName());
                });
    }

    // A logging backend that runs out of memory writing the warning, as it can once hasLock threw an OutOfMemoryError.
    // Logback lets an Error from an appender through to the caller.
    @Test
    void an_event_whose_lease_warning_throws_an_error_while_logged_waits_and_is_delivered_once_the_lease_strategy_answers() throws Exception {
        CountDownLatch warningThrew = outOfMemoryOnTheFirst(Level.WARN, "whether this node holds the lease");
        InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(RetryStrategy.none());
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        strategy.failureOnTheDeliveringThread = new OutOfMemoryError("hasLock failed");
        releaseE1.countDown();
        assertThat(warningThrew.await(5, SECONDS)).as("logging the warning for e2 throws").isTrue();
        strategy.failureOnTheDeliveringThread = null;
        inMemory.accept(List.of(event("e3")));

        assertThat(receivedEventually(3)).as("[events s1 received after logging the warning for e2 threw]").containsExactly("e1", "e2", "e3");
    }

    // The first thing logged for an event that waits, right after a look that can be one hasLock threw an
    // OutOfMemoryError for
    @Test
    void an_event_whose_debug_message_that_it_waits_throws_an_error_while_logged_is_delivered_once_the_lease_strategy_answers() throws Exception {
        modelLog.setLevel(Level.DEBUG);
        CountDownLatch debugThrew = outOfMemoryOnTheFirst(Level.DEBUG, "Holding an event");
        InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(RetryStrategy.none());
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        strategy.failureOnTheDeliveringThread = new OutOfMemoryError("hasLock failed");
        releaseE1.countDown();
        assertThat(debugThrew.await(5, SECONDS)).as("logging that e2 waits throws").isTrue();
        strategy.failureOnTheDeliveringThread = null;
        inMemory.accept(List.of(event("e3")));

        assertThat(receivedEventually(3)).as("[events s1 received after logging that e2 waits threw]").containsExactly("e1", "e2", "e3");
    }

    @Test
    void an_event_whose_debug_message_that_it_goes_without_the_lease_throws_an_error_while_logged_is_delivered() throws Exception {
        modelLog.setLevel(Level.DEBUG);
        CountDownLatch debugThrew = outOfMemoryOnTheFirst(Level.DEBUG, "Delivering an event without the lease, since this model calls");
        BlockingIsRunning inMemory = new BlockingIsRunning();
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        CompletableFuture<Void> resume = inMemory.resumeBlockedInIsRunning(model);
        strategy.fenced = true;
        releaseE1.countDown();
        assertThat(debugThrew.await(5, SECONDS)).as("logging that e2 goes without the lease throws").isTrue();
        inMemory.releaseIsRunning.countDown();
        resume.handle((ignored, failure) -> null).get(5, SECONDS);
        strategy.fenced = false;
        inMemory.accept(List.of(event("e3")));

        assertThat(receivedEventually(3)).as("[events s1 received after logging that e2 goes without the lease threw]").containsExactly("e1", "e2", "e3");
    }

    @Test
    void an_event_let_through_on_the_look_the_lease_strategy_throws_for_logs_no_warning_that_it_waits() throws Exception {
        BlockingIsRunning inMemory = new BlockingIsRunning();
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        CompletableFuture<Void> resume = inMemory.resumeBlockedInIsRunning(model);
        strategy.failureOnTheDeliveringThread = new IllegalStateException("hasLock failed");
        releaseE1.countDown();
        await().atMost(EVENTUALLY).until(() -> received.size() == 2);
        List<ILoggingEvent> warnings = warnings();
        inMemory.releaseIsRunning.countDown();
        resume.handle((ignored, failure) -> null).get(5, SECONDS);

        assertThat(strategy.failedLooks.get()).as("hasLock throws for e2").isPositive();
        assertThat(warnings).filteredOn(warning -> warning.getFormattedMessage().contains("whether this node holds the lease")).as("[lease warnings for e2, let through on the look hasLock threw for]").isEmpty();
    }

    // shutdown() begins while hasLock is asked for e2, and that hasLock throws. shutdown() lets e2 through once the wait
    // after the look ends, which it does only after the lease strategy has shut down.
    @Test
    void an_event_that_shutdown_lets_through_after_a_look_the_lease_strategy_throws_for_logs_no_warning_that_it_waits() throws Exception {
        CountDownLatch inTheStrategysShutdown = new CountDownLatch(1);
        AtomicBoolean shutdownBegun = new AtomicBoolean();
        InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(RetryStrategy.none());
        CompetingConsumerStrategy shuttingDown = new CompetingConsumerStrategy() {
            @Override
            public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
                return strategy.registerCompetingConsumer(subscriptionId, subscriberId);
            }

            @Override
            public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
                strategy.unregisterCompetingConsumer(subscriptionId, subscriberId);
            }

            @Override
            public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
                strategy.releaseCompetingConsumer(subscriptionId, subscriberId);
            }

            @Override
            public boolean hasLock(String subscriptionId, String subscriberId) {
                if (strategy.failureOnTheDeliveringThread != null && Thread.currentThread() == strategy.deliveringThread && shutdownBegun.compareAndSet(false, true)) {
                    Thread.ofPlatform().start(() -> model.shutdown());
                    try {
                        inTheStrategysShutdown.await(5, SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return strategy.hasLock(subscriptionId, subscriberId);
            }

            @Override
            public void addListener(CompetingConsumerListener listener) {
                strategy.addListener(listener);
            }

            @Override
            public void removeListener(CompetingConsumerListener listener) {
                strategy.removeListener(listener);
            }

            @Override
            public void shutdown() {
                inTheStrategysShutdown.countDown();
                try {
                    releaseTheStrategysShutdown.await(5, SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        model = new CompetingConsumerSubscriptionModel(inMemory, shuttingDown);
        subscribeAndBlockInE1(inMemory);

        strategy.failureOnTheDeliveringThread = new IllegalStateException("hasLock failed");
        releaseE1.countDown();
        await().atMost(EVENTUALLY).until(() -> received.size() == 2);
        List<ILoggingEvent> warnings = warnings();
        releaseTheStrategysShutdown.countDown();

        assertThat(strategy.failedLooks.get()).as("hasLock throws for e2 once").isEqualTo(1);
        assertThat(warnings).filteredOn(warning -> warning.getFormattedMessage().contains("whether this node holds the lease")).as("[lease warnings for e2, which shutdown() let through after the look]").isEmpty();
    }

    @Test
    void an_interrupt_set_as_an_event_that_waits_comes_is_set_in_the_action_and_after_it_when_logging_that_the_event_goes_clears_the_flag() throws Exception {
        modelLog.setLevel(Level.DEBUG);
        clearTheInterruptFlagWhenLogging("since the thread was interrupted");

        InterruptFlags flags = anInterruptedThreadHandsOverAnEventWithoutTheLease();

        assertThat(received).as("[events s1 received]").containsExactly("e2");
        assertThat(flags.inTheAction()).as("[the interrupt flag in the action, set as e2 came]").containsExactly(true);
        assertThat(flags.afterTheAction()).as("[the interrupt flag once the action has returned]").isTrue();
    }

    @Test
    void an_interrupt_set_as_an_event_that_waits_comes_lets_it_through_when_logging_that_it_waits_clears_the_flag() throws Exception {
        modelLog.setLevel(Level.DEBUG);
        clearTheInterruptFlagWhenLogging("Holding an event");

        InterruptFlags flags = anInterruptedThreadHandsOverAnEventWithoutTheLease();

        assertThat(received).as("[events s1 received]").containsExactly("e2");
        assertThat(flags.inTheAction()).as("[the interrupt flag in the action, set as e2 came]").containsExactly(true);
        assertThat(flags.afterTheAction()).as("[the interrupt flag once the action has returned]").isTrue();
    }

    // As code that catches an InterruptedException without setting the flag again does
    private void clearTheInterruptFlagWhenLogging(String text) {
        AppenderBase<ILoggingEvent> clearing = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getFormattedMessage().contains(text)) {
                    Thread.interrupted();
                }
            }
        };
        clearing.start();
        modelLog.addAppender(clearing);
        addedAppenders.add(clearing);
    }

    private record InterruptFlags(List<Boolean> inTheAction, boolean afterTheAction) {
    }

    // A thread whose interrupt flag is set hands e2 to s1 while this node doesn't hold the lease, so the interrupt lets
    // e2 through. The lease comes back after AWAY_FOR, so an e2 that went on waiting reaches the action all the same,
    // with the flag clear.
    private InterruptFlags anInterruptedThreadHandsOverAnEventWithoutTheLease() throws Exception {
        HandingOutItsActions inMemory = new HandingOutItsActions();
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        List<Boolean> inTheAction = new CopyOnWriteArrayList<>();
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> {
            inTheAction.add(Thread.currentThread().isInterrupted());
            received.add(e.getId());
        }).waitUntilStarted();
        strategy.fenced = true;

        Consumer<CloudEvent> action = inMemory.actions.get("s1");
        CompletableFuture<Boolean> afterTheAction = new CompletableFuture<>();
        Thread handingOver = Thread.ofPlatform().start(() -> {
            Thread.currentThread().interrupt();
            action.accept(event("e2"));
            afterTheAction.complete(Thread.currentThread().isInterrupted());
        });
        handingOver.join(AWAY_FOR);
        strategy.fenced = false;
        return new InterruptFlags(inTheAction, afterTheAction.get(5, SECONDS));
    }

    // Keeps the action this model hands the wrapped model for each subscription, so a test can hand an event over on a
    // thread of its own
    private static final class HandingOutItsActions extends InMemorySubscriptionModel {
        private final Map<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();

        HandingOutItsActions() {
            super(RetryStrategy.none());
        }

        @Override
        public synchronized Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            actions.put(subscriptionId, action);
            return super.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public synchronized Subscription subscribePaused(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            actions.put(subscriptionId, action);
            return super.subscribePaused(subscriptionId, filter, startAt, action);
        }
    }

    // Throws an OutOfMemoryError from the appender for the first message at the level that contains the text, and
    // counts the latch down when it does
    private CountDownLatch outOfMemoryOnTheFirst(Level level, String text) {
        CountDownLatch threw = new CountDownLatch(1);
        AppenderBase<ILoggingEvent> outOfMemory = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getLevel() == level && event.getFormattedMessage().contains(text) && threw.getCount() > 0) {
                    threw.countDown();
                    throw new OutOfMemoryError("logging " + text);
                }
            }
        };
        outOfMemory.start();
        modelLog.addAppender(outOfMemory);
        addedAppenders.add(outOfMemory);
        return threw;
    }

    // An event lost on a thread of the wrapped model that dies of an Error shows in the events received, so this waits
    // for them without failing, and the assertion on what it returns fails instead
    private List<String> receivedEventually(int count) throws InterruptedException {
        waitAtMostEventuallyUntil(() -> received.size() >= count);
        return received;
    }

    // Returns once the condition holds or EVENTUALLY has passed, without failing, so the assertion after it fails instead
    private static void waitAtMostEventuallyUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + EVENTUALLY.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
    }

    // A resume of a running competing subscription first asks the wrapped model whether it runs it, which lets s1's
    // events through. The wrapped model stays in that call until releaseIsRunning is counted down.
    private static final class BlockingIsRunning extends InMemorySubscriptionModel {
        private final CountDownLatch inIsRunning = new CountDownLatch(1);
        private final CountDownLatch releaseIsRunning = new CountDownLatch(1);
        private final AtomicBoolean blockIsRunning = new AtomicBoolean();

        BlockingIsRunning() {
            super(RetryStrategy.none());
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            if (blockIsRunning.compareAndSet(true, false)) {
                inIsRunning.countDown();
                try {
                    releaseIsRunning.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.isRunning(subscriptionId);
        }

        CompletableFuture<Void> resumeBlockedInIsRunning(CompetingConsumerSubscriptionModel model) throws InterruptedException {
            blockIsRunning.set(true);
            CompletableFuture<Void> resume = CompletableFuture.runAsync(() -> model.resumeSubscription("s1"));
            assertThat(inIsRunning.await(5, SECONDS)).as("the resume asks whether the wrapped model runs s1").isTrue();
            return resume;
        }
    }

    private List<ILoggingEvent> warnings() {
        synchronized (logged) {
            return new ArrayList<>(logged.list).stream().filter(e -> e.getLevel() == Level.WARN).toList();
        }
    }

    // e1 runs while the lease closes without anyone being told, as the MongoDB lease strategies close it, so e2 waits for
    // it. The lease then goes to another node for a while and back.
    private void theLeaseMovesAwayAndBackWhileAnEventWaits(RetryStrategy retryStrategy) throws Exception {
        InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(retryStrategy);
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        strategy.fenced = true;
        releaseE1.countDown();
        assertThat(strategy.askedWithoutTheLease.await(5, SECONDS)).as("e2 waits for the lease").isTrue();
        strategy.transfer("s1", "node", "other-node");
        // An event lost on a thread of the wrapped model that dies of it shows in the events received, not as the exception
        await().pollDelay(AWAY_FOR).atMost(AWAY_FOR.multipliedBy(2)).dontCatchUncaughtExceptions().until(() -> true);
        strategy.fenced = false;
        strategy.transfer("s1", "other-node", "node");
        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().until(() -> inMemory.isRunning("s1"));
        inMemory.accept(List.of(event("e3")));

        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().untilAsserted(() -> assertThat(received).as("[events s1 received once the lease was back]").containsExactly("e1", "e2", "e3"));
    }

    private void subscribeAndBlockInE1(InMemorySubscriptionModel inMemory) throws InterruptedException {
        model.subscribe("node", "s1", null, StartAt.subscriptionModelDefault(), e -> {
            if (e.getId().equals("e1")) {
                strategy.deliveringThread = Thread.currentThread();
                inE1.countDown();
                try {
                    releaseE1.await();
                } catch (InterruptedException x) {
                    Thread.currentThread().interrupt();
                }
            }
            received.add(e.getId());
        }).waitUntilStarted();
        inMemory.accept(List.of(event("e1"), event("e2")));
        assertThat(inE1.await(5, SECONDS)).as("e1 runs").isTrue();
    }

    private static CloudEvent event(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:test")).withType("Tested").build();
    }

    // Reports the lease of a subscription held by the node it went to, unless fenced, which closes it without telling
    // anyone. A transfer tells this node, on the calling thread, of the loss and then of the grant. Records when the
    // thread that delivered e1 asks without the lease, which is e2 waiting for it. Throws failureOnTheDeliveringThread,
    // when set, to the thread that delivered e1, and counts each time it does.
    static final class FenceStrategy implements CompetingConsumerStrategy {
        private final Map<String, String> holders = new ConcurrentHashMap<>();
        private final List<CompetingConsumerListener> listeners = new CopyOnWriteArrayList<>();
        final CountDownLatch askedWithoutTheLease = new CountDownLatch(1);
        final CountDownLatch threw = new CountDownLatch(1);
        final AtomicInteger failedLooks = new AtomicInteger();
        volatile @Nullable Throwable failureOnTheDeliveringThread;
        volatile boolean fenced;
        volatile @Nullable Thread deliveringThread;

        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            String holder = holders.putIfAbsent(subscriptionId, subscriberId);
            return holder == null || holder.equals(subscriberId);
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId, subscriberId);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
            holders.remove(subscriptionId, subscriberId);
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            Throwable failure = failureOnTheDeliveringThread;
            if (failure != null && Thread.currentThread() == deliveringThread) {
                failedLooks.incrementAndGet();
                threw.countDown();
                if (failure instanceof Error error) {
                    throw error;
                }
                throw (RuntimeException) failure;
            }
            boolean held = !fenced && subscriberId.equals(holders.get(subscriptionId));
            if (!held && Thread.currentThread() == deliveringThread) {
                askedWithoutTheLease.countDown();
            }
            return held;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
            listeners.add(listenerConsumer);
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
            listeners.remove(listenerConsumer);
        }

        // A listener that throws is skipped, as the MongoDB lease strategies log it and tell the next one
        void transfer(String subscriptionId, String from, String to) {
            holders.put(subscriptionId, to);
            listeners.forEach(listener -> told(() -> listener.onConsumeProhibited(subscriptionId, from)));
            listeners.forEach(listener -> told(() -> listener.onConsumeGranted(subscriptionId, to)));
        }

        private static void told(Runnable callback) {
            try {
                callback.run();
            } catch (RuntimeException e) {
                LoggerFactory.getLogger(FenceStrategy.class).warn("A listener of the lease strategy threw", e);
            }
        }
    }
}
