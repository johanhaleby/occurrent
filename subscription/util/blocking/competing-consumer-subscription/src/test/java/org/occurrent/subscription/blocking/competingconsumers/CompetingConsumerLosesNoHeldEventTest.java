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
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * An event that waits for the lease is delivered once the lease is back, or once this model pauses, stops or starts
 * the subscription, and is never lost, also over an {@code InMemorySubscriptionModel} that does not retry an action
 * that throws, and also when the lease strategy throws while the event waits, which logs a warning, or logging that
 * warning throws.
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
        if (model != null) {
            model.shutdown();
        }
        modelLog.detachAppender(logged);
        addedAppenders.forEach(modelLog::detachAppender);
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
    // Delivering e2 ends the wait too, so a hasLock that throws and lets e2 through fails on what s1 received.
    private void theLeaseStrategyKeepsThrowingWhileAnEventWaits(Throwable failure) throws Exception {
        InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(RetryStrategy.none());
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        strategy.failureOnTheDeliveringThread = failure;
        releaseE1.countDown();
        await().atMost(EVENTUALLY).dontCatchUncaughtExceptions().until(() -> strategy.failedLooks.get() >= 7 || received.size() > 1);
        assertThat(warnings()).as("[warnings logged while hasLock throws %s for e2]", failure).isNotEmpty();
        assertThat(received).as("[events s1 received while hasLock throws %s for e2]", failure).containsExactly("e1");
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
        CountDownLatch warningThrew = new CountDownLatch(1);
        AppenderBase<ILoggingEvent> outOfMemory = new AppenderBase<>() {
            @Override
            protected void append(ILoggingEvent event) {
                if (event.getLevel() == Level.WARN && event.getFormattedMessage().contains("holds its lease") && warningThrew.getCount() > 0) {
                    warningThrew.countDown();
                    throw new OutOfMemoryError("logging the warning");
                }
            }
        };
        outOfMemory.start();
        modelLog.addAppender(outOfMemory);
        addedAppenders.add(outOfMemory);
        InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(RetryStrategy.none());
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        strategy.failureOnTheDeliveringThread = new OutOfMemoryError("hasLock failed");
        releaseE1.countDown();
        assertThat(warningThrew.await(5, SECONDS)).as("logging the warning for e2 throws").isTrue();
        strategy.failureOnTheDeliveringThread = null;
        inMemory.accept(List.of(event("e3")));

        // An event lost on a thread of the wrapped model that dies of the Error shows in the events received
        long deadline = System.nanoTime() + EVENTUALLY.toNanos();
        while (received.size() < 3 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(received).as("[events s1 received after logging the warning for e2 threw]").containsExactly("e1", "e2", "e3");
    }

    // A resume of a running competing subscription first asks the wrapped model whether it runs it, which lets s1's
    // events through. While that call is under way, e2 goes on the look where hasLock throws.
    @Test
    void an_event_let_through_on_the_look_the_lease_strategy_throws_for_logs_no_warning_that_it_waits() throws Exception {
        CountDownLatch inIsRunning = new CountDownLatch(1);
        CountDownLatch releaseIsRunning = new CountDownLatch(1);
        AtomicBoolean blockIsRunning = new AtomicBoolean();
        InMemorySubscriptionModel inMemory = new InMemorySubscriptionModel(RetryStrategy.none()) {
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
        };
        model = new CompetingConsumerSubscriptionModel(inMemory, strategy);
        subscribeAndBlockInE1(inMemory);

        blockIsRunning.set(true);
        CompletableFuture<Void> resume = CompletableFuture.runAsync(() -> model.resumeSubscription("s1"));
        assertThat(inIsRunning.await(5, SECONDS)).as("the resume asks whether the wrapped model runs s1").isTrue();
        strategy.failureOnTheDeliveringThread = new IllegalStateException("hasLock failed");
        releaseE1.countDown();
        await().atMost(EVENTUALLY).until(() -> received.size() == 2);
        List<ILoggingEvent> warnings = warnings();
        releaseIsRunning.countDown();
        resume.handle((ignored, failure) -> null).get(5, SECONDS);

        assertThat(strategy.failedLooks.get()).as("hasLock throws for e2").isPositive();
        assertThat(warnings).filteredOn(warning -> warning.getFormattedMessage().contains("holds its lease")).as("[lease warnings for e2, let through on the look hasLock threw for]").isEmpty();
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
