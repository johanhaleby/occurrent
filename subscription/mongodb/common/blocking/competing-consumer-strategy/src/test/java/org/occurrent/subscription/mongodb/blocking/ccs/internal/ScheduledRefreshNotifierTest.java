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

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * close() does not wait for the notifier, since a listener it is calling may wait for the database. A listener that
 * also ignores the interrupt close() sends goes on running after it, on a thread that must not keep the JVM alive.
 * However many subscription ids have a listener that blocks, the refresh and the notifications of every other id go
 * on, and from Java 24 the notifier takes no platform thread for each of them.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ScheduledRefreshNotifierTest {

    private static final Duration EVENTUALLY = Duration.ofSeconds(5);
    // Starting this many platform threads before Java 24 can take a while on a loaded machine
    private static final Duration ALL_BLOCKING = Duration.ofSeconds(60);
    private static final Duration PROMPTLY = Duration.ofSeconds(10);
    private static final int BLOCKED_IDS = 2000;

    @Test
    void a_listener_that_blocks_through_close_and_ignores_interrupts_keeps_no_jvm_alive() {
        ScheduledRefresh refresh = ScheduledRefresh.auto();
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Thread> deliveredOn = new CompletableFuture<>();
        try {
            refresh.notifyInBackground("s1", () -> {
                deliveredOn.complete(Thread.currentThread());
                awaitIgnoringInterrupts(release);
            });
            assertThat(deliveredOn).as("the notification is delivered").succeedsWithin(EVENTUALLY);
            Thread notifier = deliveredOn.join();

            refresh.close();

            assertThat(notifier.isAlive()).as("the listener still blocks after close()").isTrue();
            assertThat(notifier.isDaemon()).as("[the notifier thread still delivering a notification after close() is a daemon thread]").isTrue();
        } finally {
            release.countDown();
        }
    }

    // Each listener blocks inside a monitor of its own, as a listener of a user's own may
    @Test
    void thousands_of_subscription_ids_whose_listener_blocks_hold_up_neither_the_refresh_nor_another_id() throws InterruptedException {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        int platformThreadsBefore = threads.getThreadCount();
        ScheduledRefresh refresh = ScheduledRefresh.every(Duration.ofMillis(50));
        AtomicInteger refreshes = new AtomicInteger();
        CountDownLatch blocking = new CountDownLatch(BLOCKED_IDS);
        CountDownLatch release = new CountDownLatch(1);
        try {
            refresh.scheduleInBackground(refreshes::incrementAndGet, Duration.ofSeconds(20));
            for (int id = 0; id < BLOCKED_IDS; id++) {
                Object monitor = new Object();
                refresh.notifyInBackground("blocked-" + id, () -> {
                    synchronized (monitor) {
                        blocking.countDown();
                        awaitIgnoringInterrupts(release);
                    }
                });
            }
            assertThat(blocking.await(ALL_BLOCKING.toMillis(), TimeUnit.MILLISECONDS)).as("every listener that blocks has been called").isTrue();
            int platformThreadsWhileBlocking = threads.getThreadCount();
            int refreshesWhileBlocking = refreshes.get();

            CompletableFuture<Void> another = new CompletableFuture<>();
            refresh.notifyInBackground("another", () -> another.complete(null));

            assertThat(another).as("the notification of an id whose listener doesn't block").succeedsWithin(PROMPTLY);
            assertThat(eventually(() -> refreshes.get() > refreshesWhileBlocking)).as("a refresh after every listener blocked").isTrue();
            if (Runtime.version().feature() >= 24) {
                int bound = Runtime.getRuntime().availableProcessors() + 16;
                assertThat(platformThreadsWhileBlocking - platformThreadsBefore).as("the platform threads started while %d listeners block", BLOCKED_IDS).isLessThanOrEqualTo(bound);
            }
        } finally {
            release.countDown();
            refresh.close();
        }
    }

    private static boolean eventually(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + PROMPTLY.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(10);
        }
        return condition.getAsBoolean();
    }

    private static void awaitIgnoringInterrupts(CountDownLatch latch) {
        while (true) {
            try {
                latch.await();
                return;
            } catch (InterruptedException ignored) {
                // A listener of a user's own that goes on waiting
            }
        }
    }
}
