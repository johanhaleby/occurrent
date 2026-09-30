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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * close() does not wait for the notifier, since a listener it is calling may wait for the database. A listener that
 * also ignores the interrupt close() sends goes on running after it, on a thread that must not keep the JVM alive.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ScheduledRefreshNotifierTest {

    private static final Duration EVENTUALLY = Duration.ofSeconds(5);

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
