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

package org.occurrent.deadline.inmemory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.deadline.api.blocking.Deadline;
import org.occurrent.deadline.inmemory.InMemoryDeadlineConsumerRegistry.Config;
import org.occurrent.retry.RetryStrategy;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * <a href="https://github.com/johanhaleby/occurrent/issues/999">#999</a>: a consumer that keeps throwing is retried
 * with a backoff between attempts, and {@code shutdown()} joins the polling thread without a timeout. A backoff that
 * is only interrupted between attempts therefore holds the whole shutdown open for as long as one backoff lasts.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("shutting down the in-memory deadline consumer registry")
// Not what this test relies on. The shutdown runs on a daemon thread joined with its own much shorter bound, so
// a shutdown that waits out the backoff fails an assertion rather than running out this timeout.
@Timeout(60)
class InMemoryDeadlineConsumerRegistryShutdownTest {

    /**
     * Far longer than this test is willing to wait, so a registry that only reads its lifecycle flag between
     * attempts sits out the whole backoff before the join returns.
     */
    private static final Duration BACKOFF_BETWEEN_ATTEMPTS = Duration.ofSeconds(10);

    private static final Duration MUST_FINISH_WITHIN = Duration.ofSeconds(5);

    @Test
    void stops_a_consumer_that_is_between_retry_attempts() throws InterruptedException {
        BlockingDeque<Object> queue = new LinkedBlockingDeque<>();
        CountDownLatch firstAttemptFailed = new CountDownLatch(1);
        InMemoryDeadlineConsumerRegistry registry = new InMemoryDeadlineConsumerRegistry(queue,
                new Config().retryStrategy(RetryStrategy.fixed(BACKOFF_BETWEEN_ATTEMPTS)));
        InMemoryDeadlineScheduler scheduler = new InMemoryDeadlineScheduler(queue);
        try {
            registry.register("Something", (id, category, deadline, data) -> {
                firstAttemptFailed.countDown();
                throw new IllegalStateException("this consumer never succeeds");
            });
            scheduler.schedule(UUID.randomUUID(), "Something", Deadline.afterMillis(0), "some data");

            assertThat(firstAttemptFailed.await(MUST_FINISH_WITHIN.toMillis(), TimeUnit.MILLISECONDS))
                    .as("the consumer should have been called and thrown, so the backoff before the retry has started")
                    .isTrue();

            Thread shutdown = new Thread(registry::shutdown);
            shutdown.setDaemon(true);
            shutdown.start();
            shutdown.join(MUST_FINISH_WITHIN.toMillis());

            assertThat(shutdown.isAlive())
                    .as("shutdown joins the polling thread without a timeout, so a backoff that ignores the "
                            + "lifecycle flag holds every later shutdown step behind it")
                    .isFalse();
        } finally {
            // The registry's polling thread is not a daemon, so an assertion that fails before the shutdown above
            // has run would otherwise leave it alive for the rest of the test run.
            registry.shutdown();
            scheduler.shutdown();
        }
    }

    @Test
    void does_not_retry_an_exception_the_caller_excluded() throws InterruptedException {
        BlockingDeque<Object> queue = new LinkedBlockingDeque<>();
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch consumerCalled = new CountDownLatch(1);
        // The consumer throws IllegalArgumentException, which this predicate refuses to retry. A registry that
        // replaces the predicate with its own lifecycle flag retries it anyway, forever.
        InMemoryDeadlineConsumerRegistry registry = new InMemoryDeadlineConsumerRegistry(queue,
                new Config().retryStrategy(RetryStrategy.fixed(Duration.ofMillis(10))
                        .retryIf(IllegalStateException.class::isInstance)));
        InMemoryDeadlineScheduler scheduler = new InMemoryDeadlineScheduler(queue);
        try {
            registry.register("Something", (id, category, deadline, data) -> {
                attempts.incrementAndGet();
                consumerCalled.countDown();
                throw new IllegalArgumentException("this consumer never succeeds and must not be retried");
            });
            scheduler.schedule(UUID.randomUUID(), "Something", Deadline.afterMillis(0), "some data");

            assertThat(consumerCalled.await(MUST_FINISH_WITHIN.toMillis(), TimeUnit.MILLISECONDS)).isTrue();

            await().during(Duration.ofMillis(500))
                    .atMost(Duration.ofSeconds(3))
                    .untilAtomic(attempts, equalTo(1));
        } finally {
            registry.shutdown();
            scheduler.shutdown();
        }
    }
}
