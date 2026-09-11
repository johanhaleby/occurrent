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

package org.occurrent.dsl.saga.blocking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.dsl.saga.SagaInstances;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.occurrent.subscription.api.blocking.Subscription;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Releasing the timer lease is best effort, so whatever it throws, {@link SagaSubscription#close()} still has to stop
 * the timer poller. A {@code RetryStrategy} configured with {@code mapError} can map a failure to a checked exception,
 * which the retry loop rethrows without wrapping, so what arrives here is not always a {@code RuntimeException}.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("closing a saga subscription whose lease release fails")
class SagaSubscriptionCloseTest {

    @Test
    void stops_the_timer_poller_when_the_release_throws_a_checked_exception() {
        ExecutorService timerPoller = Executors.newSingleThreadExecutor();
        SagaSubscription sagaSubscription = new SagaSubscription(new NeverStartedSubscription(), timerPoller,
                SagaInstances.of(SagaStateStore.inMemory()), new ReleaseThrows(new IOException("mapped to a checked exception")),
                "a-lease-key", "a-holder");

        assertThatCode(sagaSubscription::close).doesNotThrowAnyException();

        assertThat(timerPoller.isShutdown())
                .as("the poller shutdown comes after the release, so a throw the catch misses skips it entirely")
                .isTrue();
    }

    @Test
    void stops_the_timer_poller_when_the_release_throws_an_unchecked_exception() {
        ExecutorService timerPoller = Executors.newSingleThreadExecutor();
        SagaSubscription sagaSubscription = new SagaSubscription(new NeverStartedSubscription(), timerPoller,
                SagaInstances.of(SagaStateStore.inMemory()), new ReleaseThrows(new IllegalStateException("MongoDB is not answering")),
                "a-lease-key", "a-holder");

        assertThatCode(sagaSubscription::close).doesNotThrowAnyException();

        assertThat(timerPoller.isShutdown()).isTrue();
    }

    /**
     * Throws {@code failure} from the unregister, the way a retry strategy that has given up rethrows the failure it
     * gave up on. Rethrown unwrapped, so a checked one arrives at the caller as a checked one.
     */
    private record ReleaseThrows(Throwable failure) implements CompetingConsumerStrategy {
        @Override
        public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
            sneakyThrow(failure);
        }

        @Override
        public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        }

        @Override
        public boolean hasLock(String subscriptionId, String subscriberId) {
            return true;
        }

        @Override
        public void addListener(CompetingConsumerListener listenerConsumer) {
        }

        @Override
        public void removeListener(CompetingConsumerListener listenerConsumer) {
        }

        @SuppressWarnings("unchecked")
        private static <T extends Throwable> void sneakyThrow(Throwable throwable) throws T {
            throw (T) throwable;
        }
    }

    private record NeverStartedSubscription() implements Subscription {
        @Override
        public String id() {
            return "a-subscription";
        }

        @Override
        public boolean waitUntilStarted(Duration timeout) {
            return true;
        }
    }
}
