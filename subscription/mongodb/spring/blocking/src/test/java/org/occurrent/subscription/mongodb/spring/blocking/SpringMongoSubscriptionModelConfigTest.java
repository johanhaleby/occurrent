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

package org.occurrent.subscription.mongodb.spring.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.subscription.mongodb.spring.blocking.SpringMongoSubscriptionModelConfig.withConfig;

@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSubscriptionModelConfigTest {

    @Test
    void max_await_time_is_unset_by_default() {
        SpringMongoSubscriptionModelConfig config = withConfig("events", TimeRepresentation.DATE);

        assertThat(config.maxAwaitTime).isNull();
    }

    @Test
    void max_await_time_is_retained() {
        SpringMongoSubscriptionModelConfig config = withConfig("events", TimeRepresentation.DATE).maxAwaitTime(Duration.ofMillis(500));

        assertThat(config.maxAwaitTime).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void max_await_time_throws_iae_when_zero() {
        Throwable throwable = catchThrowable(() -> withConfig("events", TimeRepresentation.DATE).maxAwaitTime(Duration.ZERO));

        assertThat(throwable).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxAwaitTime");
    }

    @Test
    void max_await_time_throws_iae_when_negative() {
        Throwable throwable = catchThrowable(() -> withConfig("events", TimeRepresentation.DATE).maxAwaitTime(Duration.ofMillis(-1)));

        assertThat(throwable).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxAwaitTime");
    }

    @Test
    void use_virtual_threads_runs_executor_tasks_on_virtual_threads() throws InterruptedException {
        SpringMongoSubscriptionModelConfig config = withConfig("events", TimeRepresentation.DATE).useVirtualThreads();
        ThreadPoolTaskExecutor executor = (ThreadPoolTaskExecutor) config.executor;
        CountDownLatch executed = new CountDownLatch(1);
        AtomicBoolean virtual = new AtomicBoolean(false);

        try {
            executor.execute(() -> {
                virtual.set(Thread.currentThread().isVirtual());
                executed.countDown();
            });

            assertThat(executed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(virtual).isTrue();
        } finally {
            executor.shutdown();
        }
    }
}
