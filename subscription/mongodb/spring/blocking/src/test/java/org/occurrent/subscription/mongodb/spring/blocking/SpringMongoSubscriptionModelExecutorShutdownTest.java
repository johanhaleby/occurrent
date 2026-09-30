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
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether the model sees that an executor it was given no longer takes tasks, for the executor types Spring and the
 * JDK offer, so a resume such an executor rejects throws instead of being handed to it again.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SpringMongoSubscriptionModelExecutorShutdownTest {

    @Test
    void a_thread_pool_task_executor_counts_as_shut_down_once_it_is() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.initialize();
        assertThat(SpringMongoSubscriptionModel.isShutDown(executor)).as("before the shutdown").isFalse();

        executor.shutdown();

        assertThat(SpringMongoSubscriptionModel.isShutDown(executor)).as("after the shutdown").isTrue();
    }

    @Test
    void a_thread_pool_task_scheduler_counts_as_shut_down_once_it_is() {
        ThreadPoolTaskScheduler executor = new ThreadPoolTaskScheduler();
        executor.initialize();
        assertThat(SpringMongoSubscriptionModel.isShutDown(executor)).as("before the shutdown").isFalse();

        executor.shutdown();

        assertThat(SpringMongoSubscriptionModel.isShutDown(executor)).as("after the shutdown").isTrue();
    }

    @Test
    void a_simple_async_task_executor_counts_as_shut_down_once_it_is_closed() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
        assertThat(SpringMongoSubscriptionModel.isShutDown(executor)).as("before the close").isFalse();

        executor.close();

        assertThat(SpringMongoSubscriptionModel.isShutDown(executor)).as("after the close").isTrue();
    }

    @Test
    void a_concurrent_task_executor_counts_as_shut_down_once_the_executor_it_wraps_is() {
        ExecutorService wrapped = Executors.newSingleThreadExecutor();
        ConcurrentTaskExecutor executor = new ConcurrentTaskExecutor(wrapped);
        assertThat(SpringMongoSubscriptionModel.isShutDown(executor)).as("before the shutdown").isFalse();

        wrapped.shutdown();

        assertThat(SpringMongoSubscriptionModel.isShutDown(executor)).as("after the shutdown").isTrue();
    }

    @Test
    void an_executor_service_counts_as_shut_down_once_it_is() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        assertThat(SpringMongoSubscriptionModel.isShutDown(executor)).as("before the shutdown").isFalse();

        executor.shutdown();

        assertThat(SpringMongoSubscriptionModel.isShutDown(executor)).as("after the shutdown").isTrue();
    }

    @Test
    void a_thread_pool_task_executor_that_was_never_initialized_does_not_count_as_shut_down() {
        assertThat(SpringMongoSubscriptionModel.isShutDown(new ThreadPoolTaskExecutor())).isFalse();
    }

    @Test
    void an_executor_the_model_cannot_ask_does_not_count_as_shut_down() {
        assertThat(SpringMongoSubscriptionModel.isShutDown(Runnable::run)).isFalse();
    }
}
