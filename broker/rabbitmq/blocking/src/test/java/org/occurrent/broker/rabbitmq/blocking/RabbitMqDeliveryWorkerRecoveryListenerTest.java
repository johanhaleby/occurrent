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

package org.occurrent.broker.rabbitmq.blocking;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.withSettings;

@DisplayName("RabbitMqDeliveryWorkerRecoveryListener")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RabbitMqDeliveryWorkerRecoveryListenerTest {

    @Nested
    @DisplayName("when the channel can recover")
    class When_the_channel_can_recover {

        @Test
        void discardOnRecovery_adds_exactly_one_recovery_listener() {
            // Given
            RabbitMqDeliveryWorker worker = worker();
            Channel channel = recoverableChannel();

            // When
            worker.discardOnRecovery(channel);

            // Then
            then(recoverable(channel)).should(times(1)).addRecoveryListener(any());
        }

        @Test
        void the_listener_stays_on_the_channel_until_the_worker_stops() {
            // Given
            RabbitMqDeliveryWorker worker = worker();
            Channel channel = recoverableChannel();

            // When
            worker.discardOnRecovery(channel);

            // Then
            then(recoverable(channel)).should(never()).removeRecoveryListener(any());
        }

        @Test
        void stopAcceptingWork_removes_the_very_listener_it_added_once() {
            // Given
            RabbitMqDeliveryWorker worker = worker();
            Channel channel = recoverableChannel();
            worker.discardOnRecovery(channel);
            RecoveryListener added = addedListener(channel);

            // When
            worker.stopAcceptingWork();

            // Then
            then(recoverable(channel)).should(times(1)).removeRecoveryListener(added);
        }

        @Test
        void stopping_again_does_not_remove_the_listener_a_second_time() {
            // Given
            RabbitMqDeliveryWorker worker = worker();
            Channel channel = recoverableChannel();
            worker.discardOnRecovery(channel);
            worker.stopAcceptingWork();

            // When
            worker.stopAcceptingWork();
            worker.stop(Duration.ofSeconds(1));

            // Then
            then(recoverable(channel)).should(times(1)).removeRecoveryListener(any());
        }

        @Test
        void stop_removes_the_very_listener_it_added_once() {
            // Given
            RabbitMqDeliveryWorker worker = worker();
            Channel channel = recoverableChannel();
            worker.discardOnRecovery(channel);
            RecoveryListener added = addedListener(channel);

            // When
            worker.stop(Duration.ofSeconds(1));

            // Then
            then(recoverable(channel)).should(times(1)).removeRecoveryListener(added);
        }

        @Test
        void discardOnRecovery_after_the_worker_stopped_adds_no_listener() {
            // Given
            RabbitMqDeliveryWorker worker = worker();
            Channel channel = recoverableChannel();
            worker.stopAcceptingWork();

            // When
            worker.discardOnRecovery(channel);

            // Then
            then(recoverable(channel)).should(never()).addRecoveryListener(any());
        }

        @Test
        void stop_removes_the_listener_of_each_channel_the_worker_was_given() {
            // Given
            RabbitMqDeliveryWorker worker = worker();
            Channel first = recoverableChannel();
            Channel second = recoverableChannel();
            worker.discardOnRecovery(first);
            worker.discardOnRecovery(second);
            RecoveryListener addedToFirst = addedListener(first);
            RecoveryListener addedToSecond = addedListener(second);

            // When
            worker.stop(Duration.ofSeconds(1));

            // Then
            then(recoverable(first)).should(times(1)).removeRecoveryListener(addedToFirst);
            then(recoverable(second)).should(times(1)).removeRecoveryListener(addedToSecond);
        }

        @Test
        void stopAcceptingWork_still_removes_the_second_listener_when_removing_the_first_throws() {
            // Given
            RabbitMqDeliveryWorker worker = worker();
            Channel first = recoverableChannel();
            Channel second = recoverableChannel();
            worker.discardOnRecovery(first);
            worker.discardOnRecovery(second);
            RecoveryListener addedToSecond = addedListener(second);
            doThrow(new IllegalStateException("channel closed")).when(recoverable(first)).removeRecoveryListener(any());

            // When
            catchThrowable(worker::stopAcceptingWork);

            // Then
            then(recoverable(second)).should(times(1)).removeRecoveryListener(addedToSecond);
        }

        @Test
        void stopAcceptingWork_throws_the_failure_of_the_removal_that_failed() {
            // Given
            RabbitMqDeliveryWorker worker = worker();
            Channel first = recoverableChannel();
            Channel second = recoverableChannel();
            worker.discardOnRecovery(first);
            worker.discardOnRecovery(second);
            IllegalStateException failure = new IllegalStateException("channel closed");
            doThrow(failure).when(recoverable(first)).removeRecoveryListener(any());

            // When
            Throwable thrown = catchThrowable(worker::stopAcceptingWork);

            // Then
            assertThat(thrown).isSameAs(failure);
        }

        @Test
        void stop_still_waits_for_a_running_delivery_when_removing_a_listener_throws_and_then_throws_that_failure() throws Exception {
            // Given
            RabbitMqDeliveryWorker worker = worker();
            Channel channel = recoverableChannel();
            worker.discardOnRecovery(channel);
            IllegalStateException failure = new IllegalStateException("channel closed");
            doThrow(failure).when(recoverable(channel)).removeRecoveryListener(any());
            CountDownLatch started = new CountDownLatch(1);
            AtomicBoolean finished = new AtomicBoolean();
            worker.submit(1L, () -> {
                started.countDown();
                try {
                    Thread.sleep(300);
                    finished.set(true);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

            // When
            Throwable thrown = catchThrowable(() -> worker.stop(Duration.ofSeconds(5)));

            // Then
            assertThat(finished).as("the delivery had finished when stop threw").isTrue();
            assertThat(thrown).isSameAs(failure);
        }
    }

    @Nested
    @DisplayName("when the channel cannot recover")
    class When_the_channel_cannot_recover {

        @Test
        void discardOnRecovery_adds_nothing_and_stop_still_completes() {
            // Given
            RabbitMqDeliveryWorker worker = worker();
            Channel channel = mock(Channel.class);

            // When
            Throwable thrown = catchThrowable(() -> {
                worker.discardOnRecovery(channel);
                worker.stop(Duration.ofSeconds(1));
            });

            // Then
            assertThat(thrown).isNull();
        }
    }

    private static RabbitMqDeliveryWorker worker() {
        return new RabbitMqDeliveryWorker("recovery-listener-test", "queue", LoggerFactory.getLogger(RabbitMqDeliveryWorkerRecoveryListenerTest.class));
    }

    private static Channel recoverableChannel() {
        return mock(Channel.class, withSettings().extraInterfaces(Recoverable.class));
    }

    private static Recoverable recoverable(Channel channel) {
        return (Recoverable) channel;
    }

    private static RecoveryListener addedListener(Channel channel) {
        ArgumentCaptor<RecoveryListener> captor = ArgumentCaptor.forClass(RecoveryListener.class);
        then(recoverable(channel)).should().addRecoveryListener(captor.capture());
        return captor.getValue();
    }
}
