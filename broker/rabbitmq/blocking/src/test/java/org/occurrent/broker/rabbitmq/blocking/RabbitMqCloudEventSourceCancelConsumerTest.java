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

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ChannelContinuationTimeoutException;
import com.rabbitmq.client.DeliverCallback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.push.blocking.PushSubscriptionModel;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * What the lifecycle poll believes about its consumer after a cancel that threw. A cancel that fails with the
 * connection recovered underneath it throws for a consumer the client no longer has, and a source that kept the tag
 * would think it was still consuming and never register another. Only a cancel whose reply timed out may have left
 * the consumer registered, so that one keeps the tag and the next poll cancels again. The poll is driven directly
 * against a mocked {@link Channel}, since nothing schedules it on a source built this way.
 */
class RabbitMqCloudEventSourceCancelConsumerTest {

    private static final String CONSUMER_TAG = "consumer-tag-1";

    private final Channel channel = mock(Channel.class);
    private final AtomicBoolean ready = new AtomicBoolean(true);
    private RabbitMqCloudEventSource source;

    @BeforeEach
    void startConsuming() throws Exception {
        when(channel.basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class))).thenReturn(CONSUMER_TAG);
        PushSubscriptionModel model = new PushSubscriptionModel(DataFieldReader.refusing());
        model.subscribe("sub", cloudEvent -> { });
        RabbitMqDeliveryFailureAction failureAction = new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.REDELIVER, null, null,
                LoggerFactory.getLogger(RabbitMqCloudEventSourceCancelConsumerTest.class));
        source = new RabbitMqCloudEventSource(model, channel, "queue", 1, Duration.ofSeconds(1), failureAction, subscriptionId -> ready.get(), Duration.ofSeconds(1));
        poll();
        verify(channel).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
    }

    @AfterEach
    void closeSource() {
        source.close();
    }

    @Test
    void a_successful_cancel_clears_the_consumer_so_the_next_poll_that_should_consume_registers_another() throws Exception {
        ready.set(false);
        poll();

        ready.set(true);
        poll();

        verify(channel).basicCancel(CONSUMER_TAG);
        verify(channel, times(2)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
    }

    @Test
    void a_cancel_that_fails_with_an_io_exception_clears_the_consumer_so_the_next_poll_that_should_consume_registers_another() throws Exception {
        doThrow(new IOException("Unknown consumerTag")).doNothing().when(channel).basicCancel(CONSUMER_TAG);
        ready.set(false);
        poll();

        ready.set(true);
        poll();

        verify(channel).basicCancel(CONSUMER_TAG);
        verify(channel, times(2)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
    }

    @Test
    void a_cancel_that_fails_with_a_runtime_exception_clears_the_consumer_so_the_next_poll_that_should_consume_registers_another() throws Exception {
        doThrow(new IllegalStateException("the channel is closing")).doNothing().when(channel).basicCancel(CONSUMER_TAG);
        ready.set(false);
        poll();

        ready.set(true);
        poll();

        verify(channel).basicCancel(CONSUMER_TAG);
        verify(channel, times(2)).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
    }

    @Test
    void a_cancel_whose_reply_timed_out_keeps_the_consumer_so_the_next_poll_cancels_it_again_instead_of_registering_another() throws Exception {
        doThrow(new ChannelContinuationTimeoutException(new TimeoutException("no reply"), channel, 1, null))
                .doNothing().when(channel).basicCancel(CONSUMER_TAG);
        ready.set(false);
        poll();

        poll();

        verify(channel, times(2)).basicCancel(CONSUMER_TAG);
        verify(channel).basicConsume(anyString(), anyBoolean(), any(DeliverCallback.class), any(CancelCallback.class));
    }

    private void poll() throws ReflectiveOperationException {
        Method reconcileConsumption = RabbitMqCloudEventSource.class.getDeclaredMethod("reconcileConsumption");
        reconcileConsumption.setAccessible(true);
        reconcileConsumption.invoke(source);
    }
}
