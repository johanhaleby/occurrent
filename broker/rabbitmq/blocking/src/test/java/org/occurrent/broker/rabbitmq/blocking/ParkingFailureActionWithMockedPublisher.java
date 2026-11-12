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
import org.occurrent.broker.api.blocking.DeliveryFailurePolicy;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.mock;

/**
 * A {@link RabbitMqDeliveryFailureAction} that parks through a mocked {@link RabbitMqConfirmPublisher}, which
 * confirms every publish. Public because {@link RabbitMqConfirmPublisher} is package-private and a test in the
 * {@code domain} package cannot name it.
 */
public final class ParkingFailureActionWithMockedPublisher {

    private ParkingFailureActionWithMockedPublisher() {
    }

    public static RabbitMqDeliveryFailureAction over(Channel channel, RabbitMqSourceListenerCalls listener) {
        return new RabbitMqDeliveryFailureAction(channel, DeliveryFailurePolicy.PARK, mock(RabbitMqConfirmPublisher.class),
                RabbitMqDestination.of("exchange", "routingKey"), LoggerFactory.getLogger(ParkingFailureActionWithMockedPublisher.class), listener);
    }
}
