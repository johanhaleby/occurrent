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

package org.occurrent.example.saga.orderfulfillment.flow

import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.junit.jupiter.api.*
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper
import org.occurrent.command.CommandDispatcher
import org.occurrent.dsl.saga.SagaStateStore
import org.occurrent.dsl.saga.blocking.SagaRunner
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig
import org.occurrent.dsl.saga.blocking.SagaSubscription
import org.occurrent.dsl.saga.flow.FlowState
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.example.saga.orderfulfillment.*
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.time.Duration
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

/**
 * Runs [orderFulfillmentFlow] through [SagaRunner]: the happy path (payment reserved, order shipped) and the payment
 * timeout path (nobody reserves or fails the payment, so the poller fires the timeout and cancels the order).
 */
@DisplayNameGeneration(ReplaceUnderscores::class)
class OrderFulfillmentFlowSagaTest {

    private lateinit var subscriptionModel: InMemorySubscriptionModel
    private lateinit var eventStore: InMemoryEventStore
    private lateinit var converter: CloudEventConverter<OrderEvent>
    private val subscriptionsToClose = mutableListOf<SagaSubscription>()

    @BeforeEach
    fun createInstances() {
        subscriptionModel = InMemorySubscriptionModel()
        eventStore = InMemoryEventStore(subscriptionModel)
        converter = JacksonCloudEventConverter.Builder<OrderEvent>(ObjectMapper(), URI.create("urn:occurrent:example:saga:order-fulfillment"))
            .typeMapper(ReflectionCloudEventTypeMapper.simple(OrderEvent::class.java))
            .idMapper { UUID.randomUUID().toString() }
            .build()
    }

    @AfterEach
    fun shutdown() {
        subscriptionsToClose.forEach(SagaSubscription::close)
        subscriptionModel.shutdown()
    }

    private fun run(
        subscriptionId: String,
        stateStore: SagaStateStore<FlowState<OrderEvent>>,
        dispatcher: CommandDispatcher<OrderCommand>,
        paymentTimeout: Duration,
        config: SagaRunnerConfig = SagaRunnerConfig.defaults()
    ): SagaSubscription {
        val runner = SagaRunner.agnostic<OrderEvent, OrderCommand>(subscriptionModel, converter)
        val subscription = runner.run(subscriptionId, orderFulfillmentFlow(paymentTimeout), stateStore, dispatcher, null, config)
        subscriptionsToClose.add(subscription)
        return subscription
    }

    private fun write(orderId: String, vararg events: OrderEvent) {
        eventStore.write(orderId, converter.toCloudEvents(events.toList()))
    }

    @Nested
    inner class HappyPath {

        @Test
        fun `a reservation ships the order and completes the instance`() {
            val orderId = "flow-order-1"
            val stateStore = SagaStateStore.inMemory<FlowState<OrderEvent>>()
            val issued = CopyOnWriteArrayList<OrderCommand>()
            val dispatcher = CommandDispatcher<OrderCommand> { issued.add(it) }
            run("flow-happy-path", stateStore, dispatcher, Duration.ofMinutes(30)).waitUntilStarted()

            write(orderId, OrderPlaced(orderId, 42.0))
            write(orderId, PaymentReserved(orderId))

            await.untilAsserted { assertThat(issued).containsExactly(ReservePayment(orderId, 42.0), ShipOrder(orderId)) }
            val envelope = stateStore.find(orderId).orElseThrow()
            assertThat(envelope.state().completed()).isTrue()
        }
    }

    @Nested
    inner class PaymentTimeout {

        @Test
        fun `nobody resolving the payment in time cancels the order once the timer fires`() {
            val orderId = "flow-order-2"
            val stateStore = SagaStateStore.inMemory<FlowState<OrderEvent>>()
            val issued = CopyOnWriteArrayList<OrderCommand>()
            val dispatcher = CommandDispatcher<OrderCommand> { issued.add(it) }
            val config = SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofMillis(50))
            run("flow-payment-timeout", stateStore, dispatcher, Duration.ofMillis(150), config).waitUntilStarted()

            write(orderId, OrderPlaced(orderId, 42.0))

            await.atMost(5, TimeUnit.SECONDS).untilAsserted {
                assertThat(issued).containsExactly(ReservePayment(orderId, 42.0), CancelOrder(orderId, "payment timeout"))
            }
            val envelope = stateStore.find(orderId).orElseThrow()
            assertThat(envelope.state().completed()).isTrue()
        }
    }
}
