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

package org.occurrent.dsl.saga.blocking

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.*
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter
import org.occurrent.application.service.blocking.ApplicationService
import org.occurrent.application.service.blocking.generic.GenericApplicationService
import org.occurrent.command.CommandDispatchers
import org.occurrent.command.Invocation
import org.occurrent.dsl.saga.Saga
import org.occurrent.dsl.saga.SagaInput
import org.occurrent.dsl.saga.SagaStateStore
import org.occurrent.dsl.saga.flow.FlowState
import org.occurrent.dsl.saga.flow.initiating
import org.occurrent.dsl.saga.flow.saga
import org.occurrent.dsl.saga.saga
import org.occurrent.eventstore.inmemory.InMemoryEventStore
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel
import java.net.URI
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * A saga whose command type is [Invocation] issues the domain function itself, so this fixture defines no command types
 * and no command handler. Everything the saga "commands" is one of the three pure functions below.
 *
 * The assertions are on the events that end up in the target stream rather than on the commands issued, because an
 * invocation holds a lambda and so has no useful value equality. That is the documented way to test a saga written this
 * way, and it asserts what the command did rather than what it was named.
 */
@DisplayName("A saga issuing invocations instead of commands")
@DisplayNameGeneration(ReplaceUnderscores::class)
class InvocationSagaTest {

    // --- Domain. No command types anywhere. ---

    sealed interface OrderEvent {
        val eventId: String
        val orderId: String
    }

    data class OrderPlaced(override val eventId: String, override val orderId: String) : OrderEvent
    data class PaymentReserved(override val eventId: String, override val orderId: String) : OrderEvent
    data class PaymentRequested(override val eventId: String, override val orderId: String) : OrderEvent
    data class OrderShipped(override val eventId: String, override val orderId: String) : OrderEvent
    data class OrderCancelled(override val eventId: String, override val orderId: String, val reason: String) : OrderEvent

    /** The command type. `Invocation<OrderEvent>` spelled out at every declaration site is what the alias saves. */
    private fun requestPayment(events: List<OrderEvent>, orderId: String): List<OrderEvent> =
        if (events.any { it is PaymentRequested }) emptyList() else listOf(PaymentRequested(newId(), orderId))

    /**
     * Returns the narrower `List<OrderShipped>` where `List<OrderEvent>` is expected. Kotlin's `List` is covariant and
     * the extension's decision parameter is an ordinary Kotlin function type, so this is accepted. Worth pinning down,
     * because the Java `Function<List<E>, List<E>>` underneath is invariant.
     */
    private fun ship(events: List<OrderEvent>, orderId: String): List<OrderShipped> =
        if (events.any { it is OrderShipped }) emptyList() else listOf(OrderShipped(newId(), orderId))

    private fun cancel(events: List<OrderEvent>, orderId: String, reason: String): List<OrderEvent> =
        if (events.any { it is OrderCancelled }) emptyList() else listOf(OrderCancelled(newId(), orderId, reason))

    private fun newId() = UUID.randomUUID().toString()

    // --- Fixture ---

    private lateinit var subscriptionModel: InMemorySubscriptionModel
    private lateinit var eventStore: InMemoryEventStore
    private lateinit var writeSideEventStore: InMemoryEventStore
    private lateinit var converter: CloudEventConverter<OrderEvent>
    private lateinit var applicationService: ApplicationService<OrderEvent>
    private val subscriptionsToClose = mutableListOf<SagaSubscription>()

    @BeforeEach
    fun create_instances() {
        subscriptionModel = InMemorySubscriptionModel()
        eventStore = InMemoryEventStore(subscriptionModel)
        // Separate from the subscribed store, so what the saga writes is never fed back to the saga.
        writeSideEventStore = InMemoryEventStore()
        converter = JacksonCloudEventConverter.Builder<OrderEvent>(ObjectMapper().registerKotlinModule(), URI.create("urn:test"))
            .idMapper(OrderEvent::eventId)
            .build()
        applicationService = GenericApplicationService(writeSideEventStore, converter)
    }

    @AfterEach
    fun shutdown() {
        subscriptionsToClose.forEach(SagaSubscription::close)
        subscriptionModel.shutdown()
    }

    private fun run(subscriptionId: String, saga: Saga<OrderEvent, FlowState<OrderEvent>, Invocation<OrderEvent>>): SagaSubscription {
        val config = SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofMillis(50))
        val subscription = SagaRunner.agnostic<OrderEvent, Invocation<OrderEvent>>(subscriptionModel, converter)
            .run(subscriptionId, saga, SagaStateStore.inMemory(), CommandDispatchers.invocation(applicationService), null, config)
        subscriptionsToClose.add(subscription)
        subscription.waitUntilStarted()
        return subscription
    }

    private fun write(streamId: String, vararg events: OrderEvent) {
        eventStore.write(streamId, converter.toCloudEvents(events.toList()))
    }

    private fun shipmentEvents(orderId: String): List<OrderEvent> =
        writeSideEventStore.read("shipment-$orderId").eventList().map(converter::toDomainEvent)

    // --- The sagas ---

    /** Covers `startsOn`, `on` and `timeout`, the three flow reaction forms that take a single event or a timeout. */
    private fun orderFlow(paymentTimeout: Duration) = saga<OrderEvent, Invocation<OrderEvent>> {
        correlateAll { it.orderId }

        startsOn<OrderPlaced> { placed ->
            issue("shipment-${placed.orderId}") { events -> requestPayment(events, placed.orderId) }
        }

        step("await-payment") {
            on<PaymentReserved>(then = end) { reserved ->
                issue("shipment-${reserved.orderId}") { events -> ship(events, reserved.orderId) }
            }
            timeout(after = paymentTimeout, then = end) { received ->
                val orderId = received.initiating<OrderPlaced>().orderId
                issue("shipment-$orderId") { events -> cancel(events, orderId, "payment timeout") }
            }
        }
    }

    /** Covers the remaining flow reaction form, `join`. */
    private fun twoPaymentsFlow() = saga<OrderEvent, Invocation<OrderEvent>> {
        correlateAll { it.orderId }
        startsOn<OrderPlaced>()

        step("await-both-payments") {
            join(expect<PaymentReserved>(2), then = end) { received ->
                val orderId = received.initiating<OrderPlaced>().orderId
                issue("shipment-$orderId") { events -> ship(events, orderId) }
            }
        }
    }

    @Nested
    @DisplayName("through a flow saga")
    inner class FlowSagas {

        @Test
        fun `the start reaction and a branch each run their domain function against the target stream`() {
            run("invocation-happy-path", orderFlow(Duration.ofMinutes(30)))

            write("order-1", OrderPlaced(newId(), "order-1"))
            write("order-1", PaymentReserved(newId(), "order-1"))

            await().atMost(5, TimeUnit.SECONDS).untilAsserted {
                assertThat(shipmentEvents("order-1"))
                    .extracting<Class<*>> { it.javaClass }
                    .containsExactly(PaymentRequested::class.java, OrderShipped::class.java)
            }
        }

        @Test
        fun `a timeout reaction dispatches an invocation the same way an event reaction does`() {
            run("invocation-timeout", orderFlow(Duration.ofMillis(150)))

            write("order-2", OrderPlaced(newId(), "order-2"))

            await().atMost(5, TimeUnit.SECONDS).untilAsserted {
                val written = shipmentEvents("order-2")
                assertThat(written)
                    .extracting<Class<*>> { it.javaClass }
                    .containsExactly(PaymentRequested::class.java, OrderCancelled::class.java)
                assertThat(written.filterIsInstance<OrderCancelled>().single().reason).isEqualTo("payment timeout")
            }
        }

        @Test
        fun `a join reaction dispatches an invocation once its expectation is met`() {
            run("invocation-join", twoPaymentsFlow())

            write("order-3", OrderPlaced(newId(), "order-3"))
            write("order-3", PaymentReserved(newId(), "order-3"))

            // One reservation is not two, so nothing has been written yet.
            write("order-3", PaymentReserved(newId(), "order-3"))

            await().atMost(5, TimeUnit.SECONDS).untilAsserted {
                assertThat(shipmentEvents("order-3"))
                    .extracting<Class<*>> { it.javaClass }
                    .containsExactly(OrderShipped::class.java)
            }
        }
    }

    @Nested
    @DisplayName("through the core DSL")
    inner class CoreDsl {

        /**
         * The three explicit type arguments are the price of `SagaEffects<C : Any>` having nothing to infer `C` from. A
         * `typealias OrderInvocation = Invocation<OrderEvent>` is what a real codebase would use.
         */
        private fun orderSaga() = saga<OrderEvent, Boolean, Invocation<OrderEvent>>(initialState = false) {
            correlateAll { it.orderId }
            startsOn<OrderPlaced>()
            evolve<PaymentReserved> { _, _ -> true }
            react<PaymentReserved> { _, reserved ->
                issue("shipment-${reserved.orderId}") { events -> ship(events, reserved.orderId) }
                startTimeout("audit", Duration.ofMinutes(5))
            }
        }

        @Test
        fun `an invocation is asserted by applying its decision, which checks what the command does`() {
            val saga = orderSaga()
            val placed = OrderPlaced(newId(), "order-4")

            val step = saga.step(saga.initialState(), SagaInput.event(PaymentReserved(newId(), "order-4")))

            assertThat(step.issuedCommands()).singleElement().satisfies({ invocation ->
                assertThat(invocation.streamId()).isEqualTo("shipment-order-4")
                assertThat(invocation.decision().apply(listOf(placed)))
                    .singleElement()
                    .isInstanceOf(OrderShipped::class.java)
            })
        }

        @Test
        fun `a decision that has already run returns nothing, which is what makes at-least-once dispatch safe`() {
            val saga = orderSaga()
            val orderId = "order-5"
            val alreadyShipped = listOf<OrderEvent>(OrderPlaced(newId(), orderId), OrderShipped(newId(), orderId))

            val step = saga.step(saga.initialState(), SagaInput.event(PaymentReserved(newId(), orderId)))

            assertThat(step.issuedCommands().single().decision().apply(alreadyShipped)).isEmpty()
        }

        @Test
        fun `timer effects stay separable from invocations`() {
            val saga = orderSaga()

            val step = saga.step(saga.initialState(), SagaInput.event(PaymentReserved(newId(), "order-6")))

            assertThat(step.timerEffects()).containsExactly(org.occurrent.dsl.saga.SagaEffect.startTimeout("audit", Duration.ofMinutes(5)))
        }
    }
}
