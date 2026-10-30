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

package org.occurrent.dsl.saga

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.occurrent.dsl.saga.SagaEnvelope.StartMark
import org.occurrent.dsl.saga.flow.FlowState
import org.occurrent.dsl.saga.flow.saga as flowSaga
import org.occurrent.dsl.saga.internal.SagaExecutionSupport
import org.occurrent.dsl.saga.internal.SagaExecutionSupport.EventMeta
import org.occurrent.filter.Filter
import java.time.Instant

/**
 * Whether an event of the stream that started an instance, below the start, waits held. It does when it is not a start
 * event, however the start event type is declared. A Kotlin sealed class is abstract, and a saga from either DSL that starts
 * on one, on an abstract class that is not sealed or on a final class shares that rule.
 */
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class SagaBeforeTheStartKotlinTest {

    sealed interface OrderEvent {
        val orderId: String
    }

    /** The intakes that can open an order, as a Kotlin sealed class, which is abstract. */
    sealed class Intake : OrderEvent

    data class PhoneIntake(override val orderId: String) : Intake()
    data class WebIntake(override val orderId: String) : Intake()

    /** An abstract class that is not sealed, so a saga can start on it only under a replacement filter. */
    abstract class LooseIntake : OrderEvent

    data class PhoneLooseIntake(override val orderId: String) : LooseIntake()
    data class WebLooseIntake(override val orderId: String) : LooseIntake()

    /** A final class, so it stands for one event type. */
    data class Placed(override val orderId: String) : OrderEvent

    data class Noted(override val orderId: String) : OrderEvent

    sealed interface OrderCommand
    data class Notify(val orderId: String) : OrderCommand

    companion object {
        private const val SAGA_ID = "order-1"
        private const val STREAM = "order-stream"
        private val NOW = Instant.parse("2026-01-01T00:00:00Z")

        /** An instance that started at version 5 of the stream, and the question of whether the event at version 3 is below its start. */
        private fun <S> theEventAtThreeIsBelowTheStart(saga: Saga<OrderEvent, S, OrderCommand>, state: S, event: OrderEvent): Boolean {
            val startedAtFive = SagaEnvelope(
                SAGA_ID, state, SagaStatus.ACTIVE, 3L, emptyList(), emptyMap(), null, NOW, NOW, null, null, true, null,
                emptyMap(), StartMark(STREAM, 5L, null)
            )
            return SagaExecutionSupport.isBelowTheStart(saga, startedAtFive, SagaInput.event(event), EventMeta(STREAM, 3L, null, event::class.java.name))
        }

        private fun <S> theNoteAtThreeIsBelowTheStart(saga: Saga<OrderEvent, S, OrderCommand>, state: S): Boolean =
            theEventAtThreeIsBelowTheStart(saga, state, Noted(SAGA_ID))

        private fun sagaStartingOnTheSealedClass(): Saga<OrderEvent, String?, OrderCommand> =
            saga(initialState = null) {
                correlateAll { it.orderId }
                startsOn<Intake>()
                evolve<Intake> { _, _ -> "open" }
                evolve<Noted> { state, _ -> state }
                react<Noted> { _, e -> issue(Notify(e.orderId)) }
            }

        private fun sagaStartingOnTheFinalClass(): Saga<OrderEvent, String?, OrderCommand> =
            saga(initialState = null) {
                correlateAll { it.orderId }
                startsOn<Placed>()
                evolve<Placed> { _, _ -> "open" }
                evolve<Noted> { state, _ -> state }
                react<Noted> { _, e -> issue(Notify(e.orderId)) }
            }

        private fun sagaStartingOnTheAbstractClassThatIsNotSealed(): Saga<OrderEvent, String?, OrderCommand> =
            saga(initialState = null) {
                correlateAll { it.orderId }
                replacementFilter(Filter.all())
                startsOn<LooseIntake>()
                evolve<LooseIntake> { _, _ -> "open" }
                evolve<Noted> { state, _ -> state }
                react<Noted> { _, e -> issue(Notify(e.orderId)) }
            }

        private fun flowSagaStartingOnTheSealedClass(): Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> =
            flowSaga {
                correlateAll { it.orderId }
                startsOn<Intake>()
                step("watching") {
                    on<Noted>(then = end)
                }
            }

        private fun flowSagaStartingOnTheFinalClass(): Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> =
            flowSaga {
                correlateAll { it.orderId }
                startsOn<Placed>()
                step("watching") {
                    on<Noted>(then = end)
                }
            }

        private fun flowSagaStartingOnTheAbstractClassThatIsNotSealed(): Saga<OrderEvent, FlowState<OrderEvent>, OrderCommand> =
            flowSaga {
                correlateAll { it.orderId }
                replacementFilter(Filter.all())
                startsOn<LooseIntake>()
                step("watching") {
                    on<Noted>(then = end)
                }
            }
    }

    @Nested
    inner class SagaFromTheSagaDsl {

        @Test
        fun `an event that is not a start event is below the start when the start event type is a sealed class`() {
            val saga = sagaStartingOnTheSealedClass()

            assertThat(theNoteAtThreeIsBelowTheStart(saga, "open")).isTrue()
        }

        @Test
        fun `an event that is not a start event is below the start when the start event type is an abstract class that is not sealed`() {
            val saga = sagaStartingOnTheAbstractClassThatIsNotSealed()

            assertThat(theNoteAtThreeIsBelowTheStart(saga, "open")).isTrue()
        }

        @Test
        fun `an event that is not a start event is below the start when the start event type is a final class`() {
            val saga = sagaStartingOnTheFinalClass()

            assertThat(theNoteAtThreeIsBelowTheStart(saga, "open")).isTrue()
        }
    }

    @Nested
    inner class AStartEventBelowTheStart {

        @Test
        fun `is not below the start for a saga from the saga dsl that starts on a sealed class`() {
            val saga = sagaStartingOnTheSealedClass()

            assertThat(theEventAtThreeIsBelowTheStart(saga, "open", PhoneIntake(SAGA_ID))).isFalse()
        }

        @Test
        fun `is not below the start for a saga from the flow dsl that starts on a sealed class`() {
            val saga = flowSagaStartingOnTheSealedClass()

            assertThat(theEventAtThreeIsBelowTheStart(saga, saga.initialState(), WebIntake(SAGA_ID))).isFalse()
        }

        @Test
        fun `is not below the start for a saga that starts on an abstract class that is not sealed`() {
            val saga = sagaStartingOnTheAbstractClassThatIsNotSealed()

            assertThat(theEventAtThreeIsBelowTheStart(saga, "open", PhoneLooseIntake(SAGA_ID))).isFalse()
        }
    }

    @Nested
    inner class SagaFromTheFlowDsl {

        @Test
        fun `an event that is not a start event is below the start when the start event type is a sealed class`() {
            val saga = flowSagaStartingOnTheSealedClass()

            assertThat(theNoteAtThreeIsBelowTheStart(saga, saga.initialState())).isTrue()
        }

        @Test
        fun `an event that is not a start event is below the start when the start event type is an abstract class that is not sealed`() {
            val saga = flowSagaStartingOnTheAbstractClassThatIsNotSealed()

            assertThat(theNoteAtThreeIsBelowTheStart(saga, saga.initialState())).isTrue()
        }

        @Test
        fun `an event that is not a start event is below the start when the start event type is a final class`() {
            val saga = flowSagaStartingOnTheFinalClass()

            assertThat(theNoteAtThreeIsBelowTheStart(saga, saga.initialState())).isTrue()
        }
    }
}
