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

package org.occurrent.dsl.saga.flow

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.occurrent.dsl.saga.SagaInput

/**
 * Kept out of SagaFlowExtensionsTest.kt because PR #742 is concurrently editing that file.
 *
 * Proves [StepScope.on]'s [StepCondition] parameter matches the Java side's `StepCondition<? extends E>` rather than
 * the invariant `StepCondition<E>`: a leaf built once over a narrower event type binds, unmodified, to a step
 * declared over a broader event hierarchy.
 */
class StepConditionKotlinVarianceTest {

    sealed interface BaseEvent
    data class Started(val id: String) : BaseEvent
    data class Narrow(val id: String) : BaseEvent

    sealed interface Cmd

    /** Built over [Narrow] alone, and reused below in a step declared over the wider [BaseEvent]. */
    private val narrowCondition: StepCondition<Narrow> = StepCondition.event(Narrow::class.java)

    @Test
    fun `a StepCondition built over a narrower type binds to a step declared over the broader hierarchy`() {
        val saga = saga<BaseEvent, Cmd> {
            startsOn<Started>()
            correlateAll { "s1" }
            step("wait") {
                on(narrowCondition, then = end)
            }
        }

        val started = saga.evolve(saga.initialState(), SagaInput.event(Started("s1")))
        val fulfilled = saga.evolve(started, SagaInput.event(Narrow("s1")))

        assertThat(saga.isTerminal(fulfilled)).isTrue()
    }
}
