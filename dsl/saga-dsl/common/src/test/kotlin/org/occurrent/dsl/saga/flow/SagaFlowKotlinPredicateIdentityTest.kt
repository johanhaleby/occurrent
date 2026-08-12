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
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.occurrent.dsl.saga.SagaInput

/**
 * Kotlin parity for two Java-side pairs, kept out of SagaFlowExtensionsTest.kt as its own file, the same way
 * StepConditionKotlinVarianceTest.kt is: `FlowSagaTest`'s
 * (`two_leaves_sharing_a_name_and_a_predicate_are_accepted_under_the_cap` /
 * `two_leaves_sharing_a_name_while_holding_different_predicates_are_refused_the_cap`), and
 * `StepConditionTest.allOf_rejects_two_children_over_one_predicate_whatever_names_they_give_it`.
 *
 * Both `event` overloads used to wrap a Kotlin function in a fresh Java `Predicate` on every call, which has identity
 * equality, so two leaves built from the very same Kotlin function value never matched each other even though the
 * equivalent Java leaves, built from one shared `Predicate` instance, do. `wrapPredicate` closes that gap without
 * changing the negative case. Two separately-declared lambdas, even functionally identical ones, still compare
 * unequal.
 */
class SagaFlowKotlinPredicateIdentityTest {

    sealed interface CapEvent {
        val id: String
    }

    data class Opened(override val id: String) : CapEvent
    data class Approved(override val id: String, val score: Int) : CapEvent

    sealed interface CapCommand

    /** A receiver whose bound `::test` reference is what [SagaFlowKotlinPredicateIdentityTest] tests identity over. */
    class ScoreFilter(private val threshold: Int) {
        fun test(approved: Approved): Boolean = approved.score > threshold
    }

    @Test
    fun `two leaves sharing a name and the same function value are accepted under the cap`() {
        val approvedHigh: (Approved) -> Boolean = { it.score > 10 }
        val flowSaga = saga<CapEvent, CapCommand> {
            stepWindow(2)
            startsOn<Opened>()
            correlateAll { it.id }
            step("wait") {
                on(event<Approved>(2, "isBig", approvedHigh), then = transitionTo("wait"))
                on(event<Approved>(1, "isBig", approvedHigh), then = end)
            }
        }

        val opened = flowSaga.evolve(flowSaga.initialState(), SagaInput.event(Opened("c1")))
        val afterOneApproval = flowSaga.evolve(opened, SagaInput.event(Approved("c1", 50)))

        assertThat(flowSaga.isTerminal(afterOneApproval))
            .`as`("both leaves count the same events, so the count-1 leaf fires and crossing them would change nothing")
            .isTrue()
    }

    @Test
    fun `two leaves sharing a name while holding different lambdas are refused the cap`() {
        assertThatThrownBy {
            saga<CapEvent, CapCommand> {
                stepWindow(2)
                startsOn<Opened>()
                correlateAll { it.id }
                step("decide") {
                    on(event<Approved>(1, "big") { it.score > 100 }, then = end)
                    on(event<Approved>(1, "big") { it.score < 0 }, then = end)
                }
            }
        }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("step 'decide'")
            .hasMessageContaining("share the predicate name 'big'")
    }

    @Test
    fun `allOf refuses two unnamed children matching the same event via a shared function value`() {
        val sameTest: (Approved) -> Boolean = { true }

        assertThatThrownBy {
            saga<CapEvent, CapCommand> {
                startsOn<Opened>()
                correlateAll { it.id }
                step("wait") {
                    on(allOf(event<Approved>(2, sameTest), event<Approved>(3, sameTest)), then = end)
                }
            }
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("allOf children 0 and 1")
            .hasMessageContaining("Approved")
    }

    @Test
    fun `allOf refuses two named children over one shared function value whatever names they give it`() {
        val sameTest: (Approved) -> Boolean = { true }

        assertThatThrownBy {
            saga<CapEvent, CapCommand> {
                startsOn<Opened>()
                correlateAll { it.id }
                step("wait") {
                    on(allOf(event<Approved>(2, "twoOfThem", sameTest), event<Approved>(3, "threeOfThem", sameTest)), then = end)
                }
            }
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("allOf children 0 and 1")
            .hasMessageContaining("Approved")
    }

    @Test
    fun `two textually separate bound method references over the same receiver are refused the cap`() {
        val filter = ScoreFilter(10)

        assertThatThrownBy {
            saga<CapEvent, CapCommand> {
                stepWindow(2)
                startsOn<Opened>()
                correlateAll { it.id }
                step("decide") {
                    on(event<Approved>(1, "big", filter::test), then = end)
                    on(event<Approved>(1, "big", filter::test), then = end)
                }
            }
        }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("step 'decide'")
            .hasMessageContaining("share the predicate name 'big'")
    }
}
