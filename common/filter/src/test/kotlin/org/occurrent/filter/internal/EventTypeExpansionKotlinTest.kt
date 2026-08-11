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

package org.occurrent.filter.internal

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test

sealed interface KotlinOrderEvent

data class KotlinOrderPlaced(val orderId: String) : KotlinOrderEvent

data class KotlinPaymentReserved(val orderId: String) : KotlinOrderEvent

sealed interface KotlinPartlyOpenEvent

data class KotlinSealedSubtype(val orderId: String) : KotlinPartlyOpenEvent

// Kotlin lets a sealed interface permit a plain abstract class, and anything extending this one is invisible to the walk.
abstract class KotlinReopenedBase : KotlinPartlyOpenEvent

@DisplayName("EventTypeExpansion over Kotlin types")
@DisplayNameGeneration(ReplaceUnderscores::class)
class EventTypeExpansionKotlinTest {

    @Test
    fun `a Kotlin sealed interface expands into its data classes`() {
        val expanded = EventTypeExpansion.expand<KotlinOrderEvent>(setOf(KotlinOrderEvent::class.java)) { type -> IllegalArgumentException("${type.name} cannot be expanded") }

        assertThat(expanded).containsExactlyInAnyOrder(
            KotlinOrderEvent::class.java,
            KotlinOrderPlaced::class.java,
            KotlinPaymentReserved::class.java
        )
    }

    @Test
    fun `a Kotlin sealed interface permitting a plain abstract class is refused`() {
        assertThatThrownBy { EventTypeExpansion.expand<KotlinPartlyOpenEvent>(setOf(KotlinPartlyOpenEvent::class.java)) { type -> IllegalArgumentException("${type.name} cannot be expanded") } }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(KotlinPartlyOpenEvent::class.java.name)
            .hasMessageContaining("cannot be expanded")
    }

    @Test
    fun `a Kotlin data class expands to itself`() {
        val expanded = EventTypeExpansion.expand<KotlinOrderEvent>(setOf(KotlinOrderPlaced::class.java)) { type -> IllegalArgumentException("${type.name} cannot be expanded") }

        assertThat(expanded).containsExactly(KotlinOrderPlaced::class.java)
    }
}
