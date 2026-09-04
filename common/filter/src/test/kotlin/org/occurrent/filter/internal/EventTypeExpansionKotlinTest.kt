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

// A Kotlin class is final unless it says open, so open is how a Kotlin caller reaches the shape #753 is about.
open class KotlinOpenOrderPlaced(val orderId: String)

class KotlinSpecialOrderPlaced(orderId: String) : KotlinOpenOrderPlaced(orderId)

// Kotlin compiles an enum whose constants have bodies without the implicit sealing javac gives the same construct
// (JLS 8.9). KotlinPaymentEvent declares an abstract member, so every constant needs a body and the enum class is
// abstract. KotlinShipmentEvent declares an open member, so one constant has a body and the enum class is concrete.
sealed interface KotlinEnumEvent

enum class KotlinPaymentEvent : KotlinEnumEvent {
    Reserved { override fun label() = "reserved" },
    Settled { override fun label() = "settled" };

    abstract fun label(): String
}

enum class KotlinShipmentEvent {
    Dispatched { override fun label() = "dispatched" },
    Delivered;

    open fun label() = "delivered"
}

// The flag lives outside the enum because touching the enum to read it would initialize it.
object KotlinEnumInitialized {
    @JvmField
    @Volatile
    var bodiless = false
}

// No constant body, so Kotlin compiles the enum final and the walk stops at it.
enum class KotlinBodilessPaymentEvent(private val label: String) {
    Reserved("reserved"),
    Settled("settled");

    init {
        KotlinEnumInitialized.bodiless = true
    }

    override fun toString() = label
}

enum class KotlinEnumWithoutConstants

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
    fun `a Kotlin open class is refused`() {
        assertThatThrownBy { EventTypeExpansion.expand<Any>(setOf(KotlinOpenOrderPlaced::class.java)) { type -> IllegalArgumentException("${type.name} cannot be expanded") } }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(KotlinOpenOrderPlaced::class.java.name)
    }

    @Test
    fun `a Kotlin class extending an open class is accepted on its own`() {
        val expanded = EventTypeExpansion.expand<Any>(setOf(KotlinSpecialOrderPlaced::class.java)) { type -> IllegalArgumentException("${type.name} cannot be expanded") }

        assertThat(expanded).containsExactly(KotlinSpecialOrderPlaced::class.java)
    }

    @Test
    fun `a Kotlin data class expands to itself`() {
        val expanded = EventTypeExpansion.expand<KotlinOrderEvent>(setOf(KotlinOrderPlaced::class.java)) { type -> IllegalArgumentException("${type.name} cannot be expanded") }

        assertThat(expanded).containsExactly(KotlinOrderPlaced::class.java)
    }

    @Test
    fun `a Kotlin enum with constant bodies expands into its constant classes`() {
        // Kotlin marks neither the enum nor a permits clause, so the constants are reachable only through the enum
        // constants themselves. The enum class is abstract here, so no event is ever stored under its own name.
        val expanded = EventTypeExpansion.expand<Any>(setOf(KotlinPaymentEvent::class.java)) { type -> IllegalArgumentException("${type.name} cannot be expanded") }

        assertThat(expanded).containsExactlyInAnyOrder(
            KotlinPaymentEvent::class.java,
            KotlinPaymentEvent.Reserved.javaClass,
            KotlinPaymentEvent.Settled.javaClass
        )
    }

    @Test
    fun `a Kotlin enum with bodies on only some constants expands into the enum and the bodied constants`() {
        // Delivered has no body, so it is an instance of the enum class itself and that is what it is stored under,
        // which is why the enum class belongs in the expansion alongside Dispatched's own class.
        val expanded = EventTypeExpansion.expand<Any>(setOf(KotlinShipmentEvent::class.java)) { type -> IllegalArgumentException("${type.name} cannot be expanded") }

        assertThat(expanded).containsExactlyInAnyOrder(
            KotlinShipmentEvent::class.java,
            KotlinShipmentEvent.Dispatched.javaClass
        )
        assertThat(KotlinShipmentEvent.Delivered.javaClass).isEqualTo(KotlinShipmentEvent::class.java)
    }

    @Test
    fun `a sealed interface above a Kotlin enum with constant bodies expands`() {
        // The refusal reaches a caller here rather than on the enum, since the event root is what they declare.
        val expanded = EventTypeExpansion.expand<KotlinEnumEvent>(setOf(KotlinEnumEvent::class.java)) { type -> IllegalArgumentException("${type.name} cannot be expanded") }

        assertThat(expanded).containsExactlyInAnyOrder(
            KotlinEnumEvent::class.java,
            KotlinPaymentEvent.Reserved.javaClass,
            KotlinPaymentEvent.Settled.javaClass
        )
    }

    @Test
    fun `expandWhatCanBeFound finds the constant classes of a Kotlin enum with constant bodies`() {
        // expandWhatCanBeFound never refused the enum, it quietly found only the enum class, so an exclusion derived
        // from it excluded nothing at all under a mapper that stores each class under its own name.
        val expanded = EventTypeExpansion.expandWhatCanBeFound<Any>(setOf(KotlinPaymentEvent::class.java)) { type -> IllegalArgumentException("${type.name} cannot be expanded") }

        assertThat(expanded).containsExactlyInAnyOrder(
            KotlinPaymentEvent::class.java,
            KotlinPaymentEvent.Reserved.javaClass,
            KotlinPaymentEvent.Settled.javaClass
        )
    }

    @Test
    fun `a Kotlin enum with no constant bodies still expands to itself`() {
        val expanded = EventTypeExpansion.expand<Any>(setOf(KotlinBodilessPaymentEvent::class.java)) { type -> IllegalArgumentException("${type.name} cannot be expanded") }

        assertThat(expanded).containsExactly(KotlinBodilessPaymentEvent::class.java)
    }

    @Test
    fun `a Kotlin enum constant class declared directly still expands to itself`() {
        // The way out the refusal message offered before the enum expanded, and it has to keep working.
        val expanded = EventTypeExpansion.expand<Any>(setOf(KotlinPaymentEvent.Reserved.javaClass)) { type -> IllegalArgumentException("${type.name} cannot be expanded") }

        assertThat(expanded).containsExactly(KotlinPaymentEvent.Reserved.javaClass)
    }

    @Test
    fun `a Kotlin enum with no constants expands to itself`() {
        // Kotlin compiles an enum with no constants final, so it is answered the same way any other final class is.
        val expanded = EventTypeExpansion.expand<Any>(setOf(KotlinEnumWithoutConstants::class.java)) { type -> IllegalArgumentException("${type.name} cannot be expanded") }

        assertThat(expanded).containsExactly(KotlinEnumWithoutConstants::class.java)
    }

    @Test
    fun `expanding a Kotlin enum with no constant bodies does not initialize it`() {
        // Reading an enum through getEnumConstants runs its static initializer, and a bodiless enum is final, so the
        // walk stops at it and never reads a value. Only an enum whose constants have bodies is read that way.
        val expanded = EventTypeExpansion.expand<Any>(setOf(KotlinBodilessPaymentEvent::class.java)) { type -> IllegalArgumentException("${type.name} cannot be expanded") }
        val initializedByTheWalk = KotlinEnumInitialized.bodiless

        assertThat(initializedByTheWalk).isFalse()
        assertThat(expanded).containsExactly(KotlinBodilessPaymentEvent::class.java)
    }
}
