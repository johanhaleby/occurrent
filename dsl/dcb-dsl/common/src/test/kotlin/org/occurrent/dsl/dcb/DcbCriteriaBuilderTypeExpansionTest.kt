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

package org.occurrent.dsl.dcb

import io.cloudevents.CloudEvent
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.DcbCriterion
import org.occurrent.eventstore.api.dcb.Tag

// #912: type and types now go through EventTypeExpansion, the same as every other type-filter derivation in the
// library, and refuse a declared type they cannot fully expand.

sealed interface OrderEvent

data class OrderPlaced(val orderId: String) : OrderEvent

data class OrderCancelled(val orderId: String) : OrderEvent

sealed interface ReopenedEvent

// Sealed above, plain abstract here, so nothing below this class can be found.
abstract class ReopenedBase : ReopenedEvent

sealed interface EnumRoot

enum class EnumWithBodies : EnumRoot {
    A {
        override fun toString() = "a"
    },
    B {
        override fun toString() = "b"
    }
}

sealed interface BodilessEnumRoot

// The same two constants with their behavior moved off the constant bodies, which Kotlin compiles final and stores
// under one CloudEvent type where the bodied version stores one per constant.
enum class EnumWithoutBodies(private val label: String) : BodilessEnumRoot {
    A("a"),
    B("b");

    override fun toString() = label
}

@DisplayName("DcbCriteriaBuilder type expansion")
@DisplayNameGeneration(ReplaceUnderscores::class)
class DcbCriteriaBuilderTypeExpansionTest {

    @Test
    fun type_expands_a_sealed_supertype_to_every_concrete_type_it_permits() {
        val builder = DcbCriteriaBuilder(simpleNameConverter<OrderEvent>())

        val criterion = builder.type(OrderEvent::class.java)

        assertThat(criterion).isEqualTo(DcbCriteria.types(listOf("OrderEvent", "OrderPlaced", "OrderCancelled")))
    }

    @Test
    fun types_expands_each_declared_type_the_same_way_type_does() {
        // The sealed supertype sits in the vararg rest, not first, so a regression that maps rest straight across
        // without expanding it still fails this test.
        val builder = DcbCriteriaBuilder(simpleNameConverter<OrderEvent>())

        val criterion = builder.types(OrderPlaced::class.java, OrderEvent::class.java)

        assertThat(criterion).isEqualTo(DcbCriteria.types(listOf("OrderPlaced", "OrderEvent", "OrderCancelled")))
    }

    @Test
    fun type_refuses_a_declared_type_reopened_below_a_sealed_level() {
        val builder = DcbCriteriaBuilder(simpleNameConverter<ReopenedEvent>())

        assertThatThrownBy { builder.type(ReopenedEvent::class.java) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(ReopenedEvent::class.java.name)
    }

    @Test
    fun types_refuses_a_declared_type_reopened_below_a_sealed_level() {
        val builder = DcbCriteriaBuilder(simpleNameConverter<ReopenedEvent>())

        assertThatThrownBy { builder.types(ReopenedEvent::class.java) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(ReopenedEvent::class.java.name)
    }

    @Test
    fun type_on_a_boundary_seeded_builder_refines_it_with_the_expanded_types_and_keeps_its_tags() {
        val boundary = DcbCriteria.tags(Tag.of("k", "v"))
        val builder = DcbCriteriaBuilder(simpleNameConverter<OrderEvent>(), boundary)

        val criterion = builder.type(OrderEvent::class.java)

        assertThat(criterion).isEqualTo(DcbCriterion(setOf("OrderEvent", "OrderPlaced", "OrderCancelled"), setOf(Tag.of("k", "v"))))
    }

    @Test
    fun type_expands_a_kotlin_enum_with_constant_bodies_declared_directly() {
        // Given: javac seals such an enum implicitly with each constant body as a permitted subclass (JLS 8.9) and
        // Kotlin compiles it as a plain class with no permits clause at all. The expansion reads the enum constants
        // instead, which both compilers answer the same way, so A and B are found either way.
        val builder = DcbCriteriaBuilder(simpleNameConverter<EnumWithBodies>())

        val criterion = builder.type(EnumWithBodies::class.java)

        assertThat(criterion).isEqualTo(DcbCriteria.types(listOf("EnumWithBodies", "A", "B")))
    }

    @Test
    fun type_expands_the_sealed_interface_above_a_kotlin_enum_with_constant_bodies() {
        // Given: the event root is what a caller declares, so this is where the refusal used to reach them rather
        // than on the enum itself.
        val builder = DcbCriteriaBuilder(simpleNameConverter<EnumRoot>())

        val criterion = builder.type(EnumRoot::class.java)

        assertThat(criterion).isEqualTo(DcbCriteria.types(listOf("EnumRoot", "A", "B")))
    }

    @Test
    fun type_accepts_a_kotlin_enum_whose_constants_have_no_bodies() {
        // Given: Kotlin compiles an enum with no constant bodies as a final class, so the walk stops at the enum
        // itself and the constants add nothing beyond it. An enum with bodies and one without are stored under
        // different CloudEvent types, which is the reason to keep both covered.
        val builder = DcbCriteriaBuilder(simpleNameConverter<EnumWithoutBodies>())

        val criterion = builder.type(EnumWithoutBodies::class.java)

        assertThat(criterion).isEqualTo(DcbCriteria.types(listOf("EnumWithoutBodies")))
    }

    @Test
    fun type_refuses_an_array_declared_type_and_names_the_only_way_out() {
        // Given: an array class is already the concrete event type, so "declare the concrete event types instead"
        // is advice nobody can act on here. DcbCriteriaBuilder has no filter override, so the raw type string is
        // the escape the message has to name.
        val builder = DcbCriteriaBuilder(simpleNameConverter<Any>())

        assertThatThrownBy { builder.type(Array<Any>::class.java) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(Array<Any>::class.java.typeName)
            .hasMessageContaining("DcbCriteria.type(String)/types(String, ...)")
            .hasMessageNotContaining("Declare the concrete event types instead")
    }

    @Test
    fun type_accepts_a_sealed_interface_whose_only_member_is_a_kotlin_enum_without_bodies() {
        // Given: a caller declares the sealed event interface far more often than the enum under it, so the root is
        // what has to work. type_expands_the_sealed_interface_above_a_kotlin_enum_with_constant_bodies is
        // the same root over an enum whose constants do have bodies.
        val builder = DcbCriteriaBuilder(simpleNameConverter<BodilessEnumRoot>())

        val criterion = builder.type(BodilessEnumRoot::class.java)

        assertThat(criterion).isEqualTo(DcbCriteria.types(listOf("BodilessEnumRoot", "EnumWithoutBodies")))
    }

    @Test
    fun types_declared_on_each_enum_constant_class_directly_still_works() {
        // Given: each constant body compiles to its own final class, so naming them individually has always worked
        // and still does. It narrows to those two constants, where declaring the enum also names the enum itself.
        val builder = DcbCriteriaBuilder(simpleNameConverter<EnumWithBodies>())

        val criterion = builder.types(EnumWithBodies.A.javaClass, EnumWithBodies.B.javaClass)

        assertThat(criterion).isEqualTo(DcbCriteria.types(listOf("A", "B")))
    }

    @Test
    fun type_on_a_boundary_that_already_excludes_one_of_the_expanded_concrete_types_throws() {
        // Given: excludingTypes("OrderCancelled") on the boundary, and OrderEvent expands to include OrderCancelled.
        // A criterion cannot both include and exclude the same type, so DcbCriterion refuses to build one, the same
        // way it always has. Expansion just makes this overlap reachable from a supertype declaration that never
        // named OrderCancelled itself.
        val boundary = DcbCriteria.tags(Tag.of("k", "v")).excludingTypes("OrderCancelled")
        val builder = DcbCriteriaBuilder(simpleNameConverter<OrderEvent>(), boundary)

        assertThatThrownBy { builder.type(OrderEvent::class.java) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("cannot overlap")
    }
}

/** Maps every type to its simple name, only used to reach the expanded/refused-type paths above, never round-tripped. */
private fun <E : Any> simpleNameConverter(): CloudEventConverter<E> = object : CloudEventConverter<E> {
    override fun toCloudEvent(domainEvent: E): CloudEvent = throw UnsupportedOperationException("not needed for these tests")
    override fun toDomainEvent(cloudEvent: CloudEvent): E = throw UnsupportedOperationException("not needed for these tests")
    override fun getCloudEventType(type: Class<out E>): String = type.simpleName
}
