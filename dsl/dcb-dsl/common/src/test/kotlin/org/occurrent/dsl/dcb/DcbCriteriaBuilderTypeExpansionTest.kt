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
    fun type_refuses_a_kotlin_enum_with_constant_bodies_declared_directly() {
        // Given: unlike javac, which makes such an enum implicitly sealed with each constant body as a permitted
        // subclass (JLS 8.9), Kotlin compiles EnumWithBodies itself as a plain non-final, non-sealed class, so
        // EventTypeExpansion can see it is concrete but cannot see A and B as its permitted subclasses. This is a
        // shared EventTypeExpansion limitation, verified here, not something a change in this class can fix.
        val builder = DcbCriteriaBuilder(simpleNameConverter<EnumWithBodies>())

        assertThatThrownBy { builder.type(EnumWithBodies::class.java) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(EnumWithBodies::class.java.name)
    }

    @Test
    fun type_refuses_the_sealed_interface_a_kotlin_enum_with_constant_bodies_reopens() {
        val builder = DcbCriteriaBuilder(simpleNameConverter<EnumRoot>())

        assertThatThrownBy { builder.type(EnumRoot::class.java) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(EnumRoot::class.java.name)
    }

    @Test
    fun types_declared_on_each_enum_constant_class_directly_still_works() {
        // Given: the "declare the concrete event types instead" remedy the refusal message offers is real here.
        // Each constant body compiles to its own final class, so naming them individually works around the gap.
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
