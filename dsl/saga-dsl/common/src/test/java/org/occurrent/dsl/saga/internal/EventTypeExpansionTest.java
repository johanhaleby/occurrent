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

package org.occurrent.dsl.saga.internal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("EventTypeExpansion")
@DisplayNameGeneration(ReplaceUnderscores.class)
class EventTypeExpansionTest {

    sealed interface OrderEvent permits OrderPlaced, PaymentEvent {
    }

    record OrderPlaced() implements OrderEvent {
    }

    sealed interface PaymentEvent extends OrderEvent permits PaymentReserved, PaymentFailed {
    }

    record PaymentReserved() implements PaymentEvent {
    }

    record PaymentFailed() implements PaymentEvent {
    }

    interface OpenEvent {
    }

    static abstract class OpenBase implements OpenEvent {
    }

    static final class ConcreteOpenEvent extends OpenBase {
    }

    sealed interface PartlyOpenEvent permits SealedLeaf, ReopenedBase {
    }

    record SealedLeaf() implements PartlyOpenEvent {
    }

    // Sealed above, plain abstract here, so nothing below this class can be found.
    static abstract non-sealed class ReopenedBase implements PartlyOpenEvent {
    }

    sealed static class InstantiableBase permits SealedSubclass {
    }

    static final class SealedSubclass extends InstantiableBase {
    }

    sealed static class PartlyOpenInstantiableBase permits ReopenedSubclassHolder, ReopenedAbstractSubclass {
    }

    static final class ReopenedSubclassHolder extends PartlyOpenInstantiableBase {
    }

    static abstract non-sealed class ReopenedAbstractSubclass extends PartlyOpenInstantiableBase {
    }

    @Test
    void a_sealed_interface_expands_into_the_concrete_types_it_permits() {
        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(Set.of(OrderEvent.class));

        assertThat(expanded).containsExactlyInAnyOrder(OrderEvent.class, OrderPlaced.class, PaymentReserved.class,
                PaymentFailed.class);
    }

    @Test
    void an_intermediate_sealed_interface_is_left_out_because_no_event_is_stored_under_its_name() {
        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(Set.of(OrderEvent.class));

        assertThat(expanded).doesNotContain(PaymentEvent.class);
    }

    @Test
    void the_declared_type_stays_in_the_expansion() {
        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(Set.of(OrderEvent.class));

        assertThat(expanded).contains(OrderEvent.class);
    }

    @Test
    void a_nested_sealed_interface_expands_through_every_level() {
        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(Set.of(PaymentEvent.class));

        assertThat(expanded).containsExactlyInAnyOrder(PaymentEvent.class, PaymentReserved.class, PaymentFailed.class);
    }

    @Test
    void a_concrete_type_expands_to_itself() {
        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(Set.of(OrderPlaced.class));

        assertThat(expanded).containsExactly(OrderPlaced.class);
    }

    @Test
    void a_sealed_class_that_can_be_instantiated_keeps_itself_and_gains_its_subclasses() {
        Set<Class<? extends InstantiableBase>> expanded = EventTypeExpansion.expand(Set.of(InstantiableBase.class));

        assertThat(expanded).containsExactlyInAnyOrder(InstantiableBase.class, SealedSubclass.class);
    }

    @Test
    void several_declared_types_expand_independently() {
        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(Set.of(OrderPlaced.class, PaymentEvent.class));

        assertThat(expanded).containsExactlyInAnyOrder(OrderPlaced.class, PaymentEvent.class, PaymentReserved.class,
                PaymentFailed.class);
    }

    @Test
    void an_interface_that_is_not_sealed_is_refused() {
        assertThatThrownBy(() -> EventTypeExpansion.expand(Set.of(OpenEvent.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OpenEvent.class.getName())
                .hasMessageContaining("Declare the concrete event types instead");
    }

    @Test
    void an_abstract_class_that_is_not_sealed_is_refused() {
        assertThatThrownBy(() -> EventTypeExpansion.expand(Set.of(OpenBase.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OpenBase.class.getName());
    }

    @Test
    void a_sealed_interface_permitting_a_plain_abstract_class_is_refused() {
        assertThatThrownBy(() -> EventTypeExpansion.expand(Set.of(PartlyOpenEvent.class)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(PartlyOpenEvent.class.getName());
    }

    @Test
    void a_sealed_class_that_can_be_instantiated_is_kept_even_when_a_branch_below_it_is_reopened() {
        Set<Class<? extends PartlyOpenInstantiableBase>> expanded =
                EventTypeExpansion.expand(Set.of(PartlyOpenInstantiableBase.class));

        // Not refused, because events do carry this class's own name, so it never receives nothing.
        assertThat(expanded).contains(PartlyOpenInstantiableBase.class, ReopenedSubclassHolder.class);
    }

    @Test
    void a_concrete_subclass_of_a_refused_base_is_accepted_on_its_own() {
        Set<Class<? extends OpenEvent>> expanded = EventTypeExpansion.expand(Set.of(ConcreteOpenEvent.class));

        assertThat(expanded).containsExactly(ConcreteOpenEvent.class);
    }
}
