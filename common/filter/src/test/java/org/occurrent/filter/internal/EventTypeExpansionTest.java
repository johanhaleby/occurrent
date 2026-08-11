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

package org.occurrent.filter.internal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("EventTypeExpansion")
@DisplayNameGeneration(ReplaceUnderscores.class)
class EventTypeExpansionTest {

    // The helper reports which type it could not expand and leaves the wording to the caller, so the tests supply the
    // simplest possible caller.
    private static final Function<Class<?>, RuntimeException> REFUSAL =
            type -> new IllegalArgumentException(type.getName() + " cannot be expanded");

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

    sealed interface DiamondTop permits DiamondLeft, DiamondRight {
    }

    sealed interface DiamondLeft extends DiamondTop permits DiamondShared {
    }

    sealed interface DiamondRight extends DiamondTop permits DiamondShared {
    }

    record DiamondShared() implements DiamondLeft, DiamondRight {
    }

    sealed interface DeepTop permits DeepMiddle {
    }

    sealed interface DeepMiddle extends DeepTop permits DeepConcrete, DeepReopened {
    }

    record DeepConcrete() implements DeepMiddle {
    }

    static abstract non-sealed class DeepReopened implements DeepMiddle {
    }

    sealed interface ReopenedByConcreteEvent permits ExtensibleEvent {
    }

    // Concrete and extensible, so anything below it is invisible to the walk.
    static non-sealed class ExtensibleEvent implements ReopenedByConcreteEvent {
    }

    static final class SubclassOfExtensibleEvent extends ExtensibleEvent {
    }

    sealed static class PartlyOpenInstantiableBase permits ReopenedSubclassHolder, ReopenedAbstractSubclass {
    }

    static final class ReopenedSubclassHolder extends PartlyOpenInstantiableBase {
    }

    static abstract non-sealed class ReopenedAbstractSubclass extends PartlyOpenInstantiableBase {
    }

    @Test
    void a_sealed_interface_expands_into_the_concrete_types_it_permits() {
        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(Set.of(OrderEvent.class), REFUSAL);

        assertThat(expanded).containsExactlyInAnyOrder(OrderEvent.class, OrderPlaced.class, PaymentReserved.class,
                PaymentFailed.class);
    }

    @Test
    void an_intermediate_sealed_interface_is_left_out_because_no_event_is_stored_under_its_name() {
        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(Set.of(OrderEvent.class), REFUSAL);

        assertThat(expanded).doesNotContain(PaymentEvent.class);
    }

    @Test
    void the_declared_type_stays_in_the_expansion() {
        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(Set.of(OrderEvent.class), REFUSAL);

        assertThat(expanded).contains(OrderEvent.class);
    }

    @Test
    void a_nested_sealed_interface_expands_through_every_level() {
        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(Set.of(PaymentEvent.class), REFUSAL);

        assertThat(expanded).containsExactlyInAnyOrder(PaymentEvent.class, PaymentReserved.class, PaymentFailed.class);
    }

    @Test
    void a_concrete_type_expands_to_itself() {
        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(Set.of(OrderPlaced.class), REFUSAL);

        assertThat(expanded).containsExactly(OrderPlaced.class);
    }

    @Test
    void a_sealed_class_that_can_be_instantiated_keeps_itself_and_gains_its_subclasses() {
        Set<Class<? extends InstantiableBase>> expanded = EventTypeExpansion.expand(Set.of(InstantiableBase.class), REFUSAL);

        assertThat(expanded).containsExactlyInAnyOrder(InstantiableBase.class, SealedSubclass.class);
    }

    @Test
    void several_declared_types_expand_independently() {
        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(Set.of(OrderPlaced.class, PaymentEvent.class), REFUSAL);

        assertThat(expanded).containsExactlyInAnyOrder(OrderPlaced.class, PaymentEvent.class, PaymentReserved.class,
                PaymentFailed.class);
    }

    @Test
    void a_concrete_type_reachable_through_two_sealed_interfaces_is_collected_once() {
        Set<Class<? extends DiamondTop>> expanded = EventTypeExpansion.expand(Set.of(DiamondTop.class), REFUSAL);

        assertThat(expanded).containsExactlyInAnyOrder(DiamondTop.class, DiamondShared.class);
    }

    @Test
    void a_reopened_branch_is_refused_through_two_levels_of_sealing() {
        assertThatThrownBy(() -> EventTypeExpansion.expand(Set.of(DeepTop.class), REFUSAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DeepTop.class.getName());
    }

    @Test
    void iteration_order_puts_each_declared_type_before_what_it_expanded_into() {
        Set<Class<? extends OrderEvent>> declared = new LinkedHashSet<>();
        declared.add(OrderPlaced.class);
        declared.add(PaymentEvent.class);

        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(declared, REFUSAL);

        assertThat(expanded).containsExactly(OrderPlaced.class, PaymentEvent.class, PaymentReserved.class, PaymentFailed.class);
    }

    @Test
    void an_array_type_is_refused_because_no_event_is_stored_under_it() {
        assertThatThrownBy(() -> EventTypeExpansion.expand(Set.of(OrderPlaced[].class), REFUSAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(OrderPlaced[].class.getName());
    }

    @Test
    void concrete_types_of_a_sealed_type_leaves_the_declared_type_out() {
        assertThat(EventTypeExpansion.concreteTypesOf(OrderEvent.class, REFUSAL))
                .containsExactly(OrderPlaced.class, PaymentReserved.class, PaymentFailed.class);
    }

    @Test
    void concrete_types_of_a_concrete_type_is_the_type_itself() {
        assertThat(EventTypeExpansion.concreteTypesOf(OrderPlaced.class, REFUSAL)).containsExactly(OrderPlaced.class);
    }

    @Test
    void no_declared_types_expands_to_nothing() {
        assertThat(EventTypeExpansion.expand(Set.<Class<? extends OrderEvent>>of(), REFUSAL)).isEmpty();
    }

    @Test
    void the_expansion_cannot_be_modified() {
        Set<Class<? extends OrderEvent>> expanded = EventTypeExpansion.expand(Set.of(OrderPlaced.class), REFUSAL);

        assertThatThrownBy(() -> expanded.add(PaymentReserved.class)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void an_interface_that_is_not_sealed_is_refused() {
        assertThatThrownBy(() -> EventTypeExpansion.expand(Set.of(OpenEvent.class), REFUSAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(OpenEvent.class.getName())
                .hasMessageContaining("cannot be expanded");
    }

    @Test
    void an_abstract_class_that_is_not_sealed_is_refused() {
        assertThatThrownBy(() -> EventTypeExpansion.expand(Set.of(OpenBase.class), REFUSAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(OpenBase.class.getName());
    }

    @Test
    void a_sealed_interface_permitting_a_plain_abstract_class_is_refused() {
        assertThatThrownBy(() -> EventTypeExpansion.expand(Set.of(PartlyOpenEvent.class), REFUSAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PartlyOpenEvent.class.getName());
    }

    @Test
    void a_sealed_interface_permitting_an_extensible_concrete_class_is_refused() {
        // ExtensibleEvent is stored under its own name, so the walk used to call the branch complete and drop
        // SubclassOfExtensibleEvent from the filter without a word.
        assertThatThrownBy(() -> EventTypeExpansion.expand(Set.of(ReopenedByConcreteEvent.class), REFUSAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ReopenedByConcreteEvent.class.getName());
    }

    @Test
    void a_sealed_class_that_can_be_instantiated_is_kept_even_when_a_branch_below_it_is_reopened() {
        Set<Class<? extends PartlyOpenInstantiableBase>> expanded =
                EventTypeExpansion.expand(Set.of(PartlyOpenInstantiableBase.class), REFUSAL);

        // Not refused, because events do carry this class's own name, so it never receives nothing.
        assertThat(expanded).contains(PartlyOpenInstantiableBase.class, ReopenedSubclassHolder.class);
    }

    @Test
    void a_concrete_subclass_of_a_refused_base_is_accepted_on_its_own() {
        Set<Class<? extends OpenEvent>> expanded = EventTypeExpansion.expand(Set.of(ConcreteOpenEvent.class), REFUSAL);

        assertThat(expanded).containsExactly(ConcreteOpenEvent.class);
    }
}
