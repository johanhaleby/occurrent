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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    // Dispatch accepts this for a handler on PartlyOpenEvent, and no walk of permitted subclasses can reach it.
    static final class ConcreteBelowReopenedBase extends ReopenedBase {
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

    static final class ConcreteBelowDeepReopened extends DeepReopened {
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

    static final class ConcreteBelowReopenedAbstractSubclass extends ReopenedAbstractSubclass {
    }

    /**
     * Every concrete event type in this file. A handler declared on some supertype accepts each of these that is
     * assignable to it, because dispatch matches with {@code isInstance}, so this is what a derived filter is measured
     * against.
     */
    private static final List<Class<?>> EVERY_CONCRETE_TYPE = List.of(
            OrderPlaced.class, PaymentReserved.class, PaymentFailed.class,
            ConcreteOpenEvent.class, SealedLeaf.class, ConcreteBelowReopenedBase.class,
            InstantiableBase.class, SealedSubclass.class,
            PartlyOpenInstantiableBase.class, ReopenedSubclassHolder.class, ConcreteBelowReopenedAbstractSubclass.class,
            ExtensibleEvent.class, SubclassOfExtensibleEvent.class,
            DiamondShared.class, DeepConcrete.class, ConcreteBelowDeepReopened.class);

    enum Outcome {
        /** The filter names every type dispatch would accept. */
        NAMES_EVERY_DISPATCHED_TYPE,
        /** The hierarchy cannot be enumerated, so it is refused rather than turned into a filter that misses events. */
        REFUSED,
        /** The one exemption. Accepted, and dispatch accepts subclasses the filter does not name. */
        EXEMPT_AND_MISSES_SUBCLASSES
    }

    static List<Arguments> hierarchyShapes() {
        return List.of(
                Arguments.of("a record", OrderPlaced.class, Outcome.NAMES_EVERY_DISPATCHED_TYPE),
                Arguments.of("a sealed interface", OrderEvent.class, Outcome.NAMES_EVERY_DISPATCHED_TYPE),
                Arguments.of("a nested sealed interface", PaymentEvent.class, Outcome.NAMES_EVERY_DISPATCHED_TYPE),
                Arguments.of("a sealed class that can be instantiated", InstantiableBase.class, Outcome.NAMES_EVERY_DISPATCHED_TYPE),
                Arguments.of("a diamond of sealed interfaces", DiamondTop.class, Outcome.NAMES_EVERY_DISPATCHED_TYPE),
                Arguments.of("an interface that is not sealed", OpenEvent.class, Outcome.REFUSED),
                Arguments.of("an abstract class that is not sealed", OpenBase.class, Outcome.REFUSED),
                Arguments.of("a sealed interface reopened by an abstract class", PartlyOpenEvent.class, Outcome.REFUSED),
                Arguments.of("a sealed interface reopened by a concrete class", ReopenedByConcreteEvent.class, Outcome.REFUSED),
                Arguments.of("an instantiable sealed root reopened below it", PartlyOpenInstantiableBase.class, Outcome.REFUSED),
                Arguments.of("a sealed hierarchy reopened two levels down", DeepTop.class, Outcome.REFUSED),
                Arguments.of("an array", OrderPlaced[].class, Outcome.REFUSED),
                Arguments.of("a concrete class that is not final", ExtensibleEvent.class, Outcome.EXEMPT_AND_MISSES_SUBCLASSES));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("hierarchyShapes")
    void the_filter_names_every_type_dispatch_would_accept(String shape, Class<?> declaredType, Outcome expected) {
        Set<Class<?>> dispatchAccepts = EVERY_CONCRETE_TYPE.stream()
                .filter(declaredType::isAssignableFrom)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<Class<?>> named;
        try {
            named = new LinkedHashSet<>(expandOne(declaredType));
        } catch (IllegalArgumentException refused) {
            assertThat(expected).as("%s was refused", shape).isEqualTo(Outcome.REFUSED);
            return;
        }

        assertThat(expected).as("%s was accepted", shape).isNotEqualTo(Outcome.REFUSED);
        if (expected == Outcome.NAMES_EVERY_DISPATCHED_TYPE) {
            assertThat(named).as("%s names every type dispatch accepts", shape).containsAll(dispatchAccepts);
        } else {
            assertThat(named).as("%s names itself", shape).contains(declaredType);
            assertThat(dispatchAccepts).as("%s is the known hole, dispatch accepts more than the filter names", shape)
                    .isNotEmpty()
                    .anySatisfy(accepted -> assertThat(named).doesNotContain(accepted));
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<Class<?>> expandOne(Class<?> declaredType) {
        return (Set<Class<?>>) (Set<?>) EventTypeExpansion.expand(Set.of((Class<Object>) declaredType), REFUSAL);
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
    void a_sealed_class_that_can_be_instantiated_is_refused_when_a_branch_below_it_is_reopened() {
        // Being instantiable makes the root storable, it says nothing about the hierarchy below it. Accepting this used
        // to drop ConcreteBelowReopenedAbstractSubclass from the filter while dispatch still accepted it.
        assertThatThrownBy(() -> EventTypeExpansion.expand(Set.of(PartlyOpenInstantiableBase.class), REFUSAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PartlyOpenInstantiableBase.class.getName());
    }

    @Test
    void a_concrete_subclass_of_a_refused_base_is_accepted_on_its_own() {
        Set<Class<? extends OpenEvent>> expanded = EventTypeExpansion.expand(Set.of(ConcreteOpenEvent.class), REFUSAL);

        assertThat(expanded).containsExactly(ConcreteOpenEvent.class);
    }
}
