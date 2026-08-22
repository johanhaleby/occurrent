package org.occurrent.application.service.blocking;

import org.junit.jupiter.api.*;
import org.occurrent.application.converter.typemapper.CloudEventTypeGetter;
import org.occurrent.application.service.ExecuteFilter;
import org.occurrent.condition.Condition;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.StreamReadFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("ExecuteFilter")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ExecuteFilterTest {

    @Nested
    @DisplayName("when resolving type based filters")
    class When_resolving_type_based_filters {

        @Test
        void type_class_uses_cloud_event_type_getter_instead_of_class_name() {
            // Given
            ExecuteFilter<DomainEvent> executeFilter = ExecuteFilter.type(NameDefined.class);

            // When
            StreamReadFilter filter = executeFilter.resolve(typeGetter());

            // Then
            assertThat(filter).isEqualTo(StreamReadFilter.type("name-defined-v1"));
        }

        @Test
        void include_types_resolves_to_in_condition() {
            // Given
            ExecuteFilter<DomainEvent> executeFilter = ExecuteFilter.includeTypes(NameDefined.class, NameWasChanged.class);

            // When
            StreamReadFilter filter = executeFilter.resolve(typeGetter());

            // Then
            assertThat(filter).isEqualTo(StreamReadFilter.type(Condition.in("name-defined-v1", "name-was-changed-v1")));
        }

        @Test
        void exclude_types_resolves_to_not_in_condition() {
            // Given
            ExecuteFilter<DomainEvent> executeFilter = ExecuteFilter.excludeTypes(NameDefined.class, NameWasChanged.class);

            // When
            StreamReadFilter filter = executeFilter.resolve(typeGetter());

            // Then
            assertThat(filter).isEqualTo(StreamReadFilter.type(Condition.not(Condition.in("name-defined-v1", "name-was-changed-v1"))));
        }
    }

    // #912: type, includeTypes and excludeTypes now go through EventTypeExpansion like every other type-filter
    // derivation in the library, and the two directions take opposite treatments on a declared type they cannot
    // fully expand.
    @Nested
    @DisplayName("when a declared type does not resolve to a single CloudEvent type")
    class When_a_declared_type_does_not_resolve_to_a_single_cloud_event_type {

        @Test
        void type_class_expands_a_sealed_supertype_to_every_concrete_type_it_permits() {
            // Given
            ExecuteFilter<DomainEvent> executeFilter = ExecuteFilter.type(DomainEvent.class);

            // When
            StreamReadFilter filter = executeFilter.resolve(typeGetter());

            // Then
            assertThat(filter).isEqualTo(StreamReadFilter.type(Condition.in(DomainEvent.class.getName(), "name-defined-v1", "name-was-changed-v1")));
        }

        @Test
        void type_class_refuses_a_declared_type_reopened_below_a_sealed_level() {
            // Given
            ExecuteFilter<ReopenedEvent> executeFilter = ExecuteFilter.type(ReopenedEvent.class);

            // When / Then
            assertThatThrownBy(() -> executeFilter.resolve(nameGetter()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(ReopenedEvent.class.getName());
        }

        @Test
        void include_types_refuses_a_declared_type_reopened_below_a_sealed_level() {
            // Given
            ExecuteFilter<ReopenedEvent> executeFilter = ExecuteFilter.includeTypes(ReopenedEvent.class);

            // When / Then
            assertThatThrownBy(() -> executeFilter.resolve(nameGetter()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(ReopenedEvent.class.getName());
        }

        @Test
        void exclude_types_widens_an_excluded_sealed_supertype_to_every_concrete_subtype_it_covers() {
            // Given: excludeTypes(DomainEvent.class) used to exclude only the supertype's own CloudEvent type, which
            // no stored event ever carries, so nothing was really excluded. It must widen to both concrete subtypes.
            ExecuteFilter<DomainEvent> executeFilter = ExecuteFilter.excludeTypes(DomainEvent.class);

            // When
            StreamReadFilter filter = executeFilter.resolve(typeGetter());

            // Then
            assertThat(filter).isEqualTo(StreamReadFilter.type(Condition.not(Condition.in(DomainEvent.class.getName(), "name-defined-v1", "name-was-changed-v1"))));
        }

        @Test
        void exclude_types_widens_instead_of_refusing_a_declared_type_reopened_below_a_sealed_level() {
            // Given: type and includeTypes refuse this same declaration. Refusing it here too would break a caller
            // who is already correctly excluding a final class below the reopened level, for no gain, so excludeTypes
            // widens to what can be found instead.
            ExecuteFilter<ReopenedEvent> executeFilter = ExecuteFilter.excludeTypes(ReopenedEvent.class);

            // When
            StreamReadFilter filter = executeFilter.resolve(nameGetter());

            // Then
            assertThat(filter).isEqualTo(StreamReadFilter.type(Condition.not(Condition.in(ReopenedEvent.class.getName()))));
        }

        @Test
        void exclude_types_still_refuses_an_array_declared_type() {
            // Given: widening never reaches an array, since no event is ever an instance of one.
            ExecuteFilter<Object[]> executeFilter = ExecuteFilter.excludeTypes(Object[].class);

            // When / Then
            assertThatThrownBy(() -> executeFilter.resolve(nameGetter()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(Object[].class.getTypeName());
        }
    }

    sealed interface ReopenedEvent permits ReopenedBase {
    }

    // Sealed above, plain abstract here, so nothing below this class can be found.
    abstract static non-sealed class ReopenedBase implements ReopenedEvent {
    }

    private static CloudEventTypeGetter<DomainEvent> typeGetter() {
        return eventType -> {
            if (eventType.equals(NameDefined.class)) {
                return "name-defined-v1";
            } else if (eventType.equals(NameWasChanged.class)) {
                return "name-was-changed-v1";
            }
            return eventType.getName();
        };
    }

    /** Maps every type to its own name, only used where the mapped value does not matter to the assertion. */
    private static <E> CloudEventTypeGetter<E> nameGetter() {
        return Class::getName;
    }
}
