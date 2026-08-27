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
        void exclude_types_does_not_refuse_a_declared_type_reopened_below_a_sealed_level() {
            // Given: type and includeTypes refuse this same declaration. Refusing it here too would break a caller
            // who is already correctly excluding a final class below the reopened level, for no gain, so excludeTypes
            // does not refuse. This particular declaration is the degenerate case, worth calling out rather than
            // reading past: ReopenedEvent is an interface and ReopenedBase, the only level below it, is abstract, so
            // neither is ever stored under its own CloudEvent type and the downward walk finds nothing concrete
            // anywhere. The only excluded type ends up being ReopenedEvent's own name, which no stored event ever
            // carries, so this specific exclusion excludes zero real events. That is not a narrower exclusion than
            // asked for (still safe by that measure), but it is not a working one either, and a caller relying on it
            // to keep a family of events out gets none of the protection they asked for, silently.
            ExecuteFilter<ReopenedEvent> executeFilter = ExecuteFilter.excludeTypes(ReopenedEvent.class);

            // When
            StreamReadFilter filter = executeFilter.resolve(nameGetter());

            // Then
            assertThat(filter).isEqualTo(StreamReadFilter.type(Condition.not(Condition.in(ReopenedEvent.class.getName()))));
        }

        @Test
        void exclude_types_still_refuses_an_array_declared_type_and_names_the_only_way_out() {
            // Given: widening never reaches an array, refused for consistency with type/includeTypes rather than
            // because excluding one would be impossible. That makes "exclude the concrete event types instead" the
            // wrong advice here, since an array class is already the concrete type, so the message has to name the
            // raw filter escape rather than a narrower declaration that does not exist.
            ExecuteFilter<Object[]> executeFilter = ExecuteFilter.excludeTypes(Object[].class);

            // When / Then
            assertThatThrownBy(() -> executeFilter.resolve(nameGetter()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(Object[].class.getTypeName())
                    .hasMessageContaining("ExecuteFilter.from(..)")
                    .hasMessageNotContaining("Exclude the concrete event types instead");
        }

        @Test
        void exclude_types_still_refuses_a_primitive_declared_type() {
            // Given: no event is ever an instance of a primitive type, so nothing here can ever be excluded either.
            ExecuteFilter<Object> executeFilter = ExecuteFilter.excludeTypes(int.class);

            // When / Then
            assertThatThrownBy(() -> executeFilter.resolve(nameGetter()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("primitive type");
        }

        @Test
        void type_class_refuses_an_array_declared_type_and_names_the_only_way_out() {
            // Given: the same reason as the excludeTypes case. An array class is already the concrete type, so
            // pointing at the concrete event types would be advice nobody can act on.
            ExecuteFilter<Object[]> executeFilter = ExecuteFilter.type(Object[].class);

            assertThatThrownBy(() -> executeFilter.resolve(nameGetter()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(Object[].class.getTypeName())
                    .hasMessageContaining("ExecuteFilter.from(..)")
                    .hasMessageNotContaining("Filter on the concrete event types instead");
        }

        @Test
        void type_class_refuses_a_primitive_declared_type() {
            ExecuteFilter<Object> executeFilter = ExecuteFilter.type(int.class);

            assertThatThrownBy(() -> executeFilter.resolve(nameGetter()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("primitive type");
        }

        @Test
        void exclude_types_on_a_directly_declared_non_final_non_sealed_concrete_class_widens_to_only_itself() {
            // Given: unlike a sealed hierarchy reopened partway down, a concrete class declared directly has no
            // level above it for widening to still find. Reflection cannot discover a subclass stored under its
            // own name, so this is the one shape widening cannot close, the exclusion still only removes events of
            // OpenEvent's own CloudEvent type, and a caller relying on it to keep out a subclass is still exposed.
            ExecuteFilter<OpenEvent> executeFilter = ExecuteFilter.excludeTypes(OpenEvent.class);

            // When
            StreamReadFilter filter = executeFilter.resolve(nameGetter());

            // Then
            assertThat(filter).isEqualTo(StreamReadFilter.type(Condition.not(Condition.in(OpenEvent.class.getName()))));
        }

        @Test
        void type_class_accepts_a_java_enum_whose_constants_have_bodies() {
            // Given: the Kotlin side of this fix refuses the same construct, because Kotlin compiles the enum as a
            // class that is neither final nor sealed. The migration guide and the DcbCriteriaBuilder KDoc both tell
            // a Kotlin caller that Java is unaffected, so that claim needs an assertion rather than a reference to
            // JLS 8.9. javac seals the enum implicitly and lists each constant body as a permitted subclass, so the
            // walk finds both and the declaration is accepted.
            ExecuteFilter<JavaEnumWithBodies> executeFilter = ExecuteFilter.type(JavaEnumWithBodies.class);

            // When
            StreamReadFilter filter = executeFilter.resolve(nameGetter());

            // Then
            assertThat(filter).isEqualTo(StreamReadFilter.type(Condition.in(
                    JavaEnumWithBodies.class.getName(),
                    JavaEnumWithBodies.FIRST.getClass().getName(),
                    JavaEnumWithBodies.SECOND.getClass().getName())));
        }

        @Test
        void exclude_types_on_a_reopened_hierarchy_excludes_the_whole_family_under_a_collapsing_getter() {
            // Given: the same declaration as the test above, which excludes zero real events under a getter that
            // maps each type to its own class name. What the exclusion removes is decided by the getter, not by the
            // walk, so a getter of the caller's own that maps the whole hierarchy onto one CloudEvent type string
            // makes this exclusion complete instead. Both outcomes come from one declaration, which is why the
            // documentation cannot claim either one on its own.
            ExecuteFilter<ReopenedEvent> executeFilter = ExecuteFilter.excludeTypes(ReopenedEvent.class);

            // When
            StreamReadFilter filter = executeFilter.resolve(collapsingGetter());

            // Then
            assertThat(filter).isEqualTo(StreamReadFilter.type(Condition.not(Condition.in("collapsed"))));
        }

        @Test
        void include_types_deduplicates_cloud_event_types_a_collapsing_mapper_maps_several_declared_types_onto() {
            // Given: a CloudEventTypeMapper of the caller's own can map a whole hierarchy onto one CloudEvent type
            // string. The expanded classes still differ, but the condition should not repeat the same string.
            ExecuteFilter<DomainEvent> executeFilter = ExecuteFilter.includeTypes(NameDefined.class, NameWasChanged.class);

            // When
            StreamReadFilter filter = executeFilter.resolve(collapsingGetter());

            // Then
            assertThat(filter).isEqualTo(StreamReadFilter.type("collapsed"));
        }
    }

    // Concrete, but neither final nor sealed, so nothing extending it can be found by reflection.
    static class OpenEvent {
    }

    enum JavaEnumWithBodies {
        FIRST {
            @Override
            public String toString() {
                return "first";
            }
        },
        SECOND {
            @Override
            public String toString() {
                return "second";
            }
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

    /** Maps every type to the same CloudEvent type string, the way a caller's own collapsing mapper would. */
    private static <E> CloudEventTypeGetter<E> collapsingGetter() {
        return eventType -> "collapsed";
    }
}
