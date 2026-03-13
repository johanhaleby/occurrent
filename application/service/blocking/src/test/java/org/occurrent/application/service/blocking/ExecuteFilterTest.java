package org.occurrent.application.service.blocking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.CloudEventTypeGetter;
import org.occurrent.condition.Condition;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.StreamReadFilter;

import static org.assertj.core.api.Assertions.assertThat;

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
}
