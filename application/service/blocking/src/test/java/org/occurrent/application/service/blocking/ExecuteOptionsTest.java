package org.occurrent.application.service.blocking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.StreamReadFilter;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@DisplayName("ExecuteOptions")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ExecuteOptionsTest {

    @Nested
    @DisplayName("when building options from java")
    class When_building_options_from_java {

        @Test
        void options_filter_retains_filter() {
            // Given
            var filter = StreamReadFilter.type(NameDefined.class.getName());

            // When
            var executeOptions = ExecuteOptions.<DomainEvent>options().filter(filter);

            // Then
            assertThat(executeOptions.filter()).isEqualTo(filter);
        }

        @Test
        void options_side_effect_retains_side_effect() {
            // Given
            var observedNames = new ArrayList<String>();

            // When
            var executeOptions = ExecuteOptions.<DomainEvent>options()
                    .sideEffect(events -> events.map(DomainEvent::name).forEach(observedNames::add));

            executeOptions.sideEffect().accept(Stream.of(nameDefined("Ada"), nameWasChanged("Lovelace")));

            // Then
            assertThat(observedNames).containsExactly("Ada", "Lovelace");
        }

        @Test
        void options_filter_and_side_effect_can_be_composed() {
            // Given
            var filter = StreamReadFilter.type(NameDefined.class.getName());
            var observedNames = new ArrayList<String>();

            // When
            var executeOptions = ExecuteOptions.<DomainEvent>options()
                    .filter(filter)
                    .sideEffect(events -> events.map(DomainEvent::name).forEach(observedNames::add));

            executeOptions.sideEffect().accept(Stream.of(nameDefined("Ada"), nameWasChanged("Lovelace")));

            // Then
            assertAll(
                    () -> assertThat(executeOptions.filter()).isEqualTo(filter),
                    () -> assertThat(observedNames).containsExactly("Ada", "Lovelace")
            );
        }
    }

    private static NameDefined nameDefined(String name) {
        return new NameDefined("event-" + name, LocalDateTime.of(2024, 1, 2, 3, 4), "user", name);
    }

    private static NameWasChanged nameWasChanged(String name) {
        return new NameWasChanged("event-" + name, LocalDateTime.of(2024, 1, 2, 3, 4), "user", name);
    }
}
