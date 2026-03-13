package org.occurrent.application.service.blocking

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import org.occurrent.eventstore.api.StreamReadFilter
import java.time.LocalDateTime
import java.util.stream.Stream

@DisplayName("ExecuteOptions extensions")
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class ExecuteOptionsExtensionsTest {

    @Nested
    @DisplayName("when using typed side effects")
    inner class WhenUsingTypedSideEffects {

        @Test
        fun `options helper infers event type for multiple side effects`() {
            // Given
            val observedEvents = mutableListOf<String>()

            // When
            val executeOptions = ExecuteOptions.options<DomainEvent>().sideEffect(
                { event: NameDefined -> observedEvents += "defined:${event.name()}" },
                { event: NameWasChanged -> observedEvents += "changed:${event.name()}" }
            )

            executeOptions.sideEffect()!!.accept(Stream.of(nameDefined("Ada"), nameWasChanged("Lovelace")))

            // Then
            assertThat(observedEvents).containsExactly("defined:Ada", "changed:Lovelace")
        }

        @Test
        fun `direct helper infers event type for multiple side effects`() {
            // Given
            val observedEvents = mutableListOf<String>()

            // When
            val executeOptions = sideEffect<DomainEvent, NameDefined, NameWasChanged>(
                { event: NameDefined -> observedEvents += "defined:${event.name()}" },
                { event: NameWasChanged -> observedEvents += "changed:${event.name()}" }
            )

            executeOptions.sideEffect()!!.accept(Stream.of(nameDefined("Ada"), nameWasChanged("Lovelace")))

            // Then
            assertThat(observedEvents).containsExactly("defined:Ada", "changed:Lovelace")
        }

        @Test
        fun `filter helper composes with side effect without explicit event type`() {
            // Given
            val filter = StreamReadFilter.type(NameDefined::class.java.name)
            val observedEvents = mutableListOf<String>()

            // When
            val executeOptions = org.occurrent.application.service.blocking.filter(filter).sideEffect(
                { event: NameDefined -> observedEvents += event.name() },
                { event: NameWasChanged -> observedEvents += event.name() }
            )

            executeOptions.sideEffect()!!.accept(Stream.of(nameDefined("Ada"), nameWasChanged("Lovelace")))

            // Then
            assertAll(
                { assertThat(executeOptions.filter()).isEqualTo(filter) },
                { assertThat(observedEvents).containsExactly("Ada", "Lovelace") }
            )
        }

        @Test
        fun `filter helper accepts execute filter without explicit event type`() {
            // Given
            val executeFilter = ExecuteFilters.type<NameDefined>()

            // When
            val executeOptions = org.occurrent.application.service.blocking.filter(executeFilter)

            // Then
            assertThat(executeOptions.executeFilter()).isEqualTo(executeFilter)
        }
    }

    @Nested
    @DisplayName("when using typed execute filters")
    inner class WhenUsingTypedExecuteFilters {

        @Test
        fun `type helper resolves cloud event type through execute filter`() {
            // Given

            // When
            val filter = ExecuteFilters.type<NameDefined>().resolve { eventType ->
                when (eventType) {
                    NameDefined::class.java -> "name-defined-v1"
                    else -> eventType.name
                }
            }

            // Then
            assertThat(filter).isEqualTo(StreamReadFilter.type("name-defined-v1"))
        }

        @Test
        fun `include types with kclass varargs resolves all types`() {
            // Given

            // When
            val filter = ExecuteFilters.includeTypes(NameDefined::class, NameWasChanged::class).resolve { eventType ->
                when (eventType) {
                    NameDefined::class.java -> "name-defined-v1"
                    NameWasChanged::class.java -> "name-was-changed-v1"
                    else -> eventType.name
                }
            }

            // Then
            assertThat(filter).isEqualTo(StreamReadFilter.type(org.occurrent.condition.Condition.`in`("name-defined-v1", "name-was-changed-v1")))
        }

        @Test
        fun `exclude types with kclass varargs resolves all types`() {
            // Given

            // When
            val filter = ExecuteFilters.excludeTypes(NameDefined::class, NameWasChanged::class).resolve { eventType ->
                when (eventType) {
                    NameDefined::class.java -> "name-defined-v1"
                    NameWasChanged::class.java -> "name-was-changed-v1"
                    else -> eventType.name
                }
            }

            // Then
            assertThat(filter).isEqualTo(
                StreamReadFilter.type(
                    org.occurrent.condition.Condition.not(
                        org.occurrent.condition.Condition.`in`("name-defined-v1", "name-was-changed-v1")
                    )
                )
            )
        }
    }

    @Nested
    @DisplayName("when using collection based side effects")
    inner class WhenUsingCollectionBasedSideEffects {

        @Test
        fun `side effect on list receives matching events only`() {
            // Given
            var observedNames = emptyList<String>()

            // When
            val executeOptions = ExecuteOptions.options<DomainEvent>()
                .sideEffectOnList { events: List<NameDefined> -> observedNames = events.map(NameDefined::name) }

            executeOptions.sideEffect()!!.accept(Stream.of(nameDefined("Ada"), nameWasChanged("Ignored"), nameDefined("Grace")))

            // Then
            assertThat(observedNames).containsExactly("Ada", "Grace")
        }

        @Test
        fun `side effect on sequence receives matching events only`() {
            // Given
            var observedNames = emptyList<String>()

            // When
            val executeOptions = ExecuteOptions.options<DomainEvent>()
                .sideEffectOnSequence { events: Sequence<NameDefined> -> observedNames = events.map(NameDefined::name).toList() }

            executeOptions.sideEffect()!!.accept(Stream.of(nameDefined("Ada"), nameWasChanged("Ignored"), nameDefined("Grace")))

            // Then
            assertThat(observedNames).containsExactly("Ada", "Grace")
        }
    }

    companion object {
        private val timestamp = LocalDateTime.of(2024, 1, 2, 3, 4)

        private fun nameDefined(name: String) = NameDefined("event-$name", timestamp, "user", name)

        private fun nameWasChanged(name: String) = NameWasChanged("event-$name", timestamp, "user", name)
    }
}
