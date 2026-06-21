package org.occurrent.application.service.blocking.dcb

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.DisplayNameGeneration
import org.junit.jupiter.api.DisplayNameGenerator
import org.junit.jupiter.api.Test
import org.occurrent.domain.DomainEvent
import org.occurrent.domain.NameDefined
import org.occurrent.domain.NameWasChanged
import java.time.LocalDateTime
import java.util.UUID
import java.util.stream.Stream

@DisplayName("DcbExecuteOptions extensions")
@DisplayNameGeneration(DisplayNameGenerator.Simple::class)
class DcbExecuteOptionsExtensionsTest {

    @Test
    fun `dcbSideEffect fires only for the matching event type`() {
        // Given
        val observed = mutableListOf<String>()
        val options = dcbSideEffect<DomainEvent, NameDefined> { observed += "defined:${it.name()}" }

        // When
        options.sideEffect()!!.accept(Stream.of(nameDefined("Ada"), nameWasChanged("Lovelace")))

        // Then
        assertThat(observed).containsExactly("defined:Ada")
    }

    @Test
    fun `dcbSideEffect composes two typed policies`() {
        // Given
        val observed = mutableListOf<String>()
        val options = dcbSideEffect<DomainEvent, NameDefined, NameWasChanged>(
            { observed += "defined:${it.name()}" },
            { observed += "changed:${it.name()}" }
        )

        // When
        options.sideEffect()!!.accept(Stream.of(nameDefined("Ada"), nameWasChanged("Lovelace")))

        // Then
        assertThat(observed).containsExactly("defined:Ada", "changed:Lovelace")
    }

    private fun nameDefined(name: String): NameDefined =
        NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "name", name)

    private fun nameWasChanged(name: String): NameWasChanged =
        NameWasChanged(UUID.randomUUID().toString(), LocalDateTime.now(), "name", name)
}
