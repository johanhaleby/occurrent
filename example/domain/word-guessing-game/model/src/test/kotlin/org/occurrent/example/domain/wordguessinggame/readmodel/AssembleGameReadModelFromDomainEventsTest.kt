package org.occurrent.example.domain.wordguessinggame.readmodel

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.writemodel.GameId
import org.occurrent.example.domain.wordguessinggame.writemodel.PlayerId
import org.occurrent.example.domain.wordguessinggame.writemodel.Timestamp
import java.util.*

class AssembleGameReadModelFromDomainEventsTest {

    @Test
    fun `doesn't obfuscate spaces when game is started`() {
        // Given
        val gameWasStarted = GameWasStarted(UUID.randomUUID(), Timestamp(), GameId.randomUUID(), PlayerId.randomUUID(), "Category", "One Two", 2, 4)

        // When
        val readModel = AssembleGameReadModelFromDomainEvents().applyEvent(gameWasStarted).gameReadModel as OngoingGameReadModel

        // Then
        assertThat(readModel.hint).isEqualTo("___ ___")
    }
}