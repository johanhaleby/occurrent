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

package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.usecases

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.DcbEventStore
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheWrongWord
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.Bootstrap
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.TestBootstrap
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameDcbQueries
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameDcbTags
import org.occurrent.example.domain.wordguessinggame.writemodel.Word
import org.occurrent.example.domain.wordguessinggame.writemodel.WordCategory
import org.occurrent.example.domain.wordguessinggame.writemodel.WordList
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.util.Date
import java.util.UUID

@SpringBootTest(classes = [Bootstrap::class])
@Import(TestBootstrap::class)
class DeciderCommandHandlersTest {

    @Autowired
    private lateinit var startGame: StartGame

    @Autowired
    private lateinit var makeGuess: MakeGuess

    @Autowired
    private lateinit var dcbEventStore: DcbEventStore

    @Autowired
    private lateinit var cloudEventConverter: CloudEventConverter<GameEvent>

    @Test
    fun `starts game through decider command path`() {
        val gameId = UUID.randomUUID()
        val startedBy = UUID.randomUUID()

        startGame(gameId, Date(1), startedBy, wordList())

        val cloudEvents = readGameplayCloudEvents(gameId)
        val events = cloudEvents.toDomainEvents()
        assertThat(events).hasOnlyElementsOfType(GameWasStarted::class.java)

        val gameWasStarted = events.single() as GameWasStarted
        assertThat(gameWasStarted.gameId).isEqualTo(gameId)
        assertThat(gameWasStarted.startedBy).isEqualTo(startedBy)
        assertThat(gameWasStarted.wordToGuess).isIn(wordList().words.map(Word::value))
        assertThat(DcbCloudEvents.getTags(cloudEvents.single())).containsExactlyInAnyOrder(
            GameDcbTags.game(gameId),
            GameDcbTags.gameplay(gameId)
        )
        assertThat(DcbCloudEvents.getPosition(cloudEvents.single())).isGreaterThan(0)
    }

    @Test
    fun `records wrong and repeated guesses through decider command path`() {
        val gameId = UUID.randomUUID()
        val playerId = UUID.randomUUID()
        startGame(gameId, Date(1), UUID.randomUUID(), wordList())

        makeGuess(gameId, Date(2), playerId, Word("wrong"))
        makeGuess(gameId, Date(3), playerId, Word("wrong"))

        val events = readGameplayCloudEvents(gameId).toDomainEvents()
        assertThat(events).extracting("class").containsExactly(
            GameWasStarted::class.java,
            PlayerGuessedTheWrongWord::class.java,
            PlayerGuessedTheWrongWord::class.java
        )
        assertThat(events.filterIsInstance<PlayerGuessedTheWrongWord>()).extracting("guessedWord")
            .containsExactly("wrong", "wrong")
    }

    @Test
    fun `records right guess and win through decider command path`() {
        val gameId = UUID.randomUUID()
        val playerId = UUID.randomUUID()
        startGame(gameId, Date(1), UUID.randomUUID(), wordList())
        val wordToGuess = readGameplayCloudEvents(gameId).toDomainEvents().filterIsInstance<GameWasStarted>().single().wordToGuess

        makeGuess(gameId, Date(2), playerId, Word(wordToGuess))

        val cloudEvents = readGameplayCloudEvents(gameId)
        val events = cloudEvents.toDomainEvents()
        assertThat(events).extracting("class").containsExactly(
            GameWasStarted::class.java,
            PlayerGuessedTheRightWord::class.java,
            GameWasWon::class.java
        )
        assertThat(cloudEvents).allSatisfy { cloudEvent ->
            assertThat(DcbCloudEvents.getTags(cloudEvent)).contains(GameDcbTags.game(gameId), GameDcbTags.gameplay(gameId))
            assertThat(DcbCloudEvents.getPosition(cloudEvent)).isGreaterThan(0)
        }
    }

    private fun readGameplayCloudEvents(gameId: UUID) = dcbEventStore.read(GameDcbQueries.gameplay(gameId)).events()

    private fun List<io.cloudevents.CloudEvent>.toDomainEvents(): List<GameEvent> =
        cloudEventConverter.toDomainEvents(stream()).toList()

    private fun wordList(): WordList = WordList(
        WordCategory("test"),
        listOf(Word("apple"), Word("banana"), Word("orange"), Word("pear"))
    )
}
