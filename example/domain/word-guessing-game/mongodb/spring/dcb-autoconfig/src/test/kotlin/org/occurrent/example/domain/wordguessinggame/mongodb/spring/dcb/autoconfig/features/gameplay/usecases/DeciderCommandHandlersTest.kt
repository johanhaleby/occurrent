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
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.cloudevents.OccurrentCloudEventExtension
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries
import org.occurrent.dsl.dcb.blocking.queryForList
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.DcbEventStore
import org.occurrent.example.domain.wordguessinggame.event.*
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
import java.time.Duration
import java.util.*
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

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

    @Autowired
    private lateinit var domainEventQueries: DcbDomainEventQueries<GameEvent>

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
        assertThat(OccurrentCloudEventExtension.getPosition(cloudEvents.single())).isGreaterThan(0)
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
        eventuallySingle<PlayerWasAwardedPointsForGuessingTheRightWord>(GameDcbQueries.pointsCriteria(gameId))

        val cloudEvents = readGameplayCloudEvents(gameId)
        val events = cloudEvents.toDomainEvents()
        assertThat(events).extracting("class").containsExactly(
            GameWasStarted::class.java,
            PlayerGuessedTheRightWord::class.java,
            GameWasWon::class.java
        )
        assertThat(cloudEvents).allSatisfy { cloudEvent ->
            assertThat(DcbCloudEvents.getTags(cloudEvent)).contains(GameDcbTags.game(gameId), GameDcbTags.gameplay(gameId))
            assertThat(OccurrentCloudEventExtension.getPosition(cloudEvent)).isGreaterThan(0)
        }
    }

    // Every game event carries the game tag, so these commands and the word hint policy share a conflict marker and
    // MongoDB rejects one of their transactions with a WriteConflict. Nothing here serializes the writers, so that
    // collision is what the test is for. All four still commit because the append is retried, by the store while it
    // owns the transaction and by MakeGuess itself. Remove both retries and this test fails. See ADR 74.
    @Test
    fun `concurrent guesses on one game all commit while the word hint policy writes to it`() {
        val gameId = UUID.randomUUID()
        startGame(gameId, Date(1), UUID.randomUUID(), wordList())
        val players = List(4) { UUID.randomUUID() }

        val startTogether = CyclicBarrier(players.size)
        val pool = Executors.newFixedThreadPool(players.size)
        try {
            players.mapIndexed { index, playerId ->
                pool.submit {
                    startTogether.await(30, TimeUnit.SECONDS)
                    makeGuess(gameId, Date(2L + index), playerId, Word("wrong"))
                }
            }.forEach { it.get(60, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }

        val guesses = readGameplayCloudEvents(gameId).toDomainEvents().filterIsInstance<PlayerGuessedTheWrongWord>()
        assertThat(guesses).extracting("playerId").containsExactlyInAnyOrderElementsOf(players)

        // Starting the game reveals two characters and a wrong guess reveals a third, so a third reveal is the
        // proof that the policy wrote while the guesses were in flight. Without this the test would still pass
        // with the policy switched off, and then nothing would have contended.
        eventuallyAtLeast<CharacterInWordHintWasRevealed>(GameDcbQueries.wordHintCriteria(gameId), 3)
    }

    private fun readGameplayCloudEvents(gameId: UUID) = dcbEventStore.read(GameDcbQueries.gameplay(gameId)).events()

    private fun List<io.cloudevents.CloudEvent>.toDomainEvents(): List<GameEvent> =
        cloudEventConverter.toDomainEvents(stream()).toList()

    private inline fun <reified E : GameEvent> eventuallySingle(criteria: DcbCriteria): E =
        await().atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(events<E>(criteria)).hasSize(1)
        }.let { events<E>(criteria).single() }

    private inline fun <reified E : GameEvent> eventuallyAtLeast(criteria: DcbCriteria, size: Int) =
        await().atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(events<E>(criteria)).hasSizeGreaterThanOrEqualTo(size)
        }

    private inline fun <reified E : GameEvent> events(criteria: DcbCriteria): List<E> =
        domainEventQueries.queryForList(criteria).filterIsInstance<E>()

    // Every word needs at least five characters. The domain keeps two characters obfuscated, so a wrong guess
    // reveals nothing once only two remain, and the contention test needs the policy to actually write something for
    // the command to contend with. StartGame draws the word at random and WordList requires at least four words, so
    // the draw cannot be removed - keeping every word the same length makes it irrelevant instead. See
    // WordHintCharacterRevelationEdgeCasesTest for the rule.
    private fun wordList(): WordList = WordList(
        WordCategory("test"),
        listOf(Word("apple"), Word("grape"), Word("mango"), Word("peach"))
    )
}
