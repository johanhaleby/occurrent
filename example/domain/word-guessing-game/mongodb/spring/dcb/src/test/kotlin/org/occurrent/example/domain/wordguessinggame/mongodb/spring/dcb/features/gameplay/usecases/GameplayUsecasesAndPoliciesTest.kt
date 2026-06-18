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
package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.gameplay.usecases

import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.occurrent.application.converter.CloudEventConverter
import org.occurrent.cloudevents.OccurrentExtensionGetter
import org.occurrent.dsl.dcb.blocking.queryForList
import org.occurrent.dsl.query.blocking.DomainEventQueries
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.DcbEventStore
import org.occurrent.example.domain.wordguessinggame.event.CharacterInWordHintWasRevealed
import org.occurrent.example.domain.wordguessinggame.event.GameEvent
import org.occurrent.example.domain.wordguessinggame.event.GameWasStarted
import org.occurrent.example.domain.wordguessinggame.event.GameWasWon
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheRightWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerGuessedTheWrongWord
import org.occurrent.example.domain.wordguessinggame.event.PlayerWasAwardedPointsForGuessingTheRightWord
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.Bootstrap
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.TestBootstrap
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.dcb.GameDcbQueries
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.dcb.GameDcbTags
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.pointawarding.AwardPointsToPlayerThatGuessedTheRightWord
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.wordhint.RevealCharacterInWordHintAfterPlayerGuessedTheWrongWord
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.wordhint.RevealInitialCharactersInWordHintAfterGameIsStarted
import org.occurrent.example.domain.wordguessinggame.writemodel.Word
import org.occurrent.example.domain.wordguessinggame.writemodel.WordCategory
import org.occurrent.example.domain.wordguessinggame.writemodel.WordList
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.time.Duration
import java.util.Date
import java.util.UUID

@SpringBootTest(classes = [Bootstrap::class, TestBootstrap::class])
class GameplayUsecasesAndPoliciesTest {

    @Autowired
    private lateinit var startGame: StartGame

    @Autowired
    private lateinit var makeGuess: MakeGuess

    @Autowired
    private lateinit var eventStore: DcbEventStore

    @Autowired
    private lateinit var cloudEventConverter: CloudEventConverter<GameEvent>

    @Autowired
    private lateinit var domainEventQueries: DomainEventQueries<GameEvent>

    @Autowired
    private lateinit var revealInitialCharacters: RevealInitialCharactersInWordHintAfterGameIsStarted

    @Autowired
    private lateinit var revealCharacterAfterWrongGuess: RevealCharacterInWordHintAfterPlayerGuessedTheWrongWord

    @Autowired
    private lateinit var awardPoints: AwardPointsToPlayerThatGuessedTheRightWord

    @Test
    fun `start game wrong guess and right guess write gameplay events and derived policy events with DCB tags`() {
        val gameId = UUID.randomUUID()
        val startedBy = UUID.randomUUID()
        val playerId = UUID.randomUUID()

        startGame(gameId, Date(), startedBy, wordList())
        val gameWasStarted = eventuallySingle<GameWasStarted>(GameDcbQueries.gameplay(gameId))
        assertThat(cloudEvents(GameDcbQueries.gameplay(gameId)))
            .allSatisfy { cloudEvent ->
                assertThat(DcbCloudEvents.getTags(cloudEvent)).contains(GameDcbTags.game(gameId), GameDcbTags.gameplay(gameId))
                assertThat(DcbCloudEvents.getPosition(cloudEvent)).isGreaterThan(0)
                assertThat(OccurrentExtensionGetter.getStreamId(cloudEvent)).startsWith("dcb:partition:")
                assertThat(OccurrentExtensionGetter.getStreamVersion(cloudEvent)).isGreaterThan(0)
            }

        val initialHints = eventuallyAtLeast<CharacterInWordHintWasRevealed>(GameDcbQueries.wordHintDecisionContext(gameId), 1)
        assertThat(cloudEventTags(GameDcbQueries.wordHintDecisionContext(gameId)).filter { GameDcbTags.wordHint(gameId) in it })
            .allSatisfy { tags -> assertThat(tags).contains(GameDcbTags.game(gameId), GameDcbTags.wordHint(gameId)) }

        makeGuess(gameId, Date(), playerId, Word("zzzz"))
        val wrongGuess = eventuallySingle<PlayerGuessedTheWrongWord>(GameDcbQueries.gameplay(gameId))
        val hintsAfterWrongGuess = eventuallyAtLeast<CharacterInWordHintWasRevealed>(
            GameDcbQueries.wordHintDecisionContext(gameId),
            initialHints.size + 1
        )

        makeGuess(gameId, Date(), playerId, Word(gameWasStarted.wordToGuess))
        eventuallySingle<PlayerGuessedTheRightWord>(GameDcbQueries.gameplay(gameId))
        val gameWasWon = eventuallySingle<GameWasWon>(GameDcbQueries.gameplay(gameId))
        val points = eventuallySingle<PlayerWasAwardedPointsForGuessingTheRightWord>(GameDcbQueries.pointsDecisionContext(gameId))
        assertThat(points.playerId).isEqualTo(playerId)
        assertThat(points.points).isEqualTo(3)
        assertThat(cloudEventTags(GameDcbQueries.pointsDecisionContext(gameId)).filter { GameDcbTags.points(gameId) in it })
            .allSatisfy { tags -> assertThat(tags).contains(GameDcbTags.game(gameId), GameDcbTags.points(gameId)) }

        revealInitialCharacters(gameWasStarted)
        revealCharacterAfterWrongGuess(wrongGuess)
        awardPoints(PlayerGuessedTheRightWord(UUID.randomUUID(), Date(), gameId, playerId, gameWasStarted.wordToGuess))
        awardPoints(PlayerGuessedTheRightWord(UUID.randomUUID(), Date(), gameId, playerId, gameWasStarted.wordToGuess))

        assertThat(events<CharacterInWordHintWasRevealed>(GameDcbQueries.wordHintDecisionContext(gameId))).hasSize(hintsAfterWrongGuess.size)
        assertThat(events<PlayerWasAwardedPointsForGuessingTheRightWord>(GameDcbQueries.pointsDecisionContext(gameId))).hasSize(1)
        assertThat(gameWasWon.winnerId).isEqualTo(playerId)
    }

    private fun wordList() = WordList(
        WordCategory("test"),
        listOf(Word("alpha"), Word("bravo"), Word("crane"), Word("delta"))
    )

    private inline fun <reified E : GameEvent> eventuallySingle(query: org.occurrent.eventstore.api.dcb.DcbQuery): E =
        await().atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(events<E>(query)).hasSize(1)
        }.let { events<E>(query).single() }

    private inline fun <reified E : GameEvent> eventuallyAtLeast(query: org.occurrent.eventstore.api.dcb.DcbQuery, size: Int): List<E> =
        await().atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(events<E>(query)).hasSizeGreaterThanOrEqualTo(size)
        }.let { events<E>(query) }

    private inline fun <reified E : GameEvent> events(query: org.occurrent.eventstore.api.dcb.DcbQuery): List<E> =
        domainEventQueries.queryForList(query).filterIsInstance<E>()

    private fun cloudEvents(query: org.occurrent.eventstore.api.dcb.DcbQuery): List<io.cloudevents.CloudEvent> =
        eventStore.read(query).events()

    private fun cloudEventTags(query: org.occurrent.eventstore.api.dcb.DcbQuery): List<Set<String>> =
        cloudEvents(query).map(DcbCloudEvents::getTags)
}
