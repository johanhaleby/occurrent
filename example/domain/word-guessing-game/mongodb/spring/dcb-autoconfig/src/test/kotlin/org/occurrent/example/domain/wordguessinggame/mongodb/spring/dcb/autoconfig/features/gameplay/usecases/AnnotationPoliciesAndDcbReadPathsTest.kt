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
import org.occurrent.cloudevents.OccurrentExtensionGetter
import org.occurrent.dsl.dcb.blocking.DcbDomainEventQueries
import org.occurrent.dsl.dcb.blocking.queryForList
import org.occurrent.eventstore.api.dcb.DcbCloudEvents
import org.occurrent.eventstore.api.dcb.DcbCriteria
import org.occurrent.eventstore.api.dcb.DcbEventStore
import org.occurrent.eventstore.api.dcb.Tag
import org.occurrent.example.domain.wordguessinggame.event.*
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.Bootstrap
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.TestBootstrap
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameDcbQueries
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.dcb.GameDcbTags
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.views.endedgamesoverview.EndedGamesOverviewQuery
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.views.game.FindGameByIdQuery
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.gameplay.views.ongoinggamesoverview.OngoingGamesQuery
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.pointawarding.AwardPointsToPlayerThatGuessedTheRightWord
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.wordhint.RevealCharacterInWordHintAfterPlayerGuessedTheWrongWord
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.autoconfig.features.wordhint.RevealInitialCharactersInWordHintAfterGameIsStarted
import org.occurrent.example.domain.wordguessinggame.readmodel.GameWasWonReadModel
import org.occurrent.example.domain.wordguessinggame.readmodel.OngoingGameReadModel
import org.occurrent.example.domain.wordguessinggame.writemodel.Word
import org.occurrent.example.domain.wordguessinggame.writemodel.WordCategory
import org.occurrent.example.domain.wordguessinggame.writemodel.WordList
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.time.Duration
import java.util.*

@SpringBootTest(classes = [Bootstrap::class])
@Import(TestBootstrap::class)
class AnnotationPoliciesAndDcbReadPathsTest {

    @Autowired
    private lateinit var startGame: StartGame

    @Autowired
    private lateinit var makeGuess: MakeGuess

    @Autowired
    private lateinit var eventStore: DcbEventStore

    @Autowired
    private lateinit var cloudEventConverter: CloudEventConverter<GameEvent>

    @Autowired
    private lateinit var domainEventQueries: DcbDomainEventQueries<GameEvent>

    @Autowired
    private lateinit var findGameById: FindGameByIdQuery

    @Autowired
    private lateinit var ongoingGamesQuery: OngoingGamesQuery

    @Autowired
    private lateinit var endedGamesOverviewQuery: EndedGamesOverviewQuery

    @Autowired
    private lateinit var revealInitialCharacters: RevealInitialCharactersInWordHintAfterGameIsStarted

    @Autowired
    private lateinit var revealCharacterAfterWrongGuess: RevealCharacterInWordHintAfterPlayerGuessedTheWrongWord

    @Autowired
    private lateinit var awardPoints: AwardPointsToPlayerThatGuessedTheRightWord

    @Test
    fun `annotated policies append DCB tagged derived events and read paths use DCB only`() {
        val gameId = UUID.randomUUID()
        val startedBy = UUID.randomUUID()
        val playerId = UUID.randomUUID()

        assertThat(findGameById.execute(gameId)).isNull()

        startGame(gameId, Date(1), startedBy, wordList())
        val gameWasStarted = eventuallySingle<GameWasStarted>(GameDcbQueries.gameplay(gameId))
        val initialHints = eventuallyAtLeast<CharacterInWordHintWasRevealed>(GameDcbQueries.wordHintCriteria(gameId), 1)
        val ongoingGame = eventuallyReadModel<OngoingGameReadModel>(gameId)

        assertThat(ongoingGame.hint.count { it != '_' && it != '-' }).isGreaterThanOrEqualTo(initialHints.size)
        assertThat(ongoingGamesQuery.execute(10).map { it.gameId }.toList()).contains(gameId)
        assertThat(cloudEventTags(GameDcbQueries.wordHintCriteria(gameId)).filter { GameDcbTags.wordHint(gameId) in it })
            .allSatisfy { tags -> assertThat(tags).containsExactlyInAnyOrder(GameDcbTags.game(gameId), GameDcbTags.wordHint(gameId)) }

        makeGuess(gameId, Date(2), playerId, Word("wrong"))
        val wrongGuess = eventuallySingle<PlayerGuessedTheWrongWord>(GameDcbQueries.gameplay(gameId))
        val hintsAfterWrongGuess = eventuallyAtLeast<CharacterInWordHintWasRevealed>(
            GameDcbQueries.wordHintCriteria(gameId),
            initialHints.size + 1
        )
        assertThat(eventuallyReadModel<OngoingGameReadModel>(gameId).guesses).hasSize(1)

        makeGuess(gameId, Date(3), playerId, Word(gameWasStarted.wordToGuess))
        eventuallySingle<PlayerGuessedTheRightWord>(GameDcbQueries.gameplay(gameId))
        eventuallySingle<GameWasWon>(GameDcbQueries.gameplay(gameId))
        val points = eventuallySingle<PlayerWasAwardedPointsForGuessingTheRightWord>(GameDcbQueries.pointsCriteria(gameId))
        val wonGame = eventuallyReadModel<GameWasWonReadModel>(gameId)

        assertThat(points.playerId).isEqualTo(playerId)
        assertThat(points.points).isEqualTo(3)
        assertThat(cloudEvents(GameDcbQueries.pointsCriteria(gameId)).filter { GameDcbTags.points(gameId) in DcbCloudEvents.getTags(it) })
            .allSatisfy { cloudEvent ->
                assertThat(OccurrentCloudEventExtension.getPosition(cloudEvent)).isGreaterThan(0)
                assertThat(OccurrentExtensionGetter.getStreamId(cloudEvent)).startsWith("dcb:partition:")
                assertThat(OccurrentExtensionGetter.getStreamVersion(cloudEvent)).isGreaterThan(0)
            }
        assertThat(wonGame.pointsAwardedToWinner).isEqualTo(3)
        assertThat(ongoingGamesQuery.execute(10).map { it.gameId }.toList()).doesNotContain(gameId)
        assertThat(endedGamesOverviewQuery.execute(10).map { it.gameId }.toList()).contains(gameId)
        assertThat(cloudEventTags(GameDcbQueries.pointsCriteria(gameId)).filter { GameDcbTags.points(gameId) in it })
            .allSatisfy { tags -> assertThat(tags).containsExactlyInAnyOrder(GameDcbTags.game(gameId), GameDcbTags.points(gameId)) }

        revealInitialCharacters(gameWasStarted)
        revealCharacterAfterWrongGuess(wrongGuess)
        awardPoints(PlayerGuessedTheRightWord(UUID.randomUUID(), Date(4), gameId, playerId, gameWasStarted.wordToGuess))

        assertThat(events<CharacterInWordHintWasRevealed>(GameDcbQueries.wordHintCriteria(gameId))).hasSize(hintsAfterWrongGuess.size)
        assertThat(events<PlayerWasAwardedPointsForGuessingTheRightWord>(GameDcbQueries.pointsCriteria(gameId))).hasSize(1)
    }

    // Every word needs at least five characters. The domain keeps two characters obfuscated, so a wrong guess
    // reveals nothing once only two remain, and this test waits for a reveal after a wrong guess - a shorter word
    // would make that wait unsatisfiable. See WordHintCharacterRevelationEdgeCasesTest for the rule.
    private fun wordList() = WordList(
        WordCategory("test"),
        listOf(Word("alpha"), Word("bravo"), Word("crane"), Word("delta"))
    )

    private inline fun <reified E : GameEvent> eventuallySingle(criteria: DcbCriteria): E =
        await().atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(events<E>(criteria)).hasSize(1)
        }.let { events<E>(criteria).single() }

    private inline fun <reified E : GameEvent> eventuallyAtLeast(criteria: DcbCriteria, size: Int): List<E> =
        await().atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(events<E>(criteria)).hasSizeGreaterThanOrEqualTo(size)
        }.let { events<E>(criteria) }

    private inline fun <reified T> eventuallyReadModel(gameId: UUID): T =
        await().atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(findGameById.execute(gameId)).isInstanceOf(T::class.java)
        }.let { findGameById.execute(gameId) as T }

    private inline fun <reified E : GameEvent> events(criteria: DcbCriteria): List<E> =
        domainEventQueries.queryForList(criteria).filterIsInstance<E>()

    private fun cloudEvents(criteria: DcbCriteria): List<io.cloudevents.CloudEvent> =
        eventStore.read(criteria).events()

    private fun cloudEventTags(criteria: DcbCriteria): List<Set<Tag>> =
        cloudEvents(criteria).map(DcbCloudEvents::getTags)
}
