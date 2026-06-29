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
package org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.gameplay.views

import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.Bootstrap
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.TestBootstrap
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.gameplay.usecases.MakeGuess
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.gameplay.usecases.StartGame
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.gameplay.views.endedgamesoverview.EndedGamesOverviewQuery
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.gameplay.views.game.FindGameByIdQuery
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.dcb.features.gameplay.views.ongoinggamesoverview.OngoingGamesQuery
import org.occurrent.example.domain.wordguessinggame.readmodel.GameWasLostReadModel
import org.occurrent.example.domain.wordguessinggame.readmodel.GameWasWonReadModel
import org.occurrent.example.domain.wordguessinggame.readmodel.OngoingGameReadModel
import org.occurrent.example.domain.wordguessinggame.readmodel.WonGameOverview
import org.occurrent.example.domain.wordguessinggame.writemodel.Word
import org.occurrent.example.domain.wordguessinggame.writemodel.WordCategory
import org.occurrent.example.domain.wordguessinggame.writemodel.WordList
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.Date
import java.util.UUID

@SpringBootTest(classes = [Bootstrap::class, TestBootstrap::class])
class GameplayReadPathsTest {

    @Autowired
    private lateinit var startGame: StartGame

    @Autowired
    private lateinit var makeGuess: MakeGuess

    @Autowired
    private lateinit var findGameById: FindGameByIdQuery

    @Autowired
    private lateinit var ongoingGames: OngoingGamesQuery

    @Autowired
    private lateinit var endedGames: EndedGamesOverviewQuery

    @Test
    fun `find game by id reads all DCB tagged game events`() {
        val gameId = UUID.randomUUID()
        val startedBy = UUID.randomUUID()
        val playerId = UUID.randomUUID()

        assertThat(findGameById.execute(gameId)).isNull()

        startGame(gameId, Date(), startedBy, wordList())

        eventually {
            val game = findGameById.execute(gameId)
            assertThat(game).isInstanceOfSatisfying(OngoingGameReadModel::class.java) {
                assertThat(it.gameId).isEqualTo(gameId)
                assertThat(it.guesses).isEmpty()
                assertThat(it.hint).isNotEqualTo("_____")
            }
        }
        val wordToGuess = (findGameById.execute(gameId) as OngoingGameReadModel).wordToGuess

        makeGuess(gameId, Date(), playerId, Word("zzzz"))

        eventually {
            val game = findGameById.execute(gameId)
            assertThat(game).isInstanceOfSatisfying(OngoingGameReadModel::class.java) {
                assertThat(it.guesses).hasSize(1)
                assertThat(it.guesses.single().word).isEqualTo("zzzz")
            }
        }
        settleSubscriptions()

        makeGuess(gameId, Date(), playerId, Word(wordToGuess))

        eventually {
            val game = findGameById.execute(gameId)
            assertThat(game).isInstanceOfSatisfying(GameWasWonReadModel::class.java) {
                assertThat(it.winner).isEqualTo(playerId)
                assertThat(it.pointsAwardedToWinner).isEqualTo(3)
            }
        }
    }

    @Test
    fun `find game by id returns ended read model after loss`() {
        val gameId = UUID.randomUUID()
        val startedBy = UUID.randomUUID()

        startGame(gameId, Date(), startedBy, wordList())

        repeat(10) { guessNumber ->
            makeGuess(gameId, Date(), UUID.randomUUID(), Word("zzzz"))
            if (guessNumber < 9) {
                eventually {
                    assertThat((findGameById.execute(gameId) as OngoingGameReadModel).guesses).hasSize(guessNumber + 1)
                }
            } else {
                eventually {
                    assertThat(findGameById.execute(gameId)).isInstanceOf(GameWasLostReadModel::class.java)
                }
            }
            settleSubscriptions()
        }

        eventually {
            assertThat(findGameById.execute(gameId)).isInstanceOf(GameWasLostReadModel::class.java)
        }
    }

    @Test
    fun `overview projections are live subscription backed Mongo read models`() {
        val gameId = UUID.randomUUID()
        val startedBy = UUID.randomUUID()
        val playerId = UUID.randomUUID()

        startGame(gameId, Date(), startedBy, wordList())

        eventually {
            assertThat(ongoingGames.execute(20).map { it.gameId }.toList()).contains(gameId)
        }
        val wordToGuess = (findGameById.execute(gameId) as OngoingGameReadModel).wordToGuess
        settleSubscriptions()

        makeGuess(gameId, Date(), playerId, Word(wordToGuess))

        eventually {
            assertThat(ongoingGames.execute(20).map { it.gameId }.toList()).doesNotContain(gameId)
            assertThat(endedGames.execute(20).filterIsInstance<WonGameOverview>().map { it.gameId }.toList()).contains(gameId)
        }
    }

    @Test
    fun `manual bounded reads use DCB query helpers`() {
        assertThat(readSource("game/FindGameByIdQuery.kt"))
            .contains("GameDcbQueries.allGameEvents(gameId)")
            .doesNotContain("DcbQuery.")
        assertThat(readSource("endedgamesoverview/WhenGameIsEndedThenAddGameToEndedGamesOverview.kt"))
            .contains("GameDcbQueries.event<GameWasStarted>(gameId)")
            .doesNotContain("DcbQuery.")
    }

    private fun wordList() = WordList(
        WordCategory("test"),
        listOf(Word("alpha"), Word("bravo"), Word("crane"), Word("delta"))
    )

    private fun eventually(assertion: () -> Unit) {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(assertion)
    }

    private fun settleSubscriptions() {
        await().pollDelay(Duration.ofMillis(300)).until { true }
    }

    private fun readSource(file: String): String =
        Files.readString(Path.of("src/main/kotlin/org/occurrent/example/domain/wordguessinggame/mongodb/spring/dcb/features/gameplay/views/$file"))
}
