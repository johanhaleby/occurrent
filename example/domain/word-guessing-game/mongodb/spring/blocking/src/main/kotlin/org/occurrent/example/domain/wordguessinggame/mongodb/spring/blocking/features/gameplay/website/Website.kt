/*
 * Copyright 2020 Johan Haleby
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
package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.website

import kotlinx.html.*
import kotlinx.html.stream.appendHTML
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.usecases.MakeGuess
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.usecases.StartGame
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.views.endedgamesoverview.EndedGamesOverviewQuery
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.views.game.FindGameByIdQuery
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.views.ongoinggamesoverview.OngoingGamesQuery
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.website.Website.Views.gameEndedView
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.website.Website.Views.makeGuessView
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.gameplay.website.Website.Views.newGameView
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.support.loggerFor
import org.occurrent.example.domain.wordguessinggame.readmodel.GameEndedReadModel
import org.occurrent.example.domain.wordguessinggame.readmodel.GameWasWonReadModel
import org.occurrent.example.domain.wordguessinggame.readmodel.OngoingGameReadModel
import org.occurrent.example.domain.wordguessinggame.readmodel.WonGameOverview
import org.occurrent.example.domain.wordguessinggame.writemodel.*
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import javax.servlet.ServletResponse
import javax.servlet.http.HttpSession

@RestController
@RequestMapping(path = ["/games"], produces = [MediaType.TEXT_HTML_VALUE])
class Website(
    private val startGame: StartGame, private val makeGuess: MakeGuess,
    private val findGameByIdQuery: FindGameByIdQuery,
    private val ongoingGamesQuery: OngoingGamesQuery, private val endedGamesOverviewQuery: EndedGamesOverviewQuery,

    ) {
    private val log = loggerFor<Website>()

    @GetMapping
    fun games(response: ServletResponse) {
        val ongoingGames = ongoingGamesQuery.execute(numberOfGames = 10).toList()
        val endedGames = endedGamesOverviewQuery.execute(numberOfGames = 10).toList()

        response.writer.appendHTML().html {
            body {
                h1 { +"Word Guessing Game" }
                br()
                if (ongoingGames.isNotEmpty()) {
                    h3 { +"Ongoing Games" }
                    table {
                        tr {
                            th { +"Category" }
                            th { +"Started At" }
                            th { +"Play" }
                        }
                        ongoingGames.forEach { game ->
                            tr {
                                td { +game.category }
                                td { +game.startedAt.humanReadable() }
                                td {
                                    a("/games/${game.gameId}") { +"Link" }
                                }
                            }
                        }
                    }
                } else {
                    h3 { +"No games are currently ongoing" }
                }

                if (endedGames.isNotEmpty()) {
                    h3 { +"Ended Games" }
                    table {
                        tr {
                            th { +"Category" }
                            th { +"Word To Guess" }
                            th { +"Started At" }
                            th { +"Ended At" }
                            th { +"Result" }
                            th { +"More Info" }
                        }
                        endedGames.forEach { game ->
                            tr {
                                td { +game.category }
                                td { +game.wordToGuess }
                                td { +game.startedAt.humanReadable() }
                                td { +game.endedAt.humanReadable() }
                                td { if (game is WonGameOverview) +"Won by ${game.winnerId}" else +"Game was lost" }
                                td {
                                    a("/games/${game.gameId}") { +"Link" }
                                }
                            }
                        }
                    }
                }

                form(action = "/games/${GameId.randomUUID()}", method = FormMethod.get) {
                    br()
                    button(type = ButtonType.submit) {
                        +"Start new game"
                    }
                }
            }
        }
    }

    @GetMapping("/{game}")
    fun game(@PathVariable("game") gameId: UUID, session: HttpSession): String {
        val playerId = session.getOrGeneratePlayerId()
        val game = findGameByIdQuery.execute(gameId)
        return StringBuilder().appendHTML().html {
            body {
                when (game) {
                    null -> newGameView(gameId)
                    is OngoingGameReadModel -> makeGuessView(game, playerId)
                    is GameEndedReadModel -> gameEndedView(game)
                }
            }
        }.toString()
    }

    @PostMapping
    fun startGame(
        @RequestParam("gameId") gameId: UUID, @RequestParam("category") category: String,
        @RequestParam("words") words: String, session: HttpSession
    ): ResponseEntity<*> {
        val playerId = session.getOrGeneratePlayerId()
        val wordsInCategory = words.split('\n').map { it.split(',') }.flatten().map(String::trim).map(::Word)
        val wordsToChooseFrom = WordList(WordCategory(category.trim()), wordsInCategory)
        startGame(gameId, Timestamp(), playerId, wordsToChooseFrom)
        return ResponseEntity.status(HttpStatus.SEE_OTHER).header(HttpHeaders.LOCATION, gameLocation(gameId)).build<Any>()
    }

    @PostMapping("/{gameId}")
    fun makeGuess(@PathVariable("gameId") gameId: UUID, @RequestParam("word") word: String, session: HttpSession): ResponseEntity<*> {
        val playerId = session.getOrGeneratePlayerId()
        makeGuess(gameId, Timestamp(), playerId, Word(word))
        return ResponseEntity.status(HttpStatus.SEE_OTHER).header(HttpHeaders.LOCATION, gameLocation(gameId)).build<Any>()
    }

    @ExceptionHandler(RuntimeException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    fun handleRuntimeException(e: RuntimeException): String {
        log.info("An error occurred: ${e::class.simpleName} - ${e.message}")
        return StringBuilder().appendHTML().html {
            body {
                h1 { +"Something went wrong" }
                div { +"${e.message}" }
            }
        }.toString()
    }

    private object Views {
        fun BODY.newGameView(gameId: UUID) {
            h1 { +"Start New Game" }
            form(action = "/games", encType = FormEncType.multipartFormData, method = FormMethod.post) {
                textInput(name = "gameId") {
                    hidden = true
                    value = gameId.toString()
                }
                label {
                    htmlFor = "category"
                    +"Category: "
                }
                textInput(name = "category") {
                    id = "category"
                    required = true
                    placeholder = "animals"
                    autoFocus = true
                }
                br()
                label {
                    htmlFor = "words"
                    +"Words: "
                }
                textArea {
                    id = "words"
                    name = "words"
                    required = true
                    placeholder = "mouse, moose, bird, octopus, .."
                }
                br()
                button(type = ButtonType.submit) {
                    +"Start"
                }
            }
        }


        fun BODY.gameEndedView(game: GameEndedReadModel) {
            h1 { +game.category }
            div { +"Game was ${game.status}. Word to guess was \"${game.wordToGuess}\"." }
            br()
            if (game is GameWasWonReadModel) {
                div { +"The word was guessed by ${game.winner} after a total of ${game.totalNumberOfGuesses} guesses by ${game.numberOfPlayersInGame} players." }
                div { +"Player ${game.winner} guessed the word after ${game.numberOfGuessesByWinner} attempts and was awarded ${game.pointsAwardedToWinner} points." }
            } else {
                div { +"No one managed to guess the right word after a total of ${game.totalNumberOfGuesses} guesses. ${game.numberOfPlayersInGame} players attempted to guess the word." }
            }
            returnToStartPageButton()
        }

        fun BODY.makeGuessView(game: OngoingGameReadModel, playerId: PlayerId) {
            h1 { +game.category }
            val numberOfGuessesLeftForPlayer = game.numberOfGuessesLeftForPlayer(playerId)
            if (numberOfGuessesLeftForPlayer >= 1) {
                div { +"Number of guesses left in game: ${game.totalNumberOfGuessesLeft}. You can guess $numberOfGuessesLeftForPlayer more times." }
                h3 { +game.hint }
                form(action = "/games/${game.gameId}", encType = FormEncType.multipartFormData, method = FormMethod.post) {
                    textInput(name = "word") {
                        minLength = Word.MINIMUM_NUMBER_OF_CHARACTERS.toString()
                        maxLength = Word.MAXIMUM_NUMBER_OF_CHARACTERS.toString()
                    }
                    button(type = ButtonType.submit) {
                        +"Make Guess"
                    }
                }
            } else {
                div { +"You've exhausted all your guesses for this game. Try another game or check back later to see who the winner is. Number of guesses left for other players are ${game.totalNumberOfGuessesLeft}." }
                returnToStartPageButton()
            }
        }

        private fun BODY.returnToStartPageButton() {
            form(action = "/games", method = FormMethod.get) {
                br()
                button(type = ButtonType.submit) {
                    +"Return to start page"
                }
            }
        }
    }

    companion object {
        private val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")
        private fun gameLocation(gameId: UUID): String = String.format("/games/%s", gameId)
        private const val PLAYER_ID = "playerId"

        private fun Timestamp.humanReadable(): String {
            return DATE_TIME_FORMATTER.format(toInstant().atOffset(OffsetDateTime.now().offset))
        }
    }

    private fun HttpSession.getOrGeneratePlayerId(): PlayerId =
        if (hasAttribute(PLAYER_ID)) {
            attribute(PLAYER_ID)
        } else {
            PlayerId.randomUUID().also { playerId -> setAttribute(PLAYER_ID, playerId) }
        }
}

private inline fun <reified T> HttpSession.attribute(name: String) = getAttribute(name) as T
private fun HttpSession.hasAttribute(name: String) = getAttribute(name) != null