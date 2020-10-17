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
package org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.website

import kotlinx.html.*
import kotlinx.html.stream.appendHTML
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.features.game.queries.FindGameByIdQuery
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.GenericApplicationService
import org.occurrent.example.domain.wordguessinggame.readmodel.EndedGameReadModel
import org.occurrent.example.domain.wordguessinggame.readmodel.OngoingGameReadModel
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
class Website(private val applicationService: GenericApplicationService,
              private val findGameByIdQuery: FindGameByIdQuery) {

    @GetMapping
    fun games(response: ServletResponse) {
        response.writer.appendHTML().html {
            body {
                h1 { +"Word Guessing Game" }
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
                    null -> {
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
                    is OngoingGameReadModel -> {
                        h1 { +game.category }
                        val numberOfGuessesLeftForPlayer = game.numberOfGuessesLeftForPlayer(playerId)
                        if (numberOfGuessesLeftForPlayer >= 1) {
                            div { +"Number of guesses left in game: ${game.totalNumberOfGuessesLeft}. You can guess $numberOfGuessesLeftForPlayer more times." }
                            h3 { +game.hint }
                            form(action = "/games/$gameId", encType = FormEncType.multipartFormData, method = FormMethod.post) {
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
                        }
                    }
                    is EndedGameReadModel -> {
                        h1 { +game.category }

                    }
                }
            }
        }.toString()
    }

    @PostMapping
    fun startGame(@RequestParam("gameId") gameId: UUID, @RequestParam("category") category: String,
                  @RequestParam("words") words: String, session: HttpSession): ResponseEntity<*> {
        val playerId = session.getOrGeneratePlayerId()
        val wordsInCategory = words.split('\n').map { it.split(',') }.flatten().map(String::trim).map(::Word)
        val wordsToChooseFrom = WordsToChooseFrom(WordCategory(category.trim()), wordsInCategory)
        applicationService.execute(gameId) { events -> startGame(events, gameId, Timestamp(), playerId, wordsToChooseFrom, MaxNumberOfGuessesPerPlayer, MaxNumberOfGuessesTotal) }
        return ResponseEntity.status(HttpStatus.SEE_OTHER).header(HttpHeaders.LOCATION, gameLocation(gameId)).build<Any>()
    }

    @PostMapping("/{gameId}")
    fun makeGuess(@PathVariable("gameId") gameId: UUID, @RequestParam("word") word: String, session: HttpSession): ResponseEntity<*> {
        val playerId = session.getOrGeneratePlayerId()
        applicationService.execute(gameId) { events -> guessWord(events, Timestamp(), playerId, Word(word)) }
        return ResponseEntity.status(HttpStatus.SEE_OTHER).header(HttpHeaders.LOCATION, gameLocation(gameId)).build<Any>()
    }

    companion object {
        private val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")
        private fun gameLocation(gameId: UUID): String = String.format("/games/%s", gameId)
        private const val PLAYER_ID = "playerId"

        private fun fmt(timestamp: Timestamp): String {
            return DATE_TIME_FORMATTER.format(timestamp.toInstant().atOffset(OffsetDateTime.now().offset))
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