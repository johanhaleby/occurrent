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

import j2html.TagCreator
import j2html.tags.ContainerTag
import org.occurrent.example.domain.wordguessinggame.mongodb.spring.blocking.infrastructure.GenericApplicationService
import org.occurrent.example.domain.wordguessinggame.writemodel.*
import org.occurrent.example.domain.wordguessinggame.writemodel.game.*
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*

@RestController
@RequestMapping(path = ["/games"], produces = [MediaType.TEXT_HTML_VALUE])
class Website(private val applicationService: GenericApplicationService) {

    @GetMapping
    fun games(): String {
        return TagCreator.body(
                TagCreator.h1("Word Guessing Game"),
                //                generateGameOverview(latestGamesOverview),
                TagCreator.form().withMethod("post").withAction("/games").with(
                        TagCreator.input().withName("gameId").withType("hidden").withValue(UUID.randomUUID().toString()),
                        TagCreator.input().withName("playerId").withType("hidden").withValue(UUID.randomUUID().toString()),
                        TagCreator.br(),
                        TagCreator.button("Start new game").withType("submit")
                )
        ).render()
    }

    @GetMapping("/{game}")
    fun game(@PathVariable("game") gameId: UUID, @RequestParam(value = "playerId", required = false) playerId: UUID?): String {
        val playerIdToUse = playerId ?: UUID.randomUUID()
        //        return whatIsTheStatusOfGame.findFor(gameId)
        //                .map { gameStatus ->
        //                    val body: ContainerTag
        //                    body = if (gameStatus.guesses.isEmpty()) {
        //                        val message = String.format("Game started! It's time to guess a number between %d and %d.", minNumberToGuess, maxNumberToGuess)
        //                        TagCreator.body(TagCreator.h1(message), generateGuessForm(gameId, playerIdToUse, minNumberToGuess, maxNumberToGuess, gameStatus.lastGuess()))
        //                    } else if (gameStatus.isEnded()) {
        //                        TagCreator.body(TagCreator.h1("Game ended"), generateGuessesList(gameStatus), TagCreator.button("Play again").attr("onclick", "window.location.href='/games';"))
        //                    } else {
        //                        val message = java.lang.String.format("You have %d attempts left", gameStatus.numberOfGuessesLeft())
        //                        TagCreator.body(TagCreator.h1(message), generateGuessesList(gameStatus), generateGuessForm(gameId, playerIdToUse, minNumberToGuess, maxNumberToGuess, gameStatus.lastGuess()))
        //                    }
        //                    body
        //                }
        //                .orElseGet { TagCreator.body(TagCreator.h1("Game not found")) }
        //                .render()
        return TODO()
    }

    @PostMapping
    fun startGame(@RequestParam("gameId") gameId: UUID, @RequestParam("playerId") playerId: UUID,
                  @RequestParam("category") category: String, @RequestParam("words") words: String): ResponseEntity<*> {
        val wordsToChooseFrom = WordsToChooseFrom(WordCategory(category.trim()), words.split('\n').map(::Word))
        applicationService.execute(gameId) { events -> org.occurrent.example.domain.wordguessinggame.writemodel.startGame(events, gameId, Timestamp(), playerId, wordsToChooseFrom, MaxNumberOfGuessesPerPlayer, MaxNumberOfGuessesTotal) }
        return ResponseEntity.status(HttpStatus.SEE_OTHER).header(HttpHeaders.LOCATION, gameLocation(gameId, playerId)).build<Any>()
    }

    @PostMapping("/{gameId}")
    fun playGame(@PathVariable("gameId") gameId: UUID, @RequestParam("playerId") playerId: UUID,
                 @RequestParam("word") word: String): ResponseEntity<*> {
        applicationService.execute(gameId) { events -> guessWord(events, Timestamp(), playerId, Word(word)) }
        return ResponseEntity.status(HttpStatus.SEE_OTHER).header(HttpHeaders.LOCATION, gameLocation(gameId, playerId)).build<Any>()
    }

    companion object {
        private val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")
        private fun gameLocation(gameId: UUID, playerId: UUID): String {
            return String.format("/games/%s?playerId=%s", gameId, playerId)
        }

        private fun generateGuessWordForm(gameId: UUID, playerId: UUID, minNumberToGuess: Int, maxNumberToGuess: Int, value: Int): ContainerTag {
            return TagCreator.form().withMethod("post").withAction("/games/$gameId").with(
                    TagCreator.input().withName("playerId").withType("hidden").withValue(playerId.toString()),
                    TagCreator.input().withName("guess").withType("text")
                            .attr("pattern", Word.VALID_WORD_REGEX)
                            .attr("minlength", Word.MINIMUM_NUMBER_OF_CHARACTERS)
                            .attr("maxlength", Word.MAXIMUM_NUMBER_OF_CHARACTERS)
                            .withValue(value.toString()),
                    TagCreator.button("Make guess").withType("submit"))
        }

//        private fun generateGameOverview(latestGamesOverview: LatestGamesOverview): ContainerTag {
//            val trs: Array<ContainerTag> = latestGamesOverview.findOverviewOfLatestGames(10)
//                    .map { game ->
//                        val text: String
//                        text = if (game.state is GameOverview.GameState.Ongoing) {
//                            "Attempts left: " + (game.state as GameOverview.GameState.Ongoing).numberOfAttemptsLeft
//                        } else {
//                            val ended: Ended = game.state as Ended
//                            (if (ended.playerGuessedTheRightNumber) "Won" else "Lost") + " at " + fmt(ended.endedAt)
//                        }
//                        TagCreator.tr(TagCreator.td(fmt(game.startedAt)), td(game.state.getClass().getSimpleName()), TagCreator.td(text))
//                    }
//                    .toArray()
//            if (trs.size == 0) {
//                return TagCreator.div()
//            }
//            val header = TagCreator.tr(TagCreator.th("Started At"), TagCreator.th("State"), TagCreator.th("Info"))
//            val allTableData = arrayOfNulls<ContainerTag>(1 + trs.size)
//            allTableData[0] = header
//            System.arraycopy(trs, 0, allTableData, 1, trs.size)
//            return TagCreator.div(TagCreator.h3("Latest Games"), TagCreator.table(*allTableData))
//        }
//
//        private fun generateGuessesList(gameStatus: GameStatus): ContainerTag {
//            val guesses: Array<ContainerTag> = gameStatus.guesses.stream()
//                    .map { guessAndTime ->
//                        val description: StringBuilder = StringBuilder(fmt(guessAndTime.localDateTime))
//                                .append(" -- ")
//                                .append(guessAndTime.guess)
//                                .append(" was ")
//                        if (guessAndTime.guess === gameStatus.secretNumber) {
//                            description.append("correct. Well done :)")
//                        } else {
//                            description.append(if (guessAndTime.guess < gameStatus.secretNumber) "too small." else "too big.")
//                        }
//                        TagCreator.div(TagCreator.div(description.toString()), TagCreator.br())
//                    }
//                    .toArray()
//            return TagCreator.div(TagCreator.h3("Guesses"), TagCreator.div(*guesses))
//        }

        private fun fmt(localDateTime: LocalDateTime): String {
            return DATE_TIME_FORMATTER.format(localDateTime.atOffset(OffsetDateTime.now().offset))
        }
    }
}