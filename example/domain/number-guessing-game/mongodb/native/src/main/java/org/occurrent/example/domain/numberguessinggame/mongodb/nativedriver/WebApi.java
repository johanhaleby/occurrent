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

package org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver;

import io.javalin.Javalin;
import j2html.tags.ContainerTag;
import org.occurrent.example.domain.numberguessinggame.model.Guess;
import org.occurrent.example.domain.numberguessinggame.model.MaxNumberOfGuesses;
import org.occurrent.example.domain.numberguessinggame.model.NumberGuessingGame;
import org.occurrent.example.domain.numberguessinggame.model.SecretNumberToGuess;
import org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.gamestatus.GameStatus;
import org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.gamestatus.WhatIsTheStatusOfGame;
import org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.latestgamesoverview.GameOverview;
import org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.latestgamesoverview.LatestGamesOverview;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.UUID;

import static j2html.TagCreator.*;
import static java.util.Objects.requireNonNull;

public class WebApi {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");

    public static void configureRoutes(Javalin javalin, NumberGuessingGameApplicationService as, LatestGamesOverview latestGamesOverview, WhatIsTheStatusOfGame whatIsTheStatusOfGame, int minNumberToGuess, int maxNumberToGuess, MaxNumberOfGuesses maxNumberOfGuesses) {
        javalin.get("/", ctx -> ctx.redirect("/games"));

        javalin.get("/games/:gameId", ctx -> {
            UUID gameId = UUID.fromString(requireNonNull(ctx.pathParam("gameId")));
            UUID playerId = Optional.ofNullable(ctx.queryParam("playerId")).map(UUID::fromString).orElseGet(UUID::randomUUID);

            String html = whatIsTheStatusOfGame.findFor(gameId)
                    .map(gameStatus -> {
                        final ContainerTag body;
                        if (gameStatus.guesses.isEmpty()) {
                            String message = String.format("Game started! It's time to guess a number between %d and %d.", minNumberToGuess, maxNumberToGuess);
                            body = body(h1(message), generateGuessForm(gameId, playerId, minNumberToGuess, maxNumberToGuess, gameStatus.lastGuess()));
                        } else if (gameStatus.isEnded()) {
                            body = body(h1("Game ended"), generateGuessesList(gameStatus), button("Play again").attr("onclick", "window.location.href='/';"));
                        } else {
                            String message = String.format("You have %d attempts left", gameStatus.numberOfGuessesLeft());
                            body = body(h1(message), generateGuessesList(gameStatus), generateGuessForm(gameId, playerId, minNumberToGuess, maxNumberToGuess, gameStatus.lastGuess()));
                        }
                        return body;
                    })
                    .orElseGet(() -> body(h1("Game not found")))
                    .render();

            ctx.html(html);
        });

        javalin.get("/games", ctx -> {
            String body = body(
                    h1("Number Guessing Game"),
                    generateGameOverview(latestGamesOverview),
                    form().withMethod("post").withAction("/games").with(
                            input().withName("gameId").withType("hidden").withValue(UUID.randomUUID().toString()),
                            input().withName("playerId").withType("hidden").withValue(UUID.randomUUID().toString()),
                            br(),
                            button("Start new game").withType("submit")
                    )
            ).render();

            ctx.html(body);
        });

        javalin.post("/games", ctx -> {
            UUID gameId = UUID.fromString(requireNonNull(ctx.formParam("gameId")));
            UUID playerId = UUID.fromString(requireNonNull(ctx.formParam("playerId")));
            as.play(gameId, __ -> NumberGuessingGame.startNewGame(gameId, LocalDateTime.now(), playerId, SecretNumberToGuess.randomBetween(minNumberToGuess, maxNumberToGuess), maxNumberOfGuesses));

            ctx.redirect(gameLocation(gameId, playerId));
        });

        javalin.post("/games/:gameId", ctx -> {
            UUID gameId = UUID.fromString(requireNonNull(ctx.pathParam("gameId")));
            UUID playerId = UUID.fromString(requireNonNull(ctx.formParam("playerId")));
            int guess = Integer.parseInt(requireNonNull(ctx.formParam("guess")));
            as.play(gameId, state -> NumberGuessingGame.guessNumber(state, gameId, LocalDateTime.now(), playerId, new Guess(guess)));

            ctx.redirect(gameLocation(gameId, playerId));
        });
    }

    private static String gameLocation(UUID gameId, UUID playerId) {
        return String.format("/games/%s?playerId=%s", gameId, playerId);
    }

    private static ContainerTag generateGuessForm(UUID gameId, UUID playerId, int minNumberToGuess, int maxNumberToGuess, int value) {
        return form().withMethod("post").withAction("/games/" + gameId.toString()).with(
                input().withName("playerId").withType("hidden").withValue(playerId.toString()),
                input().withName("guess").withType("number").attr("min", minNumberToGuess).attr("max", maxNumberToGuess).withValue(String.valueOf(value)),
                button("Make guess").withType("submit"));
    }

    private static ContainerTag generateGameOverview(LatestGamesOverview latestGamesOverview) {
        ContainerTag[] trs = latestGamesOverview.findOverviewOfLatestGames(10)
                .map(game -> {
                    final String text;
                    if (game.state instanceof GameOverview.GameState.Ongoing) {
                        text = "Attempts left: " + ((GameOverview.GameState.Ongoing) game.state).numberOfAttemptsLeft;
                    } else {
                        GameOverview.GameState.Ended ended = (GameOverview.GameState.Ended) game.state;
                        text = "Ended At: " + fmt(ended.endedAt) + "Won: " + ended.playerGuessedTheRightNumber;
                    }

                    return tr(td(fmt(game.startedAt)), td(game.state.getClass().getSimpleName()), td(text));
                })
                .toArray(ContainerTag[]::new);

        if (trs.length == 0) {
            return div();
        }

        ContainerTag header = tr(th("Started At"), th("State"), th("Info"));
        ContainerTag[] allTableData = new ContainerTag[1 + trs.length];
        allTableData[0] = header;
        System.arraycopy(trs, 0, allTableData, 1, trs.length);
        return div(h3("Latest Games"), table(allTableData));
    }

    private static ContainerTag generateGuessesList(GameStatus gameStatus) {
        ContainerTag[] guesses = gameStatus.guesses.stream()
                .map(guessAndTime -> {
                    StringBuilder description = new StringBuilder(fmt(guessAndTime.localDateTime))
                            .append(" -- ")
                            .append(guessAndTime.guess)
                            .append(" was ");
                    if (guessAndTime.guess == gameStatus.secretNumber) {
                        description.append("correct. Well done :)");
                    } else {
                        description.append(guessAndTime.guess < gameStatus.secretNumber ? "too small." : "too big.");
                    }
                    return div(div(description.toString()), br());
                })
                .toArray(ContainerTag[]::new);
        return div(h3("Guesses"), div(guesses));
    }

    private static String fmt(LocalDateTime localDateTime) {
        return DATE_TIME_FORMATTER.format(localDateTime.atZone(ZoneId.systemDefault()));
    }
}