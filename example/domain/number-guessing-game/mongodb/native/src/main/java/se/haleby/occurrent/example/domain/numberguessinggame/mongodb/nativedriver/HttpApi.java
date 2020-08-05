package se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver;

import io.javalin.Javalin;
import j2html.tags.ContainerTag;
import se.haleby.occurrent.example.domain.numberguessinggame.model.Guess;
import se.haleby.occurrent.example.domain.numberguessinggame.model.MaxNumberOfGuesses;
import se.haleby.occurrent.example.domain.numberguessinggame.model.NumberGuessingGame;
import se.haleby.occurrent.example.domain.numberguessinggame.model.SecretNumberToGuess;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.projection.GameStatus;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.projection.WhatIsTheStatusOfGame;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.UUID;

import static j2html.TagCreator.*;
import static java.util.Objects.requireNonNull;

public class HttpApi {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");

    public static void configureRoutes(Javalin javalin, NumberGuessingGameApplicationService as, WhatIsTheStatusOfGame whatIsTheStatusOfGame, int minNumberToGuess, int maxNumberToGuess, MaxNumberOfGuesses maxNumberOfGuesses) {
        javalin.get("/", ctx -> {
            String body = body(
                    h1("Number Guessing Game"),
                    form().withMethod("post").withAction("/games").with(
                            input().withName("gameId").withType("hidden").withValue(UUID.randomUUID().toString()),
                            input().withName("playerId").withType("hidden").withValue(UUID.randomUUID().toString()),
                            button("Start new game").withType("submit")
                    )
            ).render();

            ctx.html(body);
        });

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

    private static ContainerTag generateGuessesList(GameStatus gameStatus) {
        ContainerTag[] guesses = gameStatus.guesses.stream()
                .map(guessAndTime -> {
                    StringBuilder description = new StringBuilder(DATE_TIME_FORMATTER.format(guessAndTime.localDateTime.atZone(ZoneId.systemDefault())))
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
}