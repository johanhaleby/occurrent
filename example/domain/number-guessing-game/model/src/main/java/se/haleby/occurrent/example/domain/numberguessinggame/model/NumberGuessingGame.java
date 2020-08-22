package se.haleby.occurrent.example.domain.numberguessinggame.model;

import se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * The heart of the game. This is the domain model that contains the game logic.
 */
public class NumberGuessingGame {

    public static Stream<GameEvent> startNewGame(UUID gameId, LocalDateTime startDate, UUID whoIsStartingTheGame,
                                                 SecretNumberToGuess secretNumberToGuess, MaxNumberOfGuesses maxNumberOfGuesses) {
        return Stream.of(new NumberGuessingGameWasStarted(UUID.randomUUID(), gameId, startDate, whoIsStartingTheGame, secretNumberToGuess.value, maxNumberOfGuesses.value));
    }

    public static Stream<GameEvent> guessNumber(Stream<GameEvent> events, UUID gameId, LocalDateTime guessingDate, UUID playerId, Guess guess) {
        GameState game = rehydrate(events);

        if (!game.started) {
            throw new GameNotStarted(gameId);
        } else if (game.ended) {
            throw new GameAlreadyEnded(gameId);
        }

        Consumer<List<GameEvent>> exhaustGameAttemptsIfLastGuess = gameEvents -> {
            if (game.isThisTheLastGuess()) {
                gameEvents.add(new GuessingAttemptsExhausted(UUID.randomUUID(), gameId, guessingDate));
                gameEvents.add(new NumberGuessingGameEnded(UUID.randomUUID(), gameId, guessingDate));
            }
        };

        int guessedNumber = guess.value;
        List<GameEvent> newEvents = new ArrayList<>();
        if (game.secretNumberToGuess == guessedNumber) {
            newEvents.add(new PlayerGuessedTheRightNumber(UUID.randomUUID(), gameId, guessingDate, playerId, guessedNumber));
            newEvents.add(new NumberGuessingGameEnded(UUID.randomUUID(), gameId, guessingDate));
        } else if (guessedNumber < game.secretNumberToGuess) {
            newEvents.add(new PlayerGuessedANumberThatWasTooSmall(UUID.randomUUID(), gameId, guessingDate, playerId, guessedNumber));
            exhaustGameAttemptsIfLastGuess.accept(newEvents);
        } else {
            newEvents.add(new PlayerGuessedANumberThatWasTooBig(UUID.randomUUID(), gameId, guessingDate, playerId, guessedNumber));
            exhaustGameAttemptsIfLastGuess.accept(newEvents);
        }

        return newEvents.stream();
    }

    private static GameState rehydrate(Stream<GameEvent> events) {
        return events.collect(GameState::new, (state, gameEvent) -> {
            if (gameEvent instanceof NumberGuessingGameWasStarted) {
                NumberGuessingGameWasStarted numberGuessingGameWasStarted = (NumberGuessingGameWasStarted) gameEvent;
                state.started = true;
                state.secretNumberToGuess = numberGuessingGameWasStarted.secretNumberToGuess();
                state.maxNumberOfGuesses = numberGuessingGameWasStarted.maxNumberOfGuesses();
            } else if (gameEvent instanceof PlayerGuessedANumberThatWasTooSmall || gameEvent instanceof PlayerGuessedANumberThatWasTooBig) {
                state.numberOfGuesses = state.numberOfGuesses + 1;
            } else {
                state.ended = true;
            }
        }, EMPTY);
    }

    private static class GameState {
        private boolean started;
        private int secretNumberToGuess;
        private int maxNumberOfGuesses;
        private int numberOfGuesses;
        private boolean ended;

        boolean isThisTheLastGuess() {
            return numberOfGuesses + 1 == maxNumberOfGuesses;
        }
    }


    private static final BiConsumer<GameState, GameState> EMPTY = (gameState, gameState2) -> {
    };
}