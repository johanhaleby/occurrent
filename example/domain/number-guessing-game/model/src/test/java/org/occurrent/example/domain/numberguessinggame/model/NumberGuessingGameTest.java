package org.occurrent.example.domain.numberguessinggame.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.*;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;

class NumberGuessingGameTest {

    @Nested
    @DisplayName("when new game is started")
    class WhenNewGameIsStarted {

        @Test
        void number_guessing_game_was_started_event_is_returned() {
            // Given
            UUID gameId = UUID.randomUUID();
            LocalDateTime now = LocalDateTime.now();
            UUID gameStarter = UUID.randomUUID();
            SecretNumberToGuess secretNumberToGuess = new SecretNumberToGuess(50);
            MaxNumberOfGuesses maxNumberOfGuesses = new MaxNumberOfGuesses(5);

            // When
            List<GameEvent> events = NumberGuessingGame.startNewGame(gameId, now, gameStarter, secretNumberToGuess, maxNumberOfGuesses).collect(Collectors.toList());

            // Then
            assertThat(events).hasSize(1);
            NumberGuessingGameWasStarted started = (NumberGuessingGameWasStarted) events.get(0);
            assertAll(
                    () -> assertThat(started.eventId()).isNotNull(),
                    () -> assertThat(started.gameId()).isEqualTo(gameId),
                    () -> assertThat(started.timestamp()).isEqualTo(now),
                    () -> assertThat(started.startedBy()).isEqualTo(gameStarter),
                    () -> assertThat(started.secretNumberToGuess()).isEqualTo(secretNumberToGuess.value),
                    () -> assertThat(started.maxNumberOfGuesses()).isEqualTo(maxNumberOfGuesses.value)
            );
        }
    }

    @Nested
    @DisplayName("when game is ongoing")
    class WhenNewGameIsOngoing {

        private final UUID gameId = UUID.randomUUID();
        private Stream<GameEvent> state;

        @Nested
        @DisplayName("and player guessed the right number")
        class AndPlayerGuessedTheRightNumber {

            @BeforeEach
            void state_is_initialized() {
                state = Stream.of(new NumberGuessingGameWasStarted(UUID.randomUUID(), gameId, LocalDateTime.now(), UUID.randomUUID(), 10, 1));
            }

            @Test
            void then_number_guessing_game_ended_event_is_returned() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                UUID playerId = UUID.randomUUID();
                Guess guess = new Guess(10);

                // When
                Stream<GameEvent> events = NumberGuessingGame.guessNumber(state, gameId, now, playerId, guess);

                // Then
                NumberGuessingGameEnded event = find(NumberGuessingGameEnded.class, events);
                assertAll(
                        () -> assertThat(event.eventId()).isNotNull(),
                        () -> assertThat(event.gameId()).isEqualTo(gameId),
                        () -> assertThat(event.timestamp()).isEqualTo(now)
                );
            }

            @Test
            void then_player_guessed_the_right_number_event_is_returned() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                UUID playerId = UUID.randomUUID();
                Guess guess = new Guess(10);

                // When
                Stream<GameEvent> events = NumberGuessingGame.guessNumber(state, gameId, now, playerId, guess);

                // Then
                PlayerGuessedTheRightNumber event = find(PlayerGuessedTheRightNumber.class, events);
                assertAll(
                        () -> assertThat(event.eventId()).isNotNull(),
                        () -> assertThat(event.gameId()).isEqualTo(gameId),
                        () -> assertThat(event.timestamp()).isEqualTo(now)
                );
            }
        }

        @Nested
        @DisplayName("and player guessed the wrong number")
        class AndPlayerGuessedTheWrongNumber {

            @BeforeEach
            void state_is_initialized() {
                state = Stream.of(new NumberGuessingGameWasStarted(UUID.randomUUID(), gameId, LocalDateTime.now(), UUID.randomUUID(), 10, 3));
            }

            @Nested
            @DisplayName("and it's not the last move")
            class AndItIsNotTheLastMove {

                @Test
                void then_player_guessed_a_number_that_was_too_big_event_is_returned_when_guess_was_too_big() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    UUID playerId = UUID.randomUUID();
                    Guess guess = new Guess(12);

                    // When
                    List<GameEvent> newEvents = NumberGuessingGame.guessNumber(state, gameId, now, playerId, guess).collect(Collectors.toList());

                    // Then
                    assertThat(newEvents).hasSize(1);
                    PlayerGuessedANumberThatWasTooBig event = (PlayerGuessedANumberThatWasTooBig) newEvents.get(0);
                    assertAll(
                            () -> assertThat(event.eventId()).isNotNull(),
                            () -> assertThat(event.gameId()).isEqualTo(gameId),
                            () -> assertThat(event.timestamp()).isEqualTo(now),
                            () -> assertThat(event.playerId()).isEqualTo(playerId),
                            () -> assertThat(event.guessedNumber()).isEqualTo(guess.value)
                    );
                }

                @Test
                void then_player_guessed_a_number_that_was_too_small_event_is_returned_when_guess_was_too_small() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    UUID playerId = UUID.randomUUID();
                    Guess guess = new Guess(8);

                    // When
                    List<GameEvent> newEvents = NumberGuessingGame.guessNumber(state, gameId, now, playerId, guess).collect(Collectors.toList());

                    // Then
                    assertThat(newEvents).hasSize(1);
                    PlayerGuessedANumberThatWasTooSmall event = (PlayerGuessedANumberThatWasTooSmall) newEvents.get(0);
                    assertAll(
                            () -> assertThat(event.eventId()).isNotNull(),
                            () -> assertThat(event.gameId()).isEqualTo(gameId),
                            () -> assertThat(event.timestamp()).isEqualTo(now),
                            () -> assertThat(event.playerId()).isEqualTo(playerId),
                            () -> assertThat(event.guessedNumber()).isEqualTo(guess.value)
                    );
                }
            }

            @Nested
            @DisplayName("and it's the last move")
            class AndItIsTheLastMove {

                @BeforeEach
                void state_is_initialized() {
                    state = Stream.of(new NumberGuessingGameWasStarted(UUID.randomUUID(), gameId, LocalDateTime.now(), UUID.randomUUID(), 10, 1));
                }

                @Test
                void then_player_guessing_attempts_exhausted_event_is_returned() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    UUID playerId = UUID.randomUUID();
                    Guess guess = new Guess(12);

                    // When
                    Stream<GameEvent> events = NumberGuessingGame.guessNumber(state, gameId, now, playerId, guess);

                    // Then
                    GuessingAttemptsExhausted event = find(GuessingAttemptsExhausted.class, events);
                    assertAll(
                            () -> assertThat(event.eventId()).isNotNull(),
                            () -> assertThat(event.gameId()).isEqualTo(gameId),
                            () -> assertThat(event.timestamp()).isEqualTo(now)
                    );
                }

                @Test
                void then_number_guessing_game_ended_event_is_returned() {
                    // Given
                    LocalDateTime now = LocalDateTime.now();
                    UUID playerId = UUID.randomUUID();
                    Guess guess = new Guess(12);

                    // When
                    Stream<GameEvent> events = NumberGuessingGame.guessNumber(state, gameId, now, playerId, guess);

                    // Then
                    NumberGuessingGameEnded event = find(NumberGuessingGameEnded.class, events);
                    assertAll(
                            () -> assertThat(event.eventId()).isNotNull(),
                            () -> assertThat(event.gameId()).isEqualTo(gameId),
                            () -> assertThat(event.timestamp()).isEqualTo(now)
                    );
                }
            }
        }
    }

    @Nested
    @DisplayName("when game has ended")
    class WhenGameHasEnded {
        private final UUID gameId = UUID.randomUUID();
        private Stream<GameEvent> state;

        @BeforeEach
        void state_is_initialized() {
            state = Stream.of(
                    new NumberGuessingGameWasStarted(UUID.randomUUID(), gameId, LocalDateTime.now(), UUID.randomUUID(), 10, 1),
                    new PlayerGuessedTheRightNumber(UUID.randomUUID(), gameId, LocalDateTime.now(), UUID.randomUUID(), 10),
                    new NumberGuessingGameEnded(UUID.randomUUID(), gameId, LocalDateTime.now())
            );
        }

        @Nested
        @DisplayName("and player makes a guess")
        class AndPlayerMakesAGuess {

            @Test
            void then_game_already_ended_exception_is_thrown() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                UUID playerId = UUID.randomUUID();
                Guess guess = new Guess(12);

                // When
                Throwable throwable = catchThrowable(() -> NumberGuessingGame.guessNumber(state, gameId, now, playerId, guess));

                // Then
                assertThat(throwable).isEqualTo(new GameAlreadyEnded(gameId));
            }
        }
    }

    @Nested
    @DisplayName("when game not started")
    class WhenGameNotStarted {
        private final UUID gameId = UUID.randomUUID();

        @Nested
        @DisplayName("and player makes a guess")
        class AndPlayerMakesAGuess {

            @Test
            void then_game_not_started_exception_is_thrown() {
                // Given
                LocalDateTime now = LocalDateTime.now();
                UUID playerId = UUID.randomUUID();
                Guess guess = new Guess(12);

                // When
                Throwable throwable = catchThrowable(() -> NumberGuessingGame.guessNumber(Stream.empty(), gameId, now, playerId, guess));

                // Then
                assertThat(throwable).isEqualTo(new GameNotStarted(gameId));
            }
        }
    }

    private static <T extends GameEvent> T find(Class<T> t, Stream<GameEvent> events) {
        return events.filter(t::isInstance)
                .map(t::cast)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(t.getSimpleName() + " wasn't found"));
    }
}