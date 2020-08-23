package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.gamestatus.impl;

import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.NumberGuessingGameWasStarted;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.PlayerGuessedANumberThatWasTooBig;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.PlayerGuessedANumberThatWasTooSmall;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.PlayerGuessedTheRightNumber;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.infrastructure.Serialization;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.gamestatus.GameStatus;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.gamestatus.WhatIsTheStatusOfGame;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.occurrent.filter.Filter.subject;

@Component
class SpringGameStatusFinder implements WhatIsTheStatusOfGame {

    private final EventStoreQueries queries;
    private final Serialization serialization;

    SpringGameStatusFinder(EventStoreQueries queries, Serialization serialization) {
        this.queries = queries;
        this.serialization = serialization;
    }

    @Override
    public Optional<GameStatus> findFor(UUID gameId) {
        return queries.query(subject(gameId.toString()))
                .map(serialization::deserialize)
                .collect(GameStatusBuilder::new,
                        (gameStatus, event) -> {
                            if (event instanceof NumberGuessingGameWasStarted) {
                                gameStatus.gameId = event.gameId();
                                gameStatus.secretNumber = ((NumberGuessingGameWasStarted) event).secretNumberToGuess();
                                gameStatus.maxNumberOfGuesses = ((NumberGuessingGameWasStarted) event).maxNumberOfGuesses();
                            } else if (event instanceof PlayerGuessedANumberThatWasTooSmall) {
                                PlayerGuessedANumberThatWasTooSmall e = (PlayerGuessedANumberThatWasTooSmall) event;
                                gameStatus.guesses.add(new GameStatus.GuessAndTime(e.guessedNumber(), e.timestamp()));
                            } else if (event instanceof PlayerGuessedANumberThatWasTooBig) {
                                PlayerGuessedANumberThatWasTooBig e = (PlayerGuessedANumberThatWasTooBig) event;
                                gameStatus.guesses.add(new GameStatus.GuessAndTime(e.guessedNumber(), e.timestamp()));
                            } else if (event instanceof PlayerGuessedTheRightNumber) {
                                PlayerGuessedTheRightNumber e = (PlayerGuessedTheRightNumber) event;
                                gameStatus.guesses.add(new GameStatus.GuessAndTime(e.guessedNumber(), e.timestamp()));
                            }
                        }, (gameStatus, gameStatus2) -> {
                        })
                .build();
    }

    private static class GameStatusBuilder {
        public UUID gameId;
        public int maxNumberOfGuesses;
        public int secretNumber;
        public List<GameStatus.GuessAndTime> guesses = new ArrayList<>();

        private Optional<GameStatus> build() {
            return gameId == null ? Optional.empty() : Optional.of(new GameStatus(gameId, secretNumber, maxNumberOfGuesses, guesses));
        }
    }

}
