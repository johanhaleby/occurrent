package se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.javalin.Javalin;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.StreamConsistencyGuarantee;
import se.haleby.occurrent.example.domain.numberguessinggame.model.MaxNumberOfGuesses;
import se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents.NumberGuessingGameWasStarted;
import se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents.PlayerGuessedANumberThatWasTooBig;
import se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents.PlayerGuessedANumberThatWasTooSmall;
import se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents.PlayerGuessedTheRightNumber;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.projection.GameStatus;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.projection.GameStatus.GuessAndTime;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.projection.WhatIsTheStatusOfGame;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static se.haleby.occurrent.eventstore.api.Filter.subject;

public class Bootstrap {


    private static Javalin javalin;
    private final MongoEventStore mongoEventStore;

    public Bootstrap(MongoEventStore mongoEventStore) {
        this.mongoEventStore = mongoEventStore;
    }

    public static void main(String[] args) {
        Bootstrap application = Bootstrap.bootstrap();
        Runtime.getRuntime().addShutdownHook(new Thread(application::shutdown));
    }

    public static Bootstrap bootstrap() {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoEventStore mongoEventStore = new MongoEventStore(mongoClient, "test", "events", new EventStoreConfig(StreamConsistencyGuarantee.transactional("versions"), TimeRepresentation.DATE));

        Serialization serialization = new Serialization(new ObjectMapper(), URI.create("urn:occurrent:domain:numberguessinggame"));
        NumberGuessingGameApplicationService numberGuessingGameApplicationService = new NumberGuessingGameApplicationService(mongoEventStore, serialization);

        javalin = Javalin.create(cfg -> cfg.showJavalinBanner = false).start(7000);

        WhatIsTheStatusOfGame whatIsTheStatusOfGame = gameId -> mongoEventStore.query(subject(gameId.toString()))
                .map(serialization::deserialize)
                .collect(GameStatusBuilder::new,
                        (gameStatus, event) -> {
                            if (event instanceof NumberGuessingGameWasStarted) {
                                gameStatus.gameId = event.gameId();
                                gameStatus.secretNumber = ((NumberGuessingGameWasStarted) event).secretNumberToGuess();
                                gameStatus.maxNumberOfGuesses = ((NumberGuessingGameWasStarted) event).maxNumberOfGuesses();
                            } else if (event instanceof PlayerGuessedANumberThatWasTooSmall) {
                                PlayerGuessedANumberThatWasTooSmall e = (PlayerGuessedANumberThatWasTooSmall) event;
                                gameStatus.guesses.add(new GuessAndTime(e.guessedNumber(), e.timestamp()));
                            } else if (event instanceof PlayerGuessedANumberThatWasTooBig) {
                                PlayerGuessedANumberThatWasTooBig e = (PlayerGuessedANumberThatWasTooBig) event;
                                gameStatus.guesses.add(new GuessAndTime(e.guessedNumber(), e.timestamp()));
                            } else if (event instanceof PlayerGuessedTheRightNumber) {
                                PlayerGuessedTheRightNumber e = (PlayerGuessedTheRightNumber) event;
                                gameStatus.guesses.add(new GuessAndTime(e.guessedNumber(), e.timestamp()));
                            }
                        }, (gameStatus, gameStatus2) -> {
                        })
                .build();

        HttpApi.configureRoutes(javalin, numberGuessingGameApplicationService, whatIsTheStatusOfGame, 1, 20, MaxNumberOfGuesses.of(5));
        return new Bootstrap(mongoEventStore);
    }

    public void shutdown() {
        javalin.stop();
        mongoEventStore.shutdown();
    }

    private static class GameStatusBuilder {
        public UUID gameId;
        public int maxNumberOfGuesses;
        public int secretNumber;
        public List<GuessAndTime> guesses = new ArrayList<>();

        private Optional<GameStatus> build() {
            return gameId == null ? Optional.empty() : Optional.of(new GameStatus(gameId, secretNumber, maxNumberOfGuesses, guesses));
        }
    }
}