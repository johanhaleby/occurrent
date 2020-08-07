package se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.*;
import io.javalin.Javalin;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import se.haleby.occurrent.changestreamer.mongodb.nativedriver.blocking.BlockingChangeStreamerForMongoDB;
import se.haleby.occurrent.changestreamer.mongodb.nativedriver.blocking.BlockingChangeStreamerWithPositionPersistenceForMongoDB;
import se.haleby.occurrent.changestreamer.mongodb.nativedriver.blocking.RetryStrategy;
import se.haleby.occurrent.eventstore.mongodb.TimeRepresentation;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import se.haleby.occurrent.eventstore.mongodb.nativedriver.StreamConsistencyGuarantee;
import se.haleby.occurrent.example.domain.numberguessinggame.model.MaxNumberOfGuesses;
import se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents.NumberGuessingGameWasStarted;
import se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents.PlayerGuessedANumberThatWasTooBig;
import se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents.PlayerGuessedANumberThatWasTooSmall;
import se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents.PlayerGuessedTheRightNumber;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.gamestatus.GameStatus;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.gamestatus.GameStatus.GuessAndTime;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.gamestatus.WhatIsTheStatusOfGame;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.latestgamesoverview.GameOverview;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.latestgamesoverview.GameOverview.GameState.Ended;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.latestgamesoverview.GameOverview.GameState.Ongoing;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.latestgamesoverview.LatestGamesOverview;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Sorts.descending;
import static java.time.ZoneOffset.UTC;
import static se.haleby.occurrent.eventstore.api.Filter.subject;
import static se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.latestgamesoverview.GameOverview.GameState;
import static se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.latestgamesoverview.InsertGameIntoLatestGamesOverview.insertGameIntoLatestGamesOverview;

public class Bootstrap {

    private static final String EVENTS_COLLECTION_NAME = "events";
    private static final String DATABASE_NAME = "test";
    private static final String LATEST_GAMES_OVERVIEW_COLLECTION_NAME = "latestGamesOverview";

    private final Javalin javalin;
    private final BlockingChangeStreamerWithPositionPersistenceForMongoDB streamer;
    private final MongoClient mongoClient;

    public Bootstrap(Javalin javalin, BlockingChangeStreamerWithPositionPersistenceForMongoDB streamer,
                     MongoClient mongoClient) {
        this.javalin = javalin;
        this.streamer = streamer;
        this.mongoClient = mongoClient;
    }

    public static void main(String[] args) {
        Bootstrap application = Bootstrap.bootstrap();
        Runtime.getRuntime().addShutdownHook(new Thread(application::shutdown));
    }

    public static Bootstrap bootstrap() {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoEventStore mongoEventStore = new MongoEventStore(mongoClient, DATABASE_NAME, EVENTS_COLLECTION_NAME, new EventStoreConfig(StreamConsistencyGuarantee.transactional("versions"), TimeRepresentation.DATE));

        Serialization serialization = new Serialization(new ObjectMapper(), URI.create("urn:occurrent:domain:numberguessinggame"));
        NumberGuessingGameApplicationService numberGuessingGameApplicationService = new NumberGuessingGameApplicationService(mongoEventStore, serialization);

        Javalin javalin = Javalin.create(cfg -> cfg.showJavalinBanner = false).start(7000);

        WhatIsTheStatusOfGame whatIsTheStatusOfGame = initializeWhatIsTheStatusOfGame(mongoEventStore, serialization);
        BlockingChangeStreamerWithPositionPersistenceForMongoDB streamer = initializeChangeStreamer(mongoClient);
        LatestGamesOverview latestGamesOverview = initializeLatestGamesOverview(mongoClient, serialization, streamer);

        HttpApi.configureRoutes(javalin, numberGuessingGameApplicationService, latestGamesOverview, whatIsTheStatusOfGame, 1, 20, MaxNumberOfGuesses.of(5));
        return new Bootstrap(javalin, streamer, mongoClient);
    }

    @NotNull
    private static WhatIsTheStatusOfGame initializeWhatIsTheStatusOfGame(MongoEventStore mongoEventStore, Serialization serialization) {
        return gameId -> mongoEventStore.query(subject(gameId.toString()))
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
    }

    private static BlockingChangeStreamerWithPositionPersistenceForMongoDB initializeChangeStreamer(MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        MongoCollection<Document> eventCollection = database.getCollection(EVENTS_COLLECTION_NAME);
        MongoCollection<Document> streamPositionCollection = database.getCollection("insertGameIntoLatestGamesOverviewPosition");
        BlockingChangeStreamerForMongoDB blockingChangeStreamerForMongoDB = new BlockingChangeStreamerForMongoDB(eventCollection, TimeRepresentation.DATE, Executors.newCachedThreadPool(), RetryStrategy.fixed(200));
        return new BlockingChangeStreamerWithPositionPersistenceForMongoDB(blockingChangeStreamerForMongoDB, streamPositionCollection);
    }

    private static LatestGamesOverview initializeLatestGamesOverview(MongoClient mongoClient, Serialization serialization, BlockingChangeStreamerWithPositionPersistenceForMongoDB streamer) {
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        if (!collectionExists(database, LATEST_GAMES_OVERVIEW_COLLECTION_NAME)) {
            database.createCollection(LATEST_GAMES_OVERVIEW_COLLECTION_NAME);
        }

        MongoCollection<Document> latestGamesOverviewCollection = database.getCollection(LATEST_GAMES_OVERVIEW_COLLECTION_NAME);

        LatestGamesOverview latestGamesOverview = count -> toStream(
                latestGamesOverviewCollection.find(Document.class)
                        .sort(descending("startedAt"))
                        .limit(count)
                        .map(game -> {
                            UUID gameId = UUID.fromString(game.getString("_id"));
                            LocalDateTime startedAt = toLocalDateTime(game.getDate("startedAt"));
                            int numberOfGuesses = game.getInteger("numberOfGuesses");
                            int maxNumberOfGuesses = game.getInteger("maxNumberOfGuesses");
                            Boolean playerGuessedTheRightNumber = game.getBoolean("playerGuessedTheRightNumber");
                            LocalDateTime endedAt = toLocalDateTime(game.getDate("endedAt"));

                            final GameState gameState;
                            if (endedAt == null) {
                                gameState = new Ongoing(maxNumberOfGuesses - numberOfGuesses);
                            } else {
                                gameState = new Ended(endedAt, playerGuessedTheRightNumber);
                            }
                            return new GameOverview(gameId, startedAt, gameState);
                        }));

        insertGameIntoLatestGamesOverview(streamer, latestGamesOverviewCollection, serialization::deserialize);
        return latestGamesOverview;
    }

    private static boolean collectionExists(MongoDatabase database, String collectionName) {
        MongoIterable<String> strings = database.listCollectionNames();
        return toStream(strings).anyMatch(c -> c.equals(collectionName));
    }

    @NotNull
    private static <T> Stream<T> toStream(Iterable<T> ts) {
        return StreamSupport.stream(ts.spliterator(), false);
    }

    @Nullable
    private static LocalDateTime toLocalDateTime(Date date) {
        if (date == null) {
            return null;
        }
        return LocalDateTime.ofInstant(date.toInstant(), UTC);
    }

    public void shutdown() {
        javalin.stop();
        streamer.shutdown();
        mongoClient.close();
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