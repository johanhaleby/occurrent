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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.cloudevents.CloudEvent;
import io.javalin.Javalin;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.example.domain.numberguessinggame.model.MaxNumberOfGuesses;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.*;
import org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.integrationevent.NumberGuessingGameCompleted;
import org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.integrationevent.NumberGuessingGameCompleted.GuessedNumber;
import org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.gamestatus.GameStatus;
import org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.gamestatus.WhatIsTheStatusOfGame;
import org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.latestgamesoverview.GameOverview;
import org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.latestgamesoverview.InsertGameIntoLatestGamesOverview;
import org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.view.latestgamesoverview.LatestGamesOverview;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionPositionStorage;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.occurrent.subscription.mongodb.MongoFilterSpecification.MongoBsonFilterSpecification;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionModel;
import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscriptionPositionStorage;
import org.occurrent.subscription.mongodb.nativedriver.blocking.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Sorts.descending;
import static java.time.ZoneOffset.UTC;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.filter.Filter.subject;

/**
 * Bootstrap the application
 */
public class Bootstrap {
    private static final Logger log = LoggerFactory.getLogger(Bootstrap.class);

    private static final String EVENTS_COLLECTION_NAME = "events";
    private static final String DATABASE_NAME = "test";
    private static final String LATEST_GAMES_OVERVIEW_COLLECTION_NAME = "latestGamesOverview";
    private static final String SUBSCRIPTION_POSITIONS_COLLECTION_NAME = "subscriptionPositions";
    private static final String NUMBER_GUESSING_GAME_TOPIC = "number-guessing-game";

    private final Javalin javalin;
    private final SubscriptionModel subscriptionModel;
    private final MongoClient mongoClient;
    private final RabbitMQConnectionAndChannel rabbitMQConnectionAndChannel;

    public Bootstrap(Javalin javalin, SubscriptionModel subscriptionModel,
                     MongoClient mongoClient, RabbitMQConnectionAndChannel rabbitMQConnectionAndChannel) {
        this.javalin = javalin;
        this.subscriptionModel = subscriptionModel;
        this.mongoClient = mongoClient;
        this.rabbitMQConnectionAndChannel = rabbitMQConnectionAndChannel;
    }

    public static void main(String[] args) {
        Bootstrap application = Bootstrap.bootstrap();
        Runtime.getRuntime().addShutdownHook(new Thread(application::shutdown));
    }

    public static Bootstrap bootstrap() {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoEventStore mongoEventStore = new MongoEventStore(mongoClient, DATABASE_NAME, EVENTS_COLLECTION_NAME, new EventStoreConfig(TimeRepresentation.DATE));

        ObjectMapper objectMapper = new ObjectMapper();
        Serialization serialization = new Serialization(objectMapper, URI.create("urn:occurrent:domain:numberguessinggame"));
        NumberGuessingGameApplicationService numberGuessingGameApplicationService = new NumberGuessingGameApplicationService(mongoEventStore, serialization);

        Javalin javalin = Javalin.create(cfg -> cfg.showJavalinBanner = false).start(7000);

        WhatIsTheStatusOfGame whatIsTheStatusOfGame = initializeWhatIsTheStatusOfGame(mongoEventStore, serialization);
        SubscriptionModel subscription = initializeSubscription(mongoClient);
        LatestGamesOverview latestGamesOverview = initializeLatestGamesOverview(mongoClient, serialization, subscription);
        RabbitMQConnectionAndChannel rabbitMQConnectionAndChannel = initializeRabbitMQConnection("amqp://localhost:5672").declareTopic(NUMBER_GUESSING_GAME_TOPIC, true);
        initializeNumberGuessingGameCompletedIntegrationEventPublisher(mongoEventStore, serialization::deserialize, subscription, rabbitMQConnectionAndChannel.channel, objectMapper);

        WebApi.configureRoutes(javalin, numberGuessingGameApplicationService, latestGamesOverview, whatIsTheStatusOfGame, 1, 20, MaxNumberOfGuesses.of(5));
        return new Bootstrap(javalin, subscription, mongoClient, rabbitMQConnectionAndChannel);
    }

    private static void initializeNumberGuessingGameCompletedIntegrationEventPublisher(EventStoreQueries eventStoreQueries, Function<CloudEvent, GameEvent> deserialize,
                                                                                       SubscriptionModel subscriptionModel, Channel rabbit, ObjectMapper objectMapper) {
        Consumer<CloudEvent> cloudEventConsumer = cloudEvent -> {
            String gameId = cloudEvent.getSubject();
            NumberGuessingGameCompleted numberGuessingGameCompleted = eventStoreQueries.query(subject(eq(gameId)))
                    .map(deserialize)
                    .collect(NumberGuessingGameCompleted::new, (integrationEvent, gameEvent) -> {
                        if (gameEvent instanceof NumberGuessingGameWasStarted) {
                            integrationEvent.setGameId(gameEvent.gameId().toString());
                            NumberGuessingGameWasStarted e = (NumberGuessingGameWasStarted) gameEvent;
                            integrationEvent.setSecretNumberToGuess(e.secretNumberToGuess());
                            integrationEvent.setMaxNumberOfGuesses(e.maxNumberOfGuesses());
                            integrationEvent.setStartedAt(toDate(e.timestamp()));
                        } else if (gameEvent instanceof PlayerGuessedANumberThatWasTooSmall) {
                            PlayerGuessedANumberThatWasTooSmall e = (PlayerGuessedANumberThatWasTooSmall) gameEvent;
                            integrationEvent.addGuess(new GuessedNumber(e.playerId().toString(), e.guessedNumber(), toDate(e.timestamp())));
                        } else if (gameEvent instanceof PlayerGuessedANumberThatWasTooBig) {
                            PlayerGuessedANumberThatWasTooBig e = (PlayerGuessedANumberThatWasTooBig) gameEvent;
                            integrationEvent.addGuess(new GuessedNumber(e.playerId().toString(), e.guessedNumber(), toDate(e.timestamp())));
                        } else if (gameEvent instanceof PlayerGuessedTheRightNumber) {
                            PlayerGuessedTheRightNumber e = (PlayerGuessedTheRightNumber) gameEvent;
                            integrationEvent.addGuess(new GuessedNumber(e.playerId().toString(), e.guessedNumber(), toDate(e.timestamp())));
                            integrationEvent.setRightNumberWasGuessed(true);
                        } else if (gameEvent instanceof GuessingAttemptsExhausted) {
                            integrationEvent.setRightNumberWasGuessed(false);
                        } else if (gameEvent instanceof NumberGuessingGameEnded) {
                            integrationEvent.setEndedAt(toDate(gameEvent.timestamp()));
                        }
                    }, (i1, i2) -> {
                    });

            log.info("Publishing integration event {} to {}", NumberGuessingGameCompleted.class.getSimpleName(), NUMBER_GUESSING_GAME_TOPIC);
            log.debug(numberGuessingGameCompleted.toString());

            try {
                byte[] bytes = objectMapper.writeValueAsBytes(numberGuessingGameCompleted);
                rabbit.basicPublish(NUMBER_GUESSING_GAME_TOPIC, "number-guessing-game.completed", new BasicProperties.Builder().contentType("application/json").build(), bytes);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        // We're only interested in events of type NumberGuessingGameEnded since then we know that we should publish the integration event
        subscriptionModel.subscribe("NumberGuessingGameCompletedIntegrationEventPublisher", MongoBsonFilterSpecification.filter().type(Filters::eq, NumberGuessingGameEnded.class.getSimpleName()), cloudEventConsumer);
    }

    private static RabbitMQConnectionAndChannel initializeRabbitMQConnection(String uri) {
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(uri);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            return new RabbitMQConnectionAndChannel(connection, channel);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    private static SubscriptionModel initializeSubscription(MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        NativeMongoSubscriptionModel blockingSubscriptionForMongoDB = new NativeMongoSubscriptionModel(database, EVENTS_COLLECTION_NAME, TimeRepresentation.DATE, Executors.newCachedThreadPool(), RetryStrategy.fixed(200));
        SubscriptionPositionStorage storage = new NativeMongoSubscriptionPositionStorage(database, SUBSCRIPTION_POSITIONS_COLLECTION_NAME);
        return new DurableSubscriptionModel(blockingSubscriptionForMongoDB, storage);
    }

    private static LatestGamesOverview initializeLatestGamesOverview(MongoClient mongoClient, Serialization serialization, SubscriptionModel subscription) {
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

                            final GameOverview.GameState gameState;
                            if (endedAt == null) {
                                gameState = new GameOverview.GameState.Ongoing(maxNumberOfGuesses - numberOfGuesses);
                            } else {
                                gameState = new GameOverview.GameState.Ended(endedAt, playerGuessedTheRightNumber);
                            }
                            return new GameOverview(gameId, startedAt, gameState);
                        }));

        InsertGameIntoLatestGamesOverview.insertGameIntoLatestGamesOverview(subscription, latestGamesOverviewCollection, serialization::deserialize);
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

    @Nullable
    private static Date toDate(LocalDateTime ldt) {
        if (ldt == null) {
            return null;
        }
        return Date.from(ldt.toInstant(UTC));
    }

    public void shutdown() {
        javalin.stop();
        subscriptionModel.shutdown();
        rabbitMQConnectionAndChannel.close();
        mongoClient.close();
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

    private static class RabbitMQConnectionAndChannel {
        public Connection connection;
        public Channel channel;

        RabbitMQConnectionAndChannel(Connection connection, Channel channel) {
            this.connection = connection;
            this.channel = channel;
        }

        void close() {
            try {
                channel.close();
                connection.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public RabbitMQConnectionAndChannel declareTopic(String topicName, boolean durable) {
            try {
                channel.exchangeDeclare(topicName, "topic", durable);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return this;
        }
    }
}