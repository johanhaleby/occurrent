package se.haleby.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.latestgamesoverview.impl;

import io.cloudevents.CloudEvent;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.FindAndReplaceOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import se.haleby.occurrent.changestreamer.mongodb.spring.blocking.SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB;
import se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents.*;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.infrastructure.Serialization;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Date;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Component
class InsertGameIntoLatestGamesOverview {
    private static final Logger log = LoggerFactory.getLogger(InsertGameIntoLatestGamesOverview.class);

    private final SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB changeStreamer;
    private final Serialization serialization;
    private final MongoOperations mongoOperations;

    InsertGameIntoLatestGamesOverview(SpringBlockingChangeStreamerWithPositionPersistenceForMongoDB changeStreamer,
                                      Serialization serialization, MongoOperations mongoOperations) {
        this.changeStreamer = changeStreamer;
        this.serialization = serialization;
        this.mongoOperations = mongoOperations;
    }

    @PostConstruct
    void initializeChangeStreamer() throws InterruptedException {
        changeStreamer.stream(InsertGameIntoLatestGamesOverview.class.getSimpleName(), this::insertGame)
                .await(Duration.ofSeconds(4));
    }

    @Retryable(maxAttempts = 10, backoff = @Backoff(delay = 100, multiplier = 2, maxDelay = 5000))
    void insertGame(CloudEvent cloudEvent) {
        GameEvent gameEvent = serialization.deserialize(cloudEvent);

        if (gameEvent instanceof NumberGuessingGameWasStarted) {
            NumberGuessingGameWasStarted e = (NumberGuessingGameWasStarted) gameEvent;
            Document game = new Document();
            game.put("_id", gameEvent.gameId().toString());
            game.put("startedAt", toDate(gameEvent.timestamp()));
            game.put("numberOfGuesses", 0);
            game.put("maxNumberOfGuesses", e.maxNumberOfGuesses());

            try {
                mongoOperations.insert(game, LatestGamesOverviewCollection.NAME);
            } catch (DuplicateKeyException exception) {
                log.info("Duplicate key found in MongoDB, ignoring");
            }
        } else {
            Query query = query(where("_id").is(gameEvent.gameId().toString()));
            Document game = mongoOperations.findOne(query, Document.class, LatestGamesOverviewCollection.NAME);
            if (game == null) { // This game is not one of the latest
                return;
            }

            if (gameEvent instanceof PlayerGuessedANumberThatWasTooSmall || gameEvent instanceof PlayerGuessedANumberThatWasTooBig) {
                game.put("numberOfGuesses", (int) game.get("numberOfGuesses") + 1);
            } else if (gameEvent instanceof PlayerGuessedTheRightNumber) {
                game.put("numberOfGuesses", (int) game.get("numberOfGuesses") + 1);
                game.put("playerGuessedTheRightNumber", true);
            } else if (gameEvent instanceof GuessingAttemptsExhausted) {
                game.put("playerGuessedTheRightNumber", false);
            } else if (gameEvent instanceof NumberGuessingGameEnded) {
                game.put("endedAt", toDate(gameEvent.timestamp()));
            }

            mongoOperations.findAndReplace(query, game, new FindAndReplaceOptions().upsert(), LatestGamesOverviewCollection.NAME);
        }
    }

    private static Date toDate(LocalDateTime timestamp) {
        return Date.from(timestamp.truncatedTo(MILLIS).toInstant(UTC));
    }
}