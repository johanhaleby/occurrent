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

package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.latestgamesoverview.impl;

import io.cloudevents.CloudEvent;
import jakarta.annotation.PostConstruct;
import org.bson.Document;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.*;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.infrastructure.Serialization;
import org.occurrent.subscription.blocking.durable.DurableSubscriptionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.FindAndReplaceOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

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

    private final DurableSubscriptionModel subscription;
    private final Serialization serialization;
    private final MongoOperations mongoOperations;

    InsertGameIntoLatestGamesOverview(DurableSubscriptionModel subscription,
                                      Serialization serialization, MongoOperations mongoOperations) {
        this.subscription = subscription;
        this.serialization = serialization;
        this.mongoOperations = mongoOperations;
    }

    @PostConstruct
    void initializeSubscription() throws InterruptedException {
        subscription.subscribe(InsertGameIntoLatestGamesOverview.class.getSimpleName(), this::insertGame)
                .waitUntilStarted(Duration.ofSeconds(4));
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