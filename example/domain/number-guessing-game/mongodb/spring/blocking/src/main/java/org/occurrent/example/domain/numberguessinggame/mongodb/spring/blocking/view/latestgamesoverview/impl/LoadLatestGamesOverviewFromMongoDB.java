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

import org.bson.Document;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.latestgamesoverview.GameOverview;
import org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.latestgamesoverview.LatestGamesOverview;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static org.springframework.data.domain.Sort.Direction.DESC;
import static org.springframework.data.mongodb.core.query.Query.query;

@Component
class LoadLatestGamesOverviewFromMongoDB implements LatestGamesOverview {

    private final MongoTemplate mongoTemplate;

    LoadLatestGamesOverviewFromMongoDB(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public Stream<GameOverview> findOverviewOfLatestGames(int numberOfLatestGames) {
        Query query = query(new Criteria()).limit(10).with(Sort.by(DESC, "startedAt"));
        return mongoTemplate.stream(query, Document.class, LatestGamesOverviewCollection.NAME)
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
                });
    }

    private static LocalDateTime toLocalDateTime(Date date) {
        if (date == null) {
            return null;
        }
        return LocalDateTime.ofInstant(date.toInstant(), UTC);
    }
}