package se.haleby.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.latestgamesoverview.impl;

import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.StreamUtils;
import org.springframework.stereotype.Component;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.latestgamesoverview.GameOverview;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.latestgamesoverview.GameOverview.GameState.Ended;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.latestgamesoverview.GameOverview.GameState.Ongoing;
import se.haleby.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.latestgamesoverview.LatestGamesOverview;

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
        return StreamUtils.createStreamFromIterator(mongoTemplate.stream(query, Document.class, LatestGamesOverviewCollection.NAME))
                .map(game -> {
                    UUID gameId = UUID.fromString(game.getString("_id"));
                    LocalDateTime startedAt = toLocalDateTime(game.getDate("startedAt"));
                    int numberOfGuesses = game.getInteger("numberOfGuesses");
                    int maxNumberOfGuesses = game.getInteger("maxNumberOfGuesses");
                    Boolean playerGuessedTheRightNumber = game.getBoolean("playerGuessedTheRightNumber");
                    LocalDateTime endedAt = toLocalDateTime(game.getDate("endedAt"));

                    final GameOverview.GameState gameState;
                    if (endedAt == null) {
                        gameState = new Ongoing(maxNumberOfGuesses - numberOfGuesses);
                    } else {
                        gameState = new Ended(endedAt, playerGuessedTheRightNumber);
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