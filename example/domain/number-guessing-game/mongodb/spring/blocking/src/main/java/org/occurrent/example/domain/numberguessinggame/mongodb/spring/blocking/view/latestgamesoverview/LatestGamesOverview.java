package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.latestgamesoverview;

import java.util.stream.Stream;

@FunctionalInterface
public interface LatestGamesOverview {
    Stream<GameOverview> findOverviewOfLatestGames(int numberOfLatestGames);
}