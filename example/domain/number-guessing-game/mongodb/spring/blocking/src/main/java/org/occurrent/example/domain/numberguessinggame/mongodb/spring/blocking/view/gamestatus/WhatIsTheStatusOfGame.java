package org.occurrent.example.domain.numberguessinggame.mongodb.spring.blocking.view.gamestatus;

import java.util.Optional;
import java.util.UUID;

@FunctionalInterface
public interface WhatIsTheStatusOfGame {
    Optional<GameStatus> findFor(UUID gameId);
}