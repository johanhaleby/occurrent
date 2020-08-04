package se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver.projection;

import java.util.Optional;
import java.util.UUID;

@FunctionalInterface
public interface WhatIsTheStatusOfGame {
    Optional<GameStatus> findFor(UUID gameId);
}