package org.occurrent.example.domain.numberguessinggame.model.domainevents;

import java.time.LocalDateTime;
import java.util.UUID;

public interface GameEvent {

    UUID eventId();

    LocalDateTime timestamp();

    UUID gameId();
}
