package se.haleby.occurrent.example.domain.numberguessinggame.domainevents;

import java.time.LocalDateTime;
import java.util.UUID;

public interface GameEvent {

    UUID eventId();

    LocalDateTime timestamp();

    UUID gameId();
}
