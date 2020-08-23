package org.occurrent.example.domain.numberguessinggame.mongodb.nativedriver;

import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.example.domain.numberguessinggame.model.domainevents.GameEvent;

import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

public class NumberGuessingGameApplicationService {

    private final EventStore eventStore;
    private final Serialization serialization;

    public NumberGuessingGameApplicationService(EventStore eventStore, Serialization serialization) {
        this.eventStore = eventStore;
        this.serialization = serialization;
    }

    public void play(UUID gameId, Function<Stream<GameEvent>, Stream<GameEvent>> domainFn) {
        EventStream<CloudEvent> eventStream = eventStore.read(gameId.toString());
        Stream<GameEvent> persistedGameEvents = eventStream.events().map(serialization::deserialize);

        Stream<GameEvent> newGameEvents = domainFn.apply(persistedGameEvents);

        eventStore.write(gameId.toString(), eventStream.version(), newGameEvents.map(serialization::serialize));
    }
}