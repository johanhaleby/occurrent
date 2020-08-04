package se.haleby.occurrent.example.domain.numberguessinggame.mongodb.nativedriver;

import io.cloudevents.CloudEvent;
import se.haleby.occurrent.eventstore.api.blocking.EventStore;
import se.haleby.occurrent.eventstore.api.blocking.EventStream;
import se.haleby.occurrent.example.domain.numberguessinggame.model.domainevents.GameEvent;

import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NumberGuessingGameApplicationService {

    private final EventStore eventStore;
    private final Serialization serialization;

    public NumberGuessingGameApplicationService(EventStore eventStore, Serialization serialization) {
        this.eventStore = eventStore;
        this.serialization = serialization;
    }

    public List<GameEvent> play(UUID gameId, Function<Stream<GameEvent>, Stream<GameEvent>> domainFn) {
        EventStream<CloudEvent> eventStream = eventStore.read(gameId.toString());
        Stream<GameEvent> persistedGameEvents = eventStream.events().map(serialization::deserialize);

        List<GameEvent> newGameEvents = domainFn.apply(persistedGameEvents).collect(Collectors.toList());

        eventStore.write(gameId.toString(), eventStream.version(), newGameEvents.stream().map(serialization::serialize));
        return newGameEvents;
    }
}