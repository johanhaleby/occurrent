package se.haleby.occurrent.inmemory.domain;

import java.util.stream.Stream;

public class BuildNameStateFromEvents {

    public static NameState buildNameStateFromEvents(Stream<DomainEvent> events) {
        return events.collect(() -> new NameState(""), (nameState, domainEvent) -> {
            if (domainEvent instanceof NameDefined) {
                nameState.name = ((NameDefined) domainEvent).getName();
            } else if (domainEvent instanceof NameWasChanged) {
                nameState.name = ((NameWasChanged) domainEvent).getName();
            }
        }, (nameState, nameState2) -> {
        });
    }
}