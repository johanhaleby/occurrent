package org.occurrent.domain;

import org.occurrent.time.TimeConversion;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class Name {

    public static List<DomainEvent> defineName(String eventId, LocalDateTime time, String name) {
        return Collections.singletonList(new NameDefined(eventId, TimeConversion.toDate(time), name));
    }

    public static List<DomainEvent> changeName(List<DomainEvent> events, String eventId, LocalDateTime time, String newName) {
        Predicate<DomainEvent> isInstanceOfNameDefined = NameDefined.class::isInstance;
        Predicate<DomainEvent> isInstanceOfNameWasChanged = NameWasChanged.class::isInstance;

        String currentName = events.stream()
                .filter(isInstanceOfNameDefined.or(isInstanceOfNameWasChanged))
                .reduce("", (__, e) -> e instanceof NameDefined ? e.getName() : e.getName(), (name1, name2) -> name2);

        if (Objects.equals(currentName, "John Doe")) {
            throw new IllegalArgumentException("Cannot change name from John Doe since this is the ultimate name");
        } else if (currentName.isEmpty()) {
            throw new IllegalArgumentException("Cannot change name this it is currently undefined");
        }
        return Collections.singletonList(new NameWasChanged(eventId, TimeConversion.toDate(time), newName));
    }
}