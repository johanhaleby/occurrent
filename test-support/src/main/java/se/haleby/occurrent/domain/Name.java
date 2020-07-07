package se.haleby.occurrent.domain;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static se.haleby.occurrent.time.TimeConversion.toDate;

public class Name {

    public static List<DomainEvent> defineName(LocalDateTime time, String name) {
        return Collections.singletonList(new NameDefined(toDate(time), name));
    }

    public static List<DomainEvent> changeName(List<DomainEvent> events, LocalDateTime time, String newName) {
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
        return Collections.singletonList(new NameWasChanged(toDate(time), newName));
    }
}