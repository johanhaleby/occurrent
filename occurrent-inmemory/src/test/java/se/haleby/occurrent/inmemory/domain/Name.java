package se.haleby.occurrent.inmemory.domain;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

public class Name {

    public static List<DomainEvent> defineName(LocalDateTime time, String name) {
        return Collections.singletonList(new NameDefined(time, name));
    }

    public static List<DomainEvent> changeName(NameState state, LocalDateTime time, String newName) {
        if (state == null || state.name == null || state.name.isEmpty()) {
            throw new IllegalArgumentException("Cannot change name this it is currently undefined");
        }
        return Collections.singletonList(new NameWasChanged(time, newName));
    }

}