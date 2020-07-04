package se.haleby.occurrent.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class Composition {
    public static List<DomainEvent> chain(List<DomainEvent> initialDomainEvents, Function<List<DomainEvent>, List<DomainEvent>> f) {
        List<DomainEvent> newDomainEvents = f.apply(initialDomainEvents);

        ArrayList<DomainEvent> composedEvents = new ArrayList<>(initialDomainEvents);
        composedEvents.addAll(newDomainEvents);
        return Collections.unmodifiableList(composedEvents);
    }
}
