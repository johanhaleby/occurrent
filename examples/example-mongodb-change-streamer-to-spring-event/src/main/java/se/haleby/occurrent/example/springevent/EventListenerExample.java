package se.haleby.occurrent.example.springevent;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import se.haleby.occurrent.domain.NameDefined;
import se.haleby.occurrent.domain.NameWasChanged;

import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class EventListenerExample {
    private static final Logger log = LoggerFactory.getLogger(EventListenerExample.class);

    public final CopyOnWriteArrayList<NameDefined> definedNames = new CopyOnWriteArrayList<>();
    public final CopyOnWriteArrayList<NameWasChanged> changedNames = new CopyOnWriteArrayList<>();

    @EventListener
    public void handleContextStart(NameDefined nameDefined) {
        log.info("Received {}", nameDefined);
        definedNames.add(nameDefined);
    }

    @EventListener
    public void handleContextStart(NameWasChanged nameWasChanged) {
        log.info("Received {}", nameWasChanged);
        changedNames.add(nameWasChanged);
    }
}