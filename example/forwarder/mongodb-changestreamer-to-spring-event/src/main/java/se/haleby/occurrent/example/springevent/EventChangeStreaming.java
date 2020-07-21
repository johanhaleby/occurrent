package se.haleby.occurrent.example.springevent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.changestreamer.mongodb.spring.reactive.SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB;
import se.haleby.occurrent.domain.DomainEvent;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static se.haleby.occurrent.functional.CheckedFunction.unchecked;

@Component
public class EventChangeStreaming {
    private static final Logger log = LoggerFactory.getLogger(EventChangeStreaming.class);

    private static final String SUBSCRIBER_ID = "test-app";
    private final SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB changeStreamerForMongoDB;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;
    private final AtomicReference<Disposable> subscription;

    public EventChangeStreaming(SpringReactiveChangeStreamerWithPositionPersistenceForMongoDB changeStreamerForMongoDB,
                                ObjectMapper objectMapper,
                                ApplicationEventPublisher eventPublisher) {
        this.changeStreamerForMongoDB = changeStreamerForMongoDB;
        this.objectMapper = objectMapper;
        this.eventPublisher = eventPublisher;
        subscription = new AtomicReference<>();
    }

    @PostConstruct
    void startEventStreaming() {
        log.info("Subscribing with id {}", SUBSCRIBER_ID);
        Disposable disposable = changeStreamerForMongoDB.stream(SUBSCRIBER_ID,
                event -> Mono.just(event)
                        .map(cloudEvent -> Objects.requireNonNull(cloudEvent.getData()))
                        .map(unchecked(eventJson -> objectMapper.readValue(eventJson, DomainEvent.class)))
                        .doOnNext(eventPublisher::publishEvent)
                        .then())
                .subscribe();
        subscription.set(disposable);
    }

    @PreDestroy
    void stopEventStreaming() {
        log.info("Unsubscribing");
        subscription.get().dispose();
        changeStreamerForMongoDB.cancelSubscription(SUBSCRIBER_ID);
    }
}