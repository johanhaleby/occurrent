package org.occurrent.example.springevent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.occurrent.domain.DomainEvent;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.subscription.mongodb.spring.reactor.SpringReactorSubscriptionThatStoresSubscriptionPositionInMongoDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class EventForwarder {
    private static final Logger log = LoggerFactory.getLogger(EventForwarder.class);

    private static final String SUBSCRIBER_ID = "test-app";
    private final SpringReactorSubscriptionThatStoresSubscriptionPositionInMongoDB subscriptionForMongoDB;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;
    private final AtomicReference<Disposable> subscription;

    public EventForwarder(SpringReactorSubscriptionThatStoresSubscriptionPositionInMongoDB subscription,
                          ObjectMapper objectMapper,
                          ApplicationEventPublisher eventPublisher) {
        this.subscriptionForMongoDB = subscription;
        this.objectMapper = objectMapper;
        this.eventPublisher = eventPublisher;
        this.subscription = new AtomicReference<>();
    }

    @PostConstruct
    void startEventStreaming() {
        log.info("Subscribing with id {}", SUBSCRIBER_ID);
        Disposable disposable = subscriptionForMongoDB.subscribe(SUBSCRIBER_ID,
                event -> Mono.just(event)
                        .map(cloudEvent -> Objects.requireNonNull(cloudEvent.getData()))
                        .map(CheckedFunction.unchecked(eventJson -> objectMapper.readValue(eventJson, DomainEvent.class)))
                        .doOnNext(eventPublisher::publishEvent)
                        .then())
                .subscribe();
        subscription.set(disposable);
    }

    @PreDestroy
    void stopEventStreaming() {
        log.info("Unsubscribing");
        subscription.get().dispose();
    }
}