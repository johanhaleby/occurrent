/*
 * Copyright 2020 Johan Haleby
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.occurrent.example.springevent;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.domain.DomainEvent;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;

@Component
public class EventForwarder {
    private static final Logger log = LoggerFactory.getLogger(EventForwarder.class);

    private static final String SUBSCRIBER_ID = "test-app";
    private final ReactorDurableSubscriptionModel subscriptionModel;
    private final CloudEventConverter<DomainEvent> domainEventConverter;
    private final ApplicationEventPublisher eventPublisher;
    private final AtomicReference<Disposable> subscription;

    public EventForwarder(ReactorDurableSubscriptionModel subscriptionModel,
                          CloudEventConverter<DomainEvent> domainEventConverter,
                          ApplicationEventPublisher eventPublisher) {
        this.subscriptionModel = subscriptionModel;
        this.domainEventConverter = domainEventConverter;
        this.eventPublisher = eventPublisher;
        this.subscription = new AtomicReference<>();
    }

    @PostConstruct
    void startEventStreaming() {
        log.info("Subscribing with id {}", SUBSCRIBER_ID);
        Disposable disposable = subscriptionModel.subscribe(SUBSCRIBER_ID,
                cloudEvent -> Mono.just(cloudEvent)
                        .map(domainEventConverter::toDomainEvent)
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
