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
import reactor.core.publisher.Mono;

@Component
public class EventForwarder {
    private static final Logger log = LoggerFactory.getLogger(EventForwarder.class);

    private static final String SUBSCRIBER_ID = "test-app";
    private final ReactorDurableSubscriptionModel subscriptionModel;
    private final CloudEventConverter<DomainEvent> domainEventConverter;
    private final ApplicationEventPublisher eventPublisher;

    public EventForwarder(ReactorDurableSubscriptionModel subscriptionModel,
                          CloudEventConverter<DomainEvent> domainEventConverter,
                          ApplicationEventPublisher eventPublisher) {
        this.subscriptionModel = subscriptionModel;
        this.domainEventConverter = domainEventConverter;
        this.eventPublisher = eventPublisher;
    }

    @PostConstruct
    void startEventStreaming() {
        log.info("Subscribing with id {}", SUBSCRIBER_ID);
        subscriptionModel.subscribe(SUBSCRIBER_ID,
                cloudEvent -> Mono.just(cloudEvent)
                        .map(domainEventConverter::toDomainEvent)
                        .doOnNext(eventPublisher::publishEvent)
                        .then());
    }

    @PreDestroy
    void stopEventStreaming() {
        log.info("Unsubscribing");
        // Pause just this subscription: disposes it without deleting the stored position, so it resumes on restart,
        // and without shutting down the (potentially shared) subscription model the way shutdown() would.
        subscriptionModel.pauseSubscription(SUBSCRIBER_ID);
    }
}
