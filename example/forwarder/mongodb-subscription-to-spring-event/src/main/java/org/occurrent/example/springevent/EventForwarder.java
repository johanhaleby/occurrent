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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.occurrent.domain.DomainEvent;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.subscription.util.reactor.ReactorDurableSubscriptionModel;
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
    private final ReactorDurableSubscriptionModel subscriptionModel;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;
    private final AtomicReference<Disposable> subscription;

    public EventForwarder(ReactorDurableSubscriptionModel subscriptionModel,
                          ObjectMapper objectMapper,
                          ApplicationEventPublisher eventPublisher) {
        this.subscriptionModel = subscriptionModel;
        this.objectMapper = objectMapper;
        this.eventPublisher = eventPublisher;
        this.subscription = new AtomicReference<>();
    }

    @PostConstruct
    void startEventStreaming() {
        log.info("Subscribing with id {}", SUBSCRIBER_ID);
        Disposable disposable = subscriptionModel.subscribe(SUBSCRIBER_ID,
                event -> Mono.just(event)
                        .map(cloudEvent -> Objects.requireNonNull(cloudEvent.getData()))
                        .map(CheckedFunction.unchecked(cloudEventData -> objectMapper.readValue(cloudEventData.toBytes(), DomainEvent.class)))
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