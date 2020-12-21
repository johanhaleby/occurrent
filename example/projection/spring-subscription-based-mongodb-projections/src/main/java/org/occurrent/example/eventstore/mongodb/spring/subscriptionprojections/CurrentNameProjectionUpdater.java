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

package org.occurrent.example.eventstore.mongodb.spring.subscriptionprojections;

import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;

import static io.vavr.API.*;
import static io.vavr.Predicates.instanceOf;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.occurrent.cloudevents.OccurrentExtensionGetter.getStreamId;

@Component
public class CurrentNameProjectionUpdater {

    private final SubscriptionModel subscription;
    private final CurrentNameProjection currentNameProjection;
    private final DeserializeCloudEventToDomainEvent deserializeCloudEventToDomainEvent;

    public CurrentNameProjectionUpdater(SubscriptionModel subscription,
                                        CurrentNameProjection currentNameProjection,
                                        DeserializeCloudEventToDomainEvent deserializeCloudEventToDomainEvent) {
        this.subscription = subscription;
        this.currentNameProjection = currentNameProjection;
        this.deserializeCloudEventToDomainEvent = deserializeCloudEventToDomainEvent;
    }

    @PostConstruct
    void startProjectionUpdater() {
        subscription
                .subscribe("current-name", cloudEvent -> {
                    DomainEvent domainEvent = deserializeCloudEventToDomainEvent.deserialize(cloudEvent);
                    String streamId = getStreamId(cloudEvent);
                    CurrentName currentName = Match(domainEvent).of(
                            Case($(instanceOf(NameDefined.class)), e -> new CurrentName(streamId, e.getName())),
                            Case($(instanceOf(NameWasChanged.class)), e -> new CurrentName(streamId, e.getName())));
                    currentNameProjection.save(currentName);
                })
                .waitUntilStarted(Duration.of(2, SECONDS));
    }
}