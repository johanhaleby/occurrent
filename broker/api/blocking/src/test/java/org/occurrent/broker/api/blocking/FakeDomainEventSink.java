/*
 * Copyright 2026 Johan Haleby
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

package org.occurrent.broker.api.blocking;

import org.occurrent.cloudevents.EventMetadata;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link DomainEventSink} that records what it published, together with the {@link EventMetadata} each event
 * arrived with, and can be told to fail every publish so a test can observe how a forwarder reacts.
 */
class FakeDomainEventSink<E> implements DomainEventSink<E> {

    record Published<E>(EventMetadata metadata, E domainEvent) {
    }

    private final List<Published<E>> published = new ArrayList<>();
    private boolean failing;

    @Override
    public void publish(E domainEvent) {
        publish(EventMetadata.empty(), domainEvent);
    }

    @Override
    public void publish(EventMetadata metadata, E domainEvent) {
        if (failing) {
            throw new RuntimeException("Simulated publish failure");
        }
        published.add(new Published<>(metadata, domainEvent));
    }

    List<Published<E>> published() {
        return published;
    }

    void failOnNextPublish() {
        this.failing = true;
    }
}
