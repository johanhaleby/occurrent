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

import io.cloudevents.CloudEvent;

/**
 * Publishes a {@link CloudEvent} to a broker. Routing lives behind the sink rather than beside it, so an
 * application that already has its own publisher wrapper replaces this and gets both the destination and the
 * delivery decision from its own code, instead of half of it staying here.
 * <p>
 * A shipped implementation publishes at least once. It waits for the broker to acknowledge the message and throws
 * if that acknowledgement times out, and a caller that republishes after a timeout may produce a duplicate. That
 * is the same guarantee every consumer of a {@link CloudEventForwarder} already relies on.
 */
public interface CloudEventSink {

    /**
     * Publish a single {@link CloudEvent}.
     */
    void publish(CloudEvent cloudEvent);

    /**
     * Publish several {@link CloudEvent}s. The default publishes one at a time, and an implementation that can
     * publish several more efficiently overrides this.
     */
    default void publish(Iterable<CloudEvent> cloudEvents) {
        for (CloudEvent cloudEvent : cloudEvents) {
            publish(cloudEvent);
        }
    }
}
