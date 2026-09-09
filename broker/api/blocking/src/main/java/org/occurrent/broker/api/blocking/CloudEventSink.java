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
 * A single {@code publish} call is not by itself an at-least-once guarantee. A shipped implementation waits for the
 * broker to acknowledge the message and throws if that acknowledgement times out, so it never reports success for
 * an event the broker never took, but a caller that only calls {@code publish} once and does nothing on failure can
 * still lose that event. The at-least-once guarantee comes from pairing a sink that behaves this way with a caller
 * that retries on failure, which is exactly what {@link CloudEventForwarder} does by not advancing its checkpoint
 * until {@code publish} returns. A caller that republishes after a timeout, forwarder or otherwise, may produce a
 * duplicate.
 */
public interface CloudEventSink {

    /**
     * Publish a single {@link CloudEvent}.
     */
    void publish(CloudEvent cloudEvent);

    /**
     * Publish several {@link CloudEvent}s. The default publishes one at a time, and an implementation that can
     * publish several more efficiently overrides this.
     * <p>
     * Publishing one at a time means waiting for the broker to acknowledge each event before starting the next, so
     * a call that throws part way has already published the events before the one it threw on. That is what makes
     * this an at-least-once building block rather than something to make faster. A caller that retries the whole
     * {@code Iterable} republishes those, which is a duplicate and never a loss. An override that batches has to
     * hold the same property, so it cannot report success until the broker has taken every event it was given.
     * <p>
     * That retry has to be over the same events. {@link Iterable} promises nothing about being traversable twice, so
     * a one-shot or stateful one can yield only what it had not already handed out, or nothing at all, and retrying
     * that is a loss rather than a duplicate. Pass a collection, or rebuild the input before retrying.
     */
    default void publish(Iterable<CloudEvent> cloudEvents) {
        for (CloudEvent cloudEvent : cloudEvents) {
            publish(cloudEvent);
        }
    }
}
