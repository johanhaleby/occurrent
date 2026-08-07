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

package org.occurrent.subscription.push.blocking;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.Pushable;
import org.occurrent.subscription.api.blocking.RegisteringSubscribable;
import org.occurrent.subscription.api.blocking.Subscribable;

import java.util.function.Consumer;

/**
 * A register-only {@link Subscribable} fed by an external push source rather than by an event-store change stream.
 * <p>
 * It exists so a projection can be driven from any transport that already forwards Occurrent cloud events, such as a
 * RabbitMQ or Kafka listener, a Spring application event, or an HTTP endpoint. The application registers handlers with
 * {@link #subscribe(String, SubscriptionFilter, org.occurrent.subscription.StartAt, Consumer) subscribe} (directly, or
 * through the projection DSL) and the listener hands each received event to {@link #accept(CloudEvent)}, which routes it
 * to the handler if its {@link SubscriptionFilter} matches, on the calling thread. A handler exception propagates to
 * the caller, so the listener can decide whether to acknowledge or redeliver.
 * <p>
 * <strong>One model feeds one subscription</strong>, and a second {@code subscribe} is refused. The acknowledgement is
 * what forces it: this model has exactly one per received event, so several handlers on it would share the decision to
 * acknowledge or redeliver, and a handler that keeps failing would hold up every handler behind it. Declare one model
 * per projection or saga, each fed by its own queue. See ADR 90.
 * <p>
 * Occurrent stays transport-neutral: this model has no dependency on any broker. The pushed events must carry the
 * Occurrent cloud-event extensions the handlers rely on (at minimum {@code streamid} and {@code streamversion}, add
 * {@code position} when a catch-up model reads them). Forward the stored cloud event as CloudEvents JSON and
 * reconstruct it on the listener side.
 * <p>
 * Like {@code SynchronousSubscriptionModel}, it has no start position, checkpoint, catch-up, or replay. It only
 * ever reacts to events fed to it here and now. It is a full {@link org.occurrent.subscription.api.blocking.SubscriptionModel}, so stopping it, or pausing
 * a subscription, drops rather than defers events that arrive in the meantime (ADR 85). {@link #accept(CloudEvent)}
 * returns normally either way, so a listener that acknowledges on return acknowledges those events too, and
 * stopping this model while the push feed keeps running loses them for good. For catch-up from the event store
 * before attaching the push feed, wrap it in the replay-then-push catch-up model. The shared register-and-route
 * machinery lives in {@link RegisteringSubscribable}.
 */
@NullMarked
public class PushSubscriptionModel extends RegisteringSubscribable implements Pushable {

    /**
     * Creates a model that refuses a subscription filter on a {@code data} payload field, which is what it has always
     * done.
     */
    public PushSubscriptionModel() {
        super(Consumers.ONE);
    }

    /**
     * Creates a model that can answer a subscription filter on a {@code data} payload field by reading it through
     * {@code dataFieldReader}. Occurrent ships a Jackson-backed one in
     * {@code occurrent-common-inmemory-filter-matching-jackson}. Without one, such a filter is refused.
     */
    public PushSubscriptionModel(DataFieldReader dataFieldReader) {
        super(Consumers.ONE, dataFieldReader);
    }

    /**
     * Feed a single event to the model, routing it to the registered handler if its filter matches, on the calling
     * thread.
     * <p>
     * <strong>An event fed before any subscription is registered is dropped, and this returns normally.</strong> A
     * listener that acknowledges once this returns therefore acknowledges an event nothing consumed. Ask
     * {@link #hasSubscriptions()} before feeding this model from a broker, and register the subscription before the
     * listener starts consuming. This model cannot refuse the event on your behalf, because it is also fed from the
     * write path (an {@code InMemoryEventStore} listener, say), where the event is already durably stored and
     * refusing would fail the write instead of protecting anything. The domain-event feed, which is broker-only, does
     * refuse. See ADR 104.
     *
     * @param cloudEvent The event received from the external source.
     */
    @Override
    public void accept(CloudEvent cloudEvent) {
        route(cloudEvent);
    }

    /**
     * Feed a batch of events to the model, routing each in iteration order.
     * <p>
     * Drops the batch when no subscription is registered, with the caveat {@link #accept(CloudEvent)} describes.
     *
     * @param cloudEvents The events received from the external source.
     */
    public void accept(Iterable<CloudEvent> cloudEvents) {
        route(cloudEvents);
    }
}
