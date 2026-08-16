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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.reactor.Pushable;
import org.occurrent.subscription.api.reactor.RegisteringSubscribable;
import org.occurrent.subscription.api.reactor.Subscribable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * The reactive counterpart of the blocking {@code PushSubscriptionModel}: a register-only reactive {@link Subscribable}
 * fed by an external push source rather than by an event-store change stream.
 * <p>
 * It exists so a projection can be driven from any transport that already forwards Occurrent cloud events, such as a
 * RabbitMQ or Kafka listener, a Spring application event, or an HTTP endpoint. The application registers handlers
 * through the projection DSL and the listener hands each received event to {@link #accept(CloudEvent)}, which routes it
 * to the handler if its {@link SubscriptionFilter} matches. A handler error propagates through the returned
 * {@link Mono}, so the listener can decide whether to acknowledge or redeliver.
 * <p>
 * <strong>One model feeds one subscription</strong>, and a second {@code subscribe} is refused. The acknowledgement is
 * what forces it: this model has exactly one per received event, so several handlers on it would share the decision to
 * acknowledge or redeliver, and a handler that keeps failing would hold up every handler behind it. Declare one model
 * per projection or saga, each fed by its own queue. See ADR 90.
 * <p>
 * Occurrent stays transport-neutral: this model has no dependency on any broker. The pushed events must carry the
 * Occurrent cloud-event extensions the handlers rely on (at minimum {@code streamid} and {@code streamversion}, add
 * {@code position} when a catch-up model reads them). The shared register-and-route machinery lives in
 * {@link RegisteringSubscribable}.
 */
@NullMarked
public class PushSubscriptionModel extends RegisteringSubscribable implements Pushable {

    private static final Logger log = LoggerFactory.getLogger(PushSubscriptionModel.class);

    private final PushObserver observer;
    // Precomputed once rather than compared on every accept(..). Identity against PushObserver.noop()'s singleton is
    // how "nobody is observing" is told from "an observer that happens to do nothing", so the match check this model
    // does purely for the observer's benefit is skipped for every existing caller that configured none.
    private final boolean observing;

    /**
     * Creates a model that refuses a subscription filter on a {@code data} payload field, which is what it has always
     * done.
     */
    public PushSubscriptionModel() {
        this(DataFieldReader.refusing(), PushObserver.noop());
    }

    /**
     * Creates a model that can answer a subscription filter on a {@code data} payload field by reading it through
     * {@code dataFieldReader}. Occurrent ships a Jackson-backed one in
     * {@code occurrent-common-inmemory-filter-matching-jackson}. Without one, such a filter is refused.
     */
    public PushSubscriptionModel(DataFieldReader dataFieldReader) {
        this(dataFieldReader, PushObserver.noop());
    }

    /**
     * Creates a model that both answers a subscription filter on a {@code data} payload field through
     * {@code dataFieldReader} and tells {@code observer} about every event {@link #accept(CloudEvent)} is asked to
     * deliver, see {@link PushObserver}. Pass {@link DataFieldReader#refusing()} to get the observer without also
     * answering a payload filter.
     */
    public PushSubscriptionModel(DataFieldReader dataFieldReader, PushObserver observer) {
        super(Consumers.ONE, dataFieldReader);
        this.observer = Objects.requireNonNull(observer, PushObserver.class.getSimpleName() + " cannot be null");
        this.observing = observer != PushObserver.noop();
    }

    /**
     * Feed a single event to the model, routing it to the registered handler if its filter matches.
     * <p>
     * <strong>An event fed before any subscription is registered is dropped, and the returned {@link Mono} completes
     * normally.</strong> A listener that acknowledges on completion therefore acknowledges an event nothing consumed.
     * Ask {@link #hasSubscriptions()} before feeding this model from a broker, and register the subscription before
     * the listener starts consuming. This model cannot refuse the event on your behalf, because it is also fed from
     * the write path, where the event is already durably stored and refusing would fail the write instead of
     * protecting anything. The domain-event feed, which is broker-only, does refuse. See ADR 104. A configured
     * {@link PushObserver} is told about the event, matched or not, before delivery is attempted, and that is where
     * to get visibility into it instead. Told about the event even when a subscription's filter itself throws while
     * being evaluated (a supplied {@link DataFieldReader} can), with {@code matched} reported as {@code false},
     * before that exception propagates as it always has.
     *
     * @param cloudEvent The event received from the external source.
     * @return A {@link Mono} that completes when the handler has completed.
     */
    public Mono<Void> accept(CloudEvent cloudEvent) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        if (!observing) {
            return route(cloudEvent);
        }
        // Deferred so the observer call and the routing decision both happen on subscribe, not when this Mono is
        // assembled, matching the laziness route(CloudEvent) already documents.
        return Mono.defer(() -> {
            boolean matched;
            try {
                matched = hasMatchingRegistration(cloudEvent);
            } catch (RuntimeException | AssertionError e) {
                notifyObserver(cloudEvent, false);
                throw e;
            }
            notifyObserver(cloudEvent, matched);
            return route(cloudEvent);
        });
    }

    /**
     * Feed a batch of events to the model, routing each in iteration order, sequentially.
     * <p>
     * Drops the batch when no subscription is registered, with the caveat {@link #accept(CloudEvent)} describes. An
     * event whose predecessor's handler errored is neither observed nor routed, since the batch stops there.
     *
     * @param cloudEvents The events received from the external source.
     * @return A {@link Mono} that completes when every event has been dispatched.
     */
    public Mono<Void> accept(Iterable<CloudEvent> cloudEvents) {
        Objects.requireNonNull(cloudEvents, "cloudEvents cannot be null");
        return Flux.fromIterable(cloudEvents)
                .concatMap(this::accept)
                .then();
    }

    // Keeps a broken observer from masquerading as a handler failure. accept(...) erroring is what tells a broker
    // listener to redeliver (ADR 104), so an observer exception must never trigger that for an event that was, or
    // would have been, delivered normally. RuntimeException and AssertionError are caught, the same as a handler
    // failure elsewhere on this stack (routeIsolated) plus the assertion an observer used as a test spy is likely to
    // throw. Another Error still propagates.
    private void notifyObserver(CloudEvent cloudEvent, boolean matched) {
        try {
            observer.observe(cloudEvent, matched);
        } catch (RuntimeException | AssertionError e) {
            log.warn("A PushObserver threw while observing an event pushed to {}. The observer failure did not affect routing.",
                    getClass().getSimpleName(), e);
        }
    }
}
