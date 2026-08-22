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
import org.jspecify.annotations.Nullable;
import org.occurrent.filtermatching.DataFieldReader;
import org.occurrent.subscription.RoutingOutcome;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.Pushable;
import org.occurrent.subscription.api.blocking.RegisteringSubscribable;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
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
     * Feed a single event to the model, routing it to the registered handler if its filter matches, on the calling
     * thread.
     * <p>
     * <strong>An event fed before any subscription is registered is dropped, and this returns normally.</strong> A
     * listener that acknowledges once this returns therefore acknowledges an event nothing consumed. Ask
     * {@link #hasSubscriptions()} before feeding this model from a broker, and register the subscription before the
     * listener starts consuming. This model cannot refuse the event on your behalf, because it is also fed from the
     * write path (an {@code InMemoryEventStore} listener, say), where the event is already durably stored and
     * refusing would fail the write instead of protecting anything. A caller that can redeliver instead of a write
     * path that cannot should call {@link #acceptRedeliverable(CloudEvent)} instead, which never buffers. See ADR
     * 104. A configured {@link PushObserver} is told the event's {@link RoutingOutcome} once delivery has been
     * attempted, and that is where to get visibility into it instead. Told about the event even when a
     * subscription's filter itself throws a {@link RuntimeException} or {@link AssertionError} while being evaluated
     * (a supplied {@link DataFieldReader} can), reported as {@link RoutingOutcome#NOT_DELIVERABLE}, before that
     * exception propagates as it always has. Another {@link Error} bypasses the observer and propagates directly,
     * see {@link PushObserver}.
     *
     * @param cloudEvent The event received from the external source.
     */
    @Override
    public void accept(CloudEvent cloudEvent) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        acceptEvent(cloudEvent);
    }

    /**
     * As {@link #accept(CloudEvent)}, except an event that would only buffer, a catch-up-then-live subscription
     * still replaying, say, is refused instead: reported {@link RoutingOutcome#DEFERRED} rather than buffered, and
     * never delivered by this call. Call this instead of {@link #accept(CloudEvent)} from a broker listener that
     * can redeliver the same event later, never from a write path that cannot, since a write-path event this call
     * refuses is lost rather than protected, the same reason {@link #accept(CloudEvent)} itself never refuses.
     * <p>
     * Always evaluates the full routing decision and tells the configured {@link PushObserver}, even when this
     * model was built with none, unlike {@link #accept(CloudEvent)}'s fast path for that case: a caller of this
     * method needs the genuine {@link RoutingOutcome} to decide whether to acknowledge or redeliver, which the fast
     * path has nothing to report.
     *
     * @param cloudEvent The event received from the external source, which the caller can redeliver if this refuses it.
     */
    public void acceptRedeliverable(CloudEvent cloudEvent) {
        Objects.requireNonNull(cloudEvent, "cloudEvent cannot be null");
        routeReportingMatch(cloudEvent, false, this::notifyObserver);
    }

    /**
     * Feed a batch of events to the model, routing each in iteration order.
     * <p>
     * Drops the batch when no subscription is registered, with the caveat {@link #accept(CloudEvent)} describes. An
     * event whose predecessor's handler threw is neither observed nor routed, since the batch stops there.
     *
     * @param cloudEvents The events received from the external source.
     */
    public void accept(Iterable<CloudEvent> cloudEvents) {
        Objects.requireNonNull(cloudEvents, "cloudEvents cannot be null");
        for (CloudEvent cloudEvent : cloudEvents) {
            acceptEvent(cloudEvent);
        }
    }

    // Never call the overridable accept(CloudEvent) from here. This class is public and not final, and a subclass
    // overriding accept(CloudEvent) by delegating to accept(Iterable) for a single event would recurse indefinitely
    // if the batch loop called back into it. route(..) itself is already final, so before this observer feature
    // existed the batch path never touched an overridable method at all, and this helper keeps it that way.
    private void acceptEvent(CloudEvent cloudEvent) {
        if (observing) {
            routeReportingMatch(cloudEvent, true, this::notifyObserver);
        } else {
            route(cloudEvent);
        }
    }

    // Package-private pass-through, deliberately not named subscribeReportingDelivery: that name collides with the
    // protected final superclass method as an illegal override attempt across packages, even though this is not
    // really an override, just a same-named method with the same erasure. CatchupThenPushSubscriptionModel is
    // same-package but not a subclass, so it cannot reach the protected RegisteringSubscribable method directly.
    // Lets it register an action that reports whether an event genuinely landed, instead of the plain
    // Consumer<CloudEvent> subscribe(..) takes.
    Subscription subscribeCatchupThenPush(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, RegisteringSubscribable.RoutingAction action) {
        return super.subscribeReportingDelivery(subscriptionId, filter, startAt, action);
    }

    // Keeps a broken observer from masquerading as a handler failure. accept(...) throwing is what tells a broker
    // listener to redeliver (ADR 104), so an observer exception must never trigger that for an event that was, or
    // would have been, delivered normally. RuntimeException and AssertionError are caught, the same as a handler
    // failure elsewhere on this stack (routeIsolated) plus the assertion an observer used as a test spy is likely to
    // throw. Another Error still propagates.
    private void notifyObserver(CloudEvent cloudEvent, RoutingOutcome outcome) {
        try {
            observer.observe(cloudEvent, outcome);
        } catch (RuntimeException | AssertionError e) {
            log.warn("A PushObserver threw while observing an event pushed to {}. The observer failure did not affect routing.",
                    getClass().getSimpleName(), e);
        }
    }
}
