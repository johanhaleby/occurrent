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
import org.occurrent.subscription.RoutingOutcome;

/**
 * Told about every event {@link PushSubscriptionModel#accept(CloudEvent)} is asked to deliver, once the matched
 * registration's action has run (or the model found no running, unpaused registration for it at all), so a
 * misconfigured queue binding, a missing declared event type or a type-mapping typo can be told apart from a saga
 * or projection that received an event and chose not to act on it. {@code accept(...)} itself stays silent about
 * all of these by design, see ADR 104.
 * <p>
 * Called once per event, whether or not a handler ends up running. {@code outcome} is {@link RoutingOutcome#DELIVERED}
 * only when the model is running, a currently registered, unpaused subscription's filter accepted the event, and
 * the registration's action genuinely ran, independent of whether that action goes on to succeed or throw. It is
 * {@link RoutingOutcome#FILTERED} when that same subscription evaluated the event and declined it, and
 * {@link RoutingOutcome#NOT_DELIVERABLE} when there was no running, unpaused subscription for the event to reach at
 * all, whether because nothing is registered, the model is stopped, or the subscription is paused. A caller
 * acknowledging an externally sourced event may acknowledge on {@link RoutingOutcome#DELIVERED} once
 * {@code accept(...)} has returned normally, and on {@link RoutingOutcome#FILTERED}, where redelivering would loop
 * forever against this same registration, since the event is not this consumer's under the filter currently
 * registered for it. It must never acknowledge on {@link RoutingOutcome#NOT_DELIVERABLE}, which is why the three
 * are kept apart rather than collapsed back into a single flag. It shares the same filter evaluation the actual
 * dispatch decision is made from, so the two can never disagree, and no lifecycle transition landing between the
 * evaluation and this call can change which outcome is reported.
 * <p>
 * <strong>A broker bridge feeding this model from outside the process, rather than the in-process write path
 * {@link PushSubscriptionModel#accept(CloudEvent)} serves, should call
 * {@link PushSubscriptionModel#acceptRedeliverable(CloudEvent)} instead.</strong> When this model is wrapped in a
 * {@link CatchupThenPushSubscriptionModel} still replaying or draining,
 * {@code acceptRedeliverable(...)} refuses such an event outright rather than buffering it, reported
 * {@link RoutingOutcome#DEFERRED}, safe to redeliver and never a reason to acknowledge. {@code RabbitMqCloudEventBridge}
 * and {@code KafkaCloudEventBridge} do exactly this, and are correct with no further configuration:
 * {@link CatchupThenPushSubscriptionModel#isReadyForLiveDelivery(String)} and their own {@code readinessSource}
 * remain available, but only as an optional pacing hint that cuts down on how often that refuse-and-redeliver round
 * trip happens, not as a correctness requirement. Fed directly through {@code accept(...)}, with no catch-up
 * wrapper in front, {@code DELIVERED} is exactly what it always was, safe to acknowledge on once
 * {@code accept(...)} returns.
 * <p>
 * A filter that throws while being evaluated (a supplied {@code DataFieldReader} can) never gets to answer whether
 * it matched. A {@link RuntimeException} or {@link AssertionError} is reported to the observer as
 * {@link RoutingOutcome#NOT_DELIVERABLE} instead, standing in for the answer that never came, never as
 * {@link RoutingOutcome#FILTERED}, since a filter that failed to answer did not decline the event. That exception
 * still propagates after the observer has been told. Any other {@link Error} skips the observer entirely and
 * propagates straight out.
 * <p>
 * Whatever it is being told, the real outcome or a filter's own failure, a {@link RuntimeException} or
 * {@link AssertionError} the observer throws is caught and logged rather than propagated, so a broken observer
 * cannot turn an event that was actually delivered into a broker redelivery. That much is the same either way. Any
 * other {@link Error} the observer throws is not caught, and where it goes next depends on what it was being told.
 * Told the real outcome, that {@link Error} propagates on its own, once the observer has already run. Told about a
 * filter's own failure instead, it is attached to that filter's exception through
 * {@link Throwable#addSuppressed(Throwable)} rather than propagating on its own, so a filter failure is never
 * replaced by a failure in reporting it.
 * <p>
 * The default, {@link #noop()}, changes nothing for existing code, and {@link PushSubscriptionModel} skips both this
 * call and the match check entirely when no other observer is configured.
 */
@NullMarked
@FunctionalInterface
public interface PushObserver {

    /**
     * @param cloudEvent The event {@code accept(...)} was asked to deliver.
     * @param outcome    What the single routing evaluation for this event decided.
     */
    void observe(CloudEvent cloudEvent, RoutingOutcome outcome);

    /**
     * An observer that does nothing, the default every {@link PushSubscriptionModel} constructor uses when none is
     * given. Always the same instance, which is what lets {@link PushSubscriptionModel} tell "nobody is observing"
     * from "an observer that happens to do nothing" and skip the match check for the former.
     */
    static PushObserver noop() {
        return PushObserverNoop.INSTANCE;
    }
}
