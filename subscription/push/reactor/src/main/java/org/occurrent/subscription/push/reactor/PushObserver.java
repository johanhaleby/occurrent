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
import org.occurrent.subscription.RoutingOutcome;

/**
 * The reactive counterpart of the blocking {@code PushObserver}. It is told about each event
 * {@link PushSubscriptionModel#accept(CloudEvent)} is asked to deliver, once the matched registration's action has
 * run (or the model found no running, unpaused registration for it at all), so a misconfigured queue binding, a
 * missing declared event type or a type-mapping typo can be told apart from a saga or projection that received an
 * event and chose not to act on it. {@code accept(...)} itself stays silent about all of these by design, see ADR
 * 104. A filter or an action that fails in a way this model does not catch skips this call entirely, and the
 * paragraph below names those failures.
 * <p>
 * The filter is run inside a catch of {@link RuntimeException} and {@link AssertionError}, and an action's failure
 * is caught whatever it is, apart from an {@link Error} other than an {@link AssertionError}. An undeclared checked
 * exception or such an {@link Error} from the filter, and such an {@link Error} from the action, propagate with the
 * observer never told. Every other event reaches this call once, whether or not a handler ends up running.
 * {@code outcome} is {@link RoutingOutcome#DELIVERED}
 * only when the model is running, a currently registered, unpaused subscription's filter accepted the event, and
 * the action ran rather than refusing the event before attempting it, independent of whether that handler then
 * completes or errors, apart from the {@link Error} named above, which reports nothing at all.
 * It is {@link RoutingOutcome#FILTERED} when that
 * same subscription evaluated the event and declined it, and {@link RoutingOutcome#UNAVAILABLE} when there was
 * no running, unpaused subscription for the event to reach at all, whether because nothing is registered, the model
 * is stopped, or the subscription is paused. A caller acknowledging an externally sourced event may acknowledge on
 * {@link RoutingOutcome#DELIVERED} once {@code accept(...)} has completed normally, and on
 * {@link RoutingOutcome#FILTERED}, where redelivering would loop forever against this same registration, since
 * the event is not this consumer's under the filter currently registered for it. It must never acknowledge on any
 * of the other four, which is why {@link RoutingOutcome}'s values are kept apart rather than collapsed back into a
 * single flag. Read that enum for what each of them asks a caller to do next, since offering the event again,
 * applying a failure policy and stopping for good are three different answers. It shares the same
 * filter evaluation the actual dispatch
 * decision is made from, so the two can never disagree, and no lifecycle transition landing between the evaluation
 * and this call can change which outcome is reported.
 * <p>
 * A filter that throws while being evaluated (a supplied {@code DataFieldReader} can) never gets to answer whether
 * it matched. A {@link RuntimeException} or {@link AssertionError} is reported to the observer as
 * {@link RoutingOutcome#NOT_DELIVERABLE} instead, standing in for the answer that never came, never as
 * {@link RoutingOutcome#FILTERED}, since a filter that failed to answer did not decline the event. That error still
 * propagates after the observer has been told. Any other failure, an undeclared checked exception or an
 * {@link Error} other than an {@link AssertionError}, skips the observer entirely and propagates straight out.
 * <p>
 * Whatever it is being told, the real outcome or a filter's own failure, any {@link Exception} the observer
 * throws, a checked one included, is caught and logged rather than propagated, and so is an
 * {@link AssertionError}, so a broken observer cannot turn an event that was actually delivered into a broker
 * redelivery, and cannot stop the events after it in the same batch from being routed. That much is the same
 * either way. An {@link InterruptedException} is caught like any other, and the interrupt flag is set again on
 * whichever thread ran the observer, so the interrupt is not lost. That is the thread that called
 * {@code accept(..)} only when nothing upstream moved the work off it. A registered handler whose
 * {@link reactor.core.publisher.Mono} publishes on a scheduler of its own has the flag set on that scheduler's
 * worker instead, for the rest of the task that worker is running. Any other {@link Error} the observer throws is
 * not caught, and where it goes next depends on what it was being told. Told the real outcome, that
 * {@link Error} propagates on its own, once the observer has already run. Told about a filter's own failure
 * instead, it is attached to that filter's error through {@link Throwable#addSuppressed(Throwable)} rather than
 * propagating on its own, so a filter failure is never replaced by a failure in reporting it.
 * <p>
 * The returned {@link reactor.core.publisher.Mono} from {@link PushSubscriptionModel#accept(CloudEvent)} is cold, so
 * "once per event" means once per subscription to it, not once per event handed to {@code accept(..)}. Subscribing
 * twice to the same {@code Mono} observes and, if eligible, dispatches the same event twice, the same way
 * {@code route(CloudEvent)} already re-dispatches on a resubscription.
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
