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

package org.occurrent.subscription;

/**
 * What a single routing evaluation decided for one event, on the blocking and reactor {@code RegisteringSubscribable}
 * and everything built on it ({@code PushObserver}, and {@code DomainEventFeed}'s live match).
 * <p>
 * A caller that acknowledges an externally sourced event (a broker message, say) has to tell {@link #DELIVERED} and
 * {@link #FILTERED} apart from {@link #NOT_DELIVERABLE} and {@link #DEFERRED}. The first two describe an event a
 * filter evaluated and either accepted or declined on its own terms. {@link #NOT_DELIVERABLE} covers every other
 * reason the event was not actually consumed and is not expected to resolve on redelivery. {@link #DEFERRED} is the
 * one case reported that is expected to resolve on redelivery. Acknowledging on {@link #NOT_DELIVERABLE} or
 * {@link #DEFERRED} both discard an event nothing consumed; the difference is only what a caller should do next.
 * <p>
 * All four come out of one evaluation, not a check taken before or after dispatch. A check taken separately would
 * let a {@code stop()}, a {@code pauseSubscription} or a {@code cancelSubscription} land between the check and the
 * dispatch, so the outcome reported here is guaranteed to be the one that was true at the moment routing decided it,
 * not at some other moment a race could have moved past it.
 */
public enum RoutingOutcome {

    /**
     * A running, unpaused subscription's filter accepted the event, and it genuinely landed: buffered where a
     * fold will eventually run against it, delivered immediately, or already applied by an earlier attempt. Whether
     * the registered handler has actually finished running by the time this outcome is reported is a per-surface
     * question, not something this outcome answers on its own; a direct dispatch such as {@code PushObserver}
     * reports this outcome before the handler completes, only after it is genuinely invoked. Whether the handler
     * succeeds or throws is a separate signal from this outcome either way. Consult the reporting method's own
     * javadoc for what this outcome guarantees on a particular surface.
     */
    DELIVERED,

    /**
     * The subscription evaluated the event under the filter registered at that moment and declined it. Redelivering
     * the same event against that same registration would loop forever, since the filter's answer for a fixed
     * registration does not depend on how many times the event is offered to it. A later redelivery is only safe to
     * skip if the registration in force has not changed since this outcome was reported.
     */
    FILTERED,

    /**
     * The event was not delivered, for a reason that is never a filter declining it and never expected to resolve
     * on its own. Nothing is registered, the model is not running, or the sole subscription is paused, so the
     * filter was never asked. A filter that was asked and threw instead of answering reports this too, since a
     * filter that failed to answer did not decline the event. Never reported as a stand-in for {@link #FILTERED} or
     * {@link #DEFERRED}, so a caller can tell "this event is not mine", "nothing here is currently able to receive
     * it", and "ask again shortly" apart.
     */
    NOT_DELIVERABLE,

    /**
     * The event reached a registration whose target cannot accept it yet, a catch-up-then-live engine still
     * replaying, say, for a reason expected to resolve on its own. Never a stand-in for {@link #NOT_DELIVERABLE}:
     * nothing here is broken, wrong, or permanently undeliverable, so a caller must redeliver rather than park or
     * discard. Safe to redeliver arbitrarily many times; each attempt is evaluated fresh and the underlying engine
     * dedupes what it has already applied.
     */
    DEFERRED
}
