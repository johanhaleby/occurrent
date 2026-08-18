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
 * {@link #FILTERED} apart from {@link #NOT_DELIVERABLE}. The first two describe an event a registered subscription
 * evaluated and either accepted or declined on its own terms; the third describes an event that was never offered to
 * a subscription's filter at all, because there was no running, unpaused subscription for it to reach. Acknowledging
 * on {@link #NOT_DELIVERABLE} discards an event nothing consumed.
 * <p>
 * All three come out of one evaluation, not a check taken before or after dispatch. A check taken separately would
 * let a {@code stop()}, a {@code pauseSubscription} or a {@code cancelSubscription} land between the check and the
 * dispatch, so the outcome reported here is guaranteed to be the one that was true at the moment routing decided it,
 * not at some other moment a race could have moved past it.
 */
public enum RoutingOutcome {

    /**
     * A running, unpaused subscription's filter accepted the event. The registered handler is then invoked; whether
     * that handler itself succeeds or throws is a separate signal from this outcome, which is reported before the
     * handler runs.
     */
    DELIVERED,

    /**
     * A running, unpaused subscription evaluated the event and its filter declined it. Redelivering this event would
     * loop forever, since the filter's answer does not depend on how many times the same event is offered to it.
     */
    FILTERED,

    /**
     * The event was never offered to a filter to decide. Nothing is registered, the model is not running, or the
     * sole subscription is paused. Never reported as a stand-in for {@link #FILTERED}, so a caller can tell "this
     * event is not mine" from "nothing here is currently able to receive it".
     */
    NOT_DELIVERABLE
}
