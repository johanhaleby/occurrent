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
 * A caller that acknowledges an externally sourced event (a broker message, say) acknowledges on {@link #DELIVERED}
 * and {@link #FILTERED} and on nothing else. Those two describe an event a filter evaluated and either accepted or
 * declined on its own terms. The other four each say something different about why the event was not consumed, and
 * each calls for a different response.
 * <ul>
 *   <li>{@link #DEFERRED} and {@link #UNAVAILABLE} mean nothing is wrong. Offer the event again later.</li>
 *   <li>{@link #NOT_DELIVERABLE} means the filter failed to answer, or a registered action refused the event
 *       without promising the refusal is permanent. The caller's own failure policy decides.</li>
 *   <li>{@link #REFUSED} means a registered action refused the event and promised that refusing is permanent for
 *       it. Offering the same event to the same registration again gets the same answer, so stop instead.</li>
 * </ul>
 * <p>
 * Two of the six always come with an exception propagating to the caller as well, {@link #NOT_DELIVERABLE} and
 * {@link #REFUSED}. {@link #DELIVERED} may, since a handler that ran and threw is still a handler that ran, and its
 * exception propagates after this outcome has been reported. {@link #FILTERED}, {@link #DEFERRED} and
 * {@link #UNAVAILABLE} never do.
 * <p>
 * So an outcome that arrives on its own, with nothing thrown, is one of those three, and a caller can tell an event
 * nothing was able to receive from an event something tried to receive and failed on without reading any state a
 * concurrent lifecycle call could have changed in the meantime.
 * <p>
 * All six come out of one evaluation, not a check taken before or after dispatch. A check taken separately would
 * let a {@code stop()}, a {@code pauseSubscription} or a {@code cancelSubscription} land between the check and the
 * dispatch, so the outcome reported here is guaranteed to be the one that was true at the moment routing decided it,
 * not at some other moment a race could have moved past it.
 */
public enum RoutingOutcome {

    /**
     * A running, unpaused subscription's filter accepted the event, and it genuinely landed. It may be buffered
     * where a handler will eventually run against it, delivered immediately, or already applied by an earlier
     * attempt. A direct dispatch such as the blocking or reactor {@code PushObserver} reports this outcome only
     * once the registered handler has run (or, on the reactor stack, once its {@code Mono} has completed or
     * errored), whether it succeeded or failed, never before. Whether the handler succeeds or fails is a separate
     * signal from this outcome either way. Consult the reporting method's own javadoc for what this outcome
     * guarantees on a particular surface.
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
     * The filter was asked and threw instead of answering, so it neither accepted nor declined the event. The
     * exception it threw propagates to the caller as well, which is why this outcome never arrives quietly. What to
     * do next is the caller's own failure policy to decide, since a filter that fails on one event may answer the
     * next one, and a filter that fails on every event needs an operator rather than a redelivery.
     * <p>
     * Also reported for a registered action that refused the event before attempting dispatch without promising the
     * refusal is permanent, a catch-up-then-live engine whose live buffer is full while its replay is still
     * running, say. That refusal clears on its own once the replay drains, so it is a failure to report rather than
     * a reason to stop. A refusal the action does promise is permanent reports {@link #REFUSED} instead.
     */
    NOT_DELIVERABLE,

    /**
     * The event reached a registration whose target cannot accept it yet, a catch-up-then-live engine still
     * replaying, say. Never a stand-in for {@link #NOT_DELIVERABLE} or {@link #REFUSED}. Nothing here is broken,
     * wrong, or permanently undeliverable, so a caller must redeliver rather than park or discard. Safe to retry
     * arbitrarily many times, though a target that is stopped rather than merely replaying needs an operator to
     * restart it, redelivery alone will not resolve that case. Each attempt is evaluated fresh and the underlying
     * engine skips what it has already applied.
     */
    DEFERRED,

    /**
     * No registration was in a position to be asked. Nothing is registered, the model is not running, or the sole
     * subscription is paused, so no filter and no handler ran and nothing was thrown.
     * <p>
     * Every one of those three can change without anyone touching the event, so a caller offers it again later
     * rather than sending it through a failure policy. A stopped model or a paused subscription still needs an
     * operator to start or resume it before an offer succeeds, so a caller that only redelivers will keep getting
     * this same answer until that happens.
     */
    UNAVAILABLE,

    /**
     * A registered action refused the event before attempting any dispatch, and promised that refusing is permanent
     * for that registration. A catch-up-then-live engine whose replay has failed is the case this exists for. The
     * refusal's own cause propagates to the caller as well.
     * <p>
     * Offering the same event to the same registration again gets the same answer, so a caller stops rather than
     * parking, discarding or redelivering. Recovery is a lifecycle action, cancelling the subscription and
     * subscribing again, or building a new projection feed. A refusal that can clear on its own reports
     * {@link #NOT_DELIVERABLE} instead, so this outcome alone is enough to decide on.
     */
    REFUSED
}
