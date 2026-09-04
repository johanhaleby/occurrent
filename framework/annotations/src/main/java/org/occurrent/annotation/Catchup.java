/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.annotation;

/**
 * Whether a {@link Source#PUSH} subscription is backfilled from the event store before it takes live events. Applies
 * only to a push source. A {@link Source#EVENT_STORE} subscription chooses how much history it reads with
 * {@code startAt} instead. Read by both {@code @Saga} and {@code @Projection}.
 */
public enum Catchup {
    /**
     * By default it replays the event store from the beginning once, then hands over to the live feed, so a
     * subscription that has never run works through the stored history before it starts reacting to live events. It
     * records that it finished, so a restart skips it and lets the broker resume.
     * <p>
     * The replay reads the local event store, so what it finds there is what this application appended itself. That
     * covers the ordinary case, where the same application writes its events and publishes them, and the feed then
     * delivers what the store already holds. An event published by another service is not in this store and the
     * replay will not find it, which does not make the source misconfigured. What it means is that such an event
     * exists only on the broker until it has been applied, and the catch-up readiness gate is what stops a bridge
     * acknowledging it before that happens.
     */
    FROM_EVENT_STORE,
    /**
     * Take live events only, from whatever the feed delivers next, with no replay and no event store involved.
     * <p>
     * This is what a subscription fed entirely by another application's broker needs. The local event store holds
     * none of those events, so a replay reads a history that is either empty or somebody else's, and applies whatever
     * unrelated events happen to live there. It is also the option when the history is simply not wanted.
     * <p>
     * A restart is unaffected either way. A {@code @Saga} keeps its per-instance state in its own
     * {@code SagaStateStore}, and a {@code @Projection} keeps its read model in its own store, so an instance or key
     * already recorded there picks up where it left off regardless of {@code catchup}. What changes with
     * {@code NONE} is only what happens the first time a subscription reacts to a given instance or key. With the
     * default, the event store's existing history for it is read and applied before anything else. With
     * {@code NONE} there is none of that. The saga or projection starts from its own initial value and reacts only
     * to what arrives from here on.
     * <p>
     * Delivery is still at-least-once, so the handler has to tolerate the same event arriving twice. The de-dup cache
     * behind a catch-up only suppresses the overlap between the replay and the live feed, and with {@code NONE} there
     * is no replay and therefore no overlap. It is not a guard against your broker redelivering a message.
     */
    NONE
}
