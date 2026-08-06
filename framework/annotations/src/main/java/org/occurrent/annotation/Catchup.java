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
     * The default. It replays the event store from the beginning once, then hands over to the live feed, so a
     * subscription that has never run works through the stored history before it starts reacting to live events. The
     * replay reads the local event store, so that store has to hold the events the feed carries. It records that it
     * finished, so a restart skips it and lets the broker resume.
     */
    FROM_EVENT_STORE,
    /**
     * Take live events only, from whatever the feed delivers next, with no replay and no event store involved.
     * <p>
     * This is what a subscription fed by another application's broker needs. The local event store does not hold
     * those events, so a replay would find nothing, or worse, apply unrelated events that happen to live there. It is
     * also the option when the history is simply not wanted.
     * <p>
     * A restart is unaffected either way. A {@code @Saga} keeps its per-instance state in its own
     * {@code SagaStateStore}, and a {@code @Projection} keeps its read model in its own store, so an instance or key
     * already recorded there picks up where it left off regardless of {@code catchup}. What changes with
     * {@code NONE} is only what happens the first time a subscription reacts to a given instance or key. With the
     * default, the event store's existing history for it is read and applied before anything else. With
     * {@code NONE} there is none of that. The saga or projection starts from its own initial value and reacts only
     * to what arrives from here on.
     */
    NONE
}
