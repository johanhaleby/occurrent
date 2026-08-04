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
 * only to a push source: a {@link Source#EVENT_STORE} subscription chooses how much history it reads with
 * {@code startAt} instead.
 * <p>
 * Read by {@code @Saga} only, for now. A {@code @Projection(source = PUSH)} always catches up and has no way to say
 * otherwise, which is a gap rather than a decision: the reason a saga needs {@link #NONE}, a feed carrying events the
 * local event store does not hold, is the same for a projection. This enum lives here rather than in the saga module so
 * closing that gap is additive.
 */
public enum Catchup {
    /**
     * The default: replay the event store from the beginning once, then hand over to the live feed, so a saga that has
     * never run is folded up from history before it starts reacting. The replay reads the local event store, so that
     * store has to hold the events the feed carries. It records that it finished, so a restart skips it and lets the
     * broker resume.
     */
    FROM_EVENT_STORE,
    /**
     * Take live events only, from whatever the feed delivers next, with no replay and no event store involved.
     * <p>
     * This is what a saga fed by another application's broker needs: the local event store does not hold those events,
     * so a replay would find nothing, or worse, fold in unrelated events that happen to live there. It is also the
     * option when the history is simply not wanted.
     * <p>
     * A saga keeps its per-instance state in its {@code SagaStateStore}, so one that has run before picks up where it
     * left off either way. The difference shows on a first run against an existing history: the saga starts from
     * nothing and reacts only to what arrives from here on.
     */
    NONE
}
