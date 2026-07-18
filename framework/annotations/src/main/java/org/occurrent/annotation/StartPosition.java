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
 * A set of predefined, capability-neutral start positions over a subscription, projection, or snapshot's unified
 * position (the global {@code position} for {@link Subscription} and {@link Projection}, the DCB sequence position for
 * {@link DcbSubscription} and a DCB-scoped {@link Snapshot}). Shared by {@link Subscription}, {@link DcbSubscription},
 * {@link Projection}, and {@link Snapshot}.
 */
public enum StartPosition {
    /**
     * Replay the whole position sequence from the beginning (position 0) before switching to live delivery, so a read
     * model can be rebuilt from history.
     */
    BEGINNING,
    /**
     * Start from "now", delivering only events written after the subscription, projection, or snapshot starts.
     */
    NOW,
    /**
     * Use the default behavior of the subscription model. Typically this resumes from the last stored position if it
     * has run before, otherwise it behaves like {@link #NOW}.
     */
    DEFAULT
}
