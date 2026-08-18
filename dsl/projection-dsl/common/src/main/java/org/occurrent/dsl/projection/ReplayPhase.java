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

package org.occurrent.dsl.projection;

import org.jspecify.annotations.NullMarked;

/**
 * Tells a recording wrapper whether its projection is currently replaying, so it knows when recording an applied
 * append is safe
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
 * decision 8).
 * <p>
 * The owner of a composition supplies this, rather than a recorder asking a subscription model on its own: on the
 * reactive stack a plain capability lookup cannot see a catch-up model composed behind a durable wrapper, so the
 * default reactive Mongo wiring would otherwise look replay-free and record straight through a genuine replay. A
 * caller that knows the composition (the blocking registrar unwrapping with {@code ReplayAwareSubscriptions.findIn(..)},
 * or whoever composed the reactive model) is the one who can answer correctly.
 * <p>
 * A named functional type rather than a bare {@code BooleanSupplier}, so the one case a composition can never
 * replay has an explicit value ({@link #neverReplays()}) instead of a boolean whose polarity a caller could invert
 * by mistake.
 */
@NullMarked
@FunctionalInterface
public interface ReplayPhase {

    /** Whether the projection this phase describes is currently replaying history. */
    boolean isReplaying();

    /**
     * A phase for a composition that never replays: an in-memory model, a durable-only model with no catch-up layer,
     * or a push feed with {@code catchup = NONE}. Always answers {@code false}.
     */
    static ReplayPhase neverReplays() {
        return () -> false;
    }
}
