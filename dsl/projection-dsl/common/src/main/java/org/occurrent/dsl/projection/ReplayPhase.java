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
 * Tells a recording wrapper which part of a catch-up its projection is in, so it knows when recording an applied
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
 * A composition that can only answer whether it is catching up at all, without telling history apart from the
 * events written since the catch-up started, answers {@link CatchupPhase#REPLAYING_HISTORY} for the whole catch-up.
 * That records nothing until the handover, which is what this interface did before it had three values, and it
 * costs the appends delivered by the reconciliation.
 */
@NullMarked
@FunctionalInterface
public interface ReplayPhase {

    /** Which part of a catch-up the projection this phase describes is in right now. */
    CatchupPhase currentPhase();

    /**
     * Which catch-up the projection is in, as a value that changes when a new one starts and is {@code 0} while it is
     * live. A recorder samples this rather than watching every transition, so without it two catch-ups in a row look
     * like one when nothing sampled the gap between them, and the second would record without first clearing what the
     * rebuild is discarding.
     * <p>
     * The default cannot tell two apart, which is the honest answer for a composition that has nothing to derive it
     * from.
     */
    default long currentGeneration() {
        return 0L;
    }

    /**
     * A phase for a composition that never replays: an in-memory model, a durable-only model with no catch-up layer,
     * or a push feed with {@code catchup = NONE}. Always answers {@link CatchupPhase#LIVE}.
     * <p>
     * Always the same instance, so a caller distinguishing this known case from an unresolved one can compare with
     * {@code ==} rather than needing its own sentinel. A non-capturing lambda expression here would not give that
     * guarantee, since the platform is free to allocate a fresh instance per call.
     */
    static ReplayPhase neverReplays() {
        return NeverReplays.INSTANCE;
    }

    // Interfaces cannot declare a private field, so the singleton instance neverReplays() hands out lives here
    // instead, in a nested class initialized on first use.
    class NeverReplays {
        private static final ReplayPhase INSTANCE = () -> CatchupPhase.LIVE;
    }
}
