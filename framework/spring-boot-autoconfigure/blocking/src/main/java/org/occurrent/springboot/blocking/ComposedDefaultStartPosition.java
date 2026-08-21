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

package org.occurrent.springboot.blocking;

import org.jspecify.annotations.NullMarked;

/**
 * The holder the default blocking Mongo auto-configuration fills when it knows its own composed subscription
 * model's {@code StartPosition.DEFAULT} bypasses the catch-up layer unconditionally (ADR 132 decision 7), read
 * afterwards by the {@code @Projection(recordAppliedAppends = true)} registrar to decide whether warning about a
 * default-position projection that never resets is a fact this registrar can verify.
 * <p>
 * The blocking stack's minimal equivalent of the reactor stack's {@code ComposedCatchupModel.defaultBypassesCatchup()},
 * just the known-fact half, not a port of its full replay-phase machinery. This stack's capability lookup already
 * unwraps a wrapper chain to find {@code ReplayAwareSubscriptions} on its own (ADR 132 decision 8), so it has no
 * equivalent gap on that side to work around.
 * <p>
 * {@code public} for the same reason {@code PushCatchupStatusImpl} is, the auto-configuration and the registrar
 * that use it live in different modules.
 */
@NullMarked
public final class ComposedDefaultStartPosition {

    private volatile boolean defaultBypassesCatchup = false;

    /**
     * Records, as a known fact, that this composition's {@code StartPosition.DEFAULT} bypasses its catch-up layer
     * unconditionally, the checkpoint is never consulted, so a wiped checkpoint changes nothing (ADR 132 decision 7).
     * Called at most once, by the auto-configuration bean method that composed this model, since only it actually
     * knows how its own composition resolves that marker. Never inferred here, and never assumed true for a
     * composition an application supplied itself, whose own {@code DEFAULT} semantics are its own to declare.
     */
    public void defaultBypassesCatchup() {
        this.defaultBypassesCatchup = true;
    }

    /**
     * Whether {@link #defaultBypassesCatchup()} was called. {@code false} until then, including for a composition
     * an application supplied itself, so a warning keyed on this answers honestly rather than by inferring
     * composition-specific behavior it cannot verify.
     */
    public boolean isDefaultKnownLiveOnly() {
        return defaultBypassesCatchup;
    }
}
