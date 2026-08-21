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
 * Which part of a catch-up a delivery belongs to, as {@link ReplayPhase} reports it
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
 * decision 6).
 * <p>
 * A recording projection needs these told apart because only the first of them is history. A catch-up also delivers
 * events written after it started, and some of those reach the projection once and never again, so a recorder that
 * treated the whole catch-up as a replay would never record them.
 */
@NullMarked
public enum CatchupPhase {

    /**
     * The projection is being given events that already existed when its replay started. Nothing is recorded for a
     * delivery in this phase, and the projection clears what it had recorded before.
     */
    REPLAYING_HISTORY,

    /**
     * The replay has read all the history it set out to read, and the catch-up is now delivering events written since
     * it started. These are recorded like live events, because for some of them the catch-up is the only delivery
     * they get.
     */
    RECONCILING,

    /**
     * The catch-up has handed over to live delivery, or there was never a catch-up. Deliveries are recorded.
     */
    LIVE
}
