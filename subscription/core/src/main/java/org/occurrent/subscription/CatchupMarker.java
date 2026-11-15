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

import org.jspecify.annotations.Nullable;

/**
 * The catch-up marker of a subscription id and the generation of that id, as a marker storage held them at one
 * instant.
 * <p>
 * The generation changes each time the marker is forgotten. Pass {@link #generation()} back when you save the marker
 * at the end of the catch-up that read it. If the marker is forgotten in between, the saved marker reads as absent.
 *
 * @param checkpoint The stored marker, or {@code null} when no marker is stored or the stored one was saved with a
 *                   generation that a later forget replaced
 * @param generation The generation of the subscription id, {@code 0} when the marker has never been forgotten
 */
public record CatchupMarker(@Nullable Checkpoint checkpoint, long generation) {

    /**
     * @return {@code true} if a marker is stored and was saved with the current generation, so a catch-up can skip
     * the replay
     */
    public boolean isPresent() {
        return checkpoint != null;
    }
}
