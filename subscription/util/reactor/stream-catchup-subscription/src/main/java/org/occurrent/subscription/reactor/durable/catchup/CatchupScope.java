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

package org.occurrent.subscription.reactor.durable.catchup;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

/**
 * The reads of one replay attempt, and the live start they are consistent with. {@link PositionCatchupPipeline} opens
 * one for each attempt, reads the head and every window through it, and closes it when the attempt ends, whether it
 * completed, failed or was cancelled.
 */
@NullMarked
interface CatchupScope extends CatchupReader {

    /**
     * The live start the event store reports for this attempt, or null when it reports none, in which case the wrapped
     * subscription model is asked for it.
     */
    @Nullable String liveStart();

    /**
     * Ends the scope and releases what the event store holds for it.
     */
    Mono<Void> close();
}
