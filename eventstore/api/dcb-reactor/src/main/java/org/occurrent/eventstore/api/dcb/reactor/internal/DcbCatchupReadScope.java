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

package org.occurrent.eventstore.api.dcb.reactor.internal;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import reactor.core.publisher.Mono;

/**
 * Internal, not part of the public API. The reads of one reactive DCB catch-up replay attempt are the live start, the head
 * and every replay read. A store that reports a live start makes these reads see everything that committed before it.
 * Close the scope when the attempt ends.
 */
@NullMarked
public interface DcbCatchupReadScope {

    /**
     * The live start the catch-up hands to the subscription model it wraps, so that every event committed after the
     * scope opened is delivered live. {@code null} when the store reports none, and the catch-up then asks the wrapped
     * subscription model for it.
     */
    @Nullable String liveStart();

    /**
     * Reads the events matching {@code criteria} in a position range, and reports the head of the DCB sequence as seen by
     * this scope in {@link DcbEventStream#lastSequencePosition()}. Only {@link DcbReadOptions#between(long, long)} style
     * options are allowed, so a read that asks for marker versions, skips, limits or reads backwards fails with
     * {@link IllegalArgumentException}.
     */
    Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options);

    /**
     * Ends the scope and releases what it holds. Reading after this is not allowed.
     */
    Mono<Void> close();
}
