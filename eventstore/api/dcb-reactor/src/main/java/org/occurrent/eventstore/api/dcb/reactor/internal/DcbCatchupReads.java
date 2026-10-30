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
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import reactor.core.publisher.Mono;

import static java.util.Objects.requireNonNull;

/**
 * Internal, not part of the public API. The reactive counterpart of the blocking {@code DcbCatchupReads}. Implemented by
 * a reactive {@link DcbEventStore} that a reactive DCB catch-up subscription reads through. The catch-up opens one
 * {@link DcbCatchupReadScope} for each replay attempt and reads the live start and every event of that attempt through
 * it.
 * <p>
 * A reactive DCB catch-up refuses at construction a {@link DcbEventStore} that doesn't implement this interface. A
 * decorator around a store therefore can't be handed to a catch-up. Hand the catch-up the store itself and decorate
 * around the subscription instead.
 */
@NullMarked
public interface DcbCatchupReads {

    /**
     * Opens the scope of one replay attempt. Subscribing to the returned {@link Mono} opens it, and the catch-up closes
     * it when the attempt ends, whether it completed, failed or was cancelled.
     *
     * @param notBefore The live start an earlier attempt stored, when this attempt resumes from it, otherwise {@code null}.
     *                  A store that reports a live start makes every read of the scope see at least everything that
     *                  committed before {@code notBefore}. A value the store doesn't recognize as one of its own live
     *                  starts is ignored.
     */
    Mono<DcbCatchupReadScope> openDcbCatchupReadScope(@Nullable String notBefore);

    /**
     * Reads {@code store} directly, in no session of its own, and reports no live start. The catch-up then takes its
     * live start from the subscription model it wraps, as it did before this interface.
     * <p>
     * Only correct for a store that assigns a position and makes the event visible in one atomic step, so no event can
     * commit below a position a read has already seen. A MongoDB store reserves a position before its write commits, so
     * using this over one brings back the event loss in
     * <a href="https://github.com/johanhaleby/occurrent/issues/1230">#1230</a>.
     */
    static DcbCatchupReads unscoped(DcbEventStore store) {
        requireNonNull(store, DcbEventStore.class.getSimpleName() + " cannot be null");
        return notBefore -> Mono.just(new DcbCatchupReadScope() {
            @Override
            public @Nullable String liveStart() {
                return null;
            }

            @Override
            public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
                return store.read(criteria, org.occurrent.eventstore.api.dcb.internal.DcbCatchupReadScope.requirePositionRange(options));
            }

            @Override
            public Mono<Void> close() {
                return Mono.empty();
            }
        });
    }
}
