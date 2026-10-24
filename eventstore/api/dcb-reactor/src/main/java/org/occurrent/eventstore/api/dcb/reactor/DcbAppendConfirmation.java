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

package org.occurrent.eventstore.api.dcb.reactor;

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import reactor.core.publisher.Mono;

/**
 * Answers whether a DCB append is durably committed, so its {@link DcbAppendResult#markerVersions() marker versions}
 * can't be handed out again to other events. A DCB snapshot saves at an append's versions only after this answers
 * {@code true}.
 * <p>
 * A store that doesn't implement this interface is treated as unconfirmed, so a snapshot over it is never saved.
 */
@NullMarked
public interface DcbAppendConfirmation {

    /**
     * Emits {@code true} only when the store finds an event with the result's {@link DcbAppendResult#appendId()
     * append id} at a position from its first to its last sequence position, in a read that doesn't join a session or
     * transaction the subscriber context carries. A MongoDB store reads at read concern majority on the primary.
     * <p>
     * Emits {@code false} when the result has no append id. The implementations in Occurrent don't emit an error. They
     * log it at debug level and emit {@code false}.
     *
     * @param appendResult the result of the append to confirm
     * @return whether the append is confirmed
     */
    Mono<Boolean> isConfirmed(DcbAppendResult appendResult);
}
