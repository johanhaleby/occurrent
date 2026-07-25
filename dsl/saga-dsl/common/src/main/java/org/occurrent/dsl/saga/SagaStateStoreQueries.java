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

package org.occurrent.dsl.saga;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.List;

/**
 * Additional querying capabilities that may be supported by a {@link SagaStateStore} implementation, for observing
 * instances rather than running them. The executor never calls anything here: it needs only
 * {@link SagaStateStore#find(String)}, {@link SagaStateStore#compareAndSave(String, SagaEnvelope, long)} and
 * {@link SagaStateStore#findWithDueTimers(Instant, int)}, so a store can run sagas perfectly well without implementing
 * this.
 * <p>
 * It is a separate capability because enumeration asks something genuinely new of a store: an <em>ordering</em>.
 * {@code findWithDueTimers} may return its instances in any order at all, while {@link #findByStatus} must return them
 * ascending by {@code updatedAt}. A store that cannot index or sort on that field can still satisfy the core contract.
 * <p>
 * {@link SagaInstances} needs this only to enumerate; a by-id lookup works against any store. Calling
 * {@link SagaInstances#findByStatus(SagaStatus, Instant, int)} on a store that does not implement this fails fast.
 *
 * @param <S> the user state type
 */
public interface SagaStateStoreQueries<S extends @Nullable Object> {

    /**
     * Instances with {@code status} whose {@link SagaEnvelope#updatedAt()} is strictly before {@code updatedBefore},
     * least recently updated first, at most {@code limit} of them. This is what {@link SagaInstances} enumerates over,
     * so every store must agree on the contract:
     * <ul>
     *   <li>{@code updatedBefore} is <em>exclusive</em>. Pass the current time to mean "every instance in this status",
     *       or {@code now} minus a threshold to mean "every instance that has gone quiet for longer than that". The
     *       <em>resolution</em> of that comparison is store-dependent and at best milliseconds: a store that persists
     *       {@code updatedAt} as epoch millis compares truncated values, while the executor stamps a possibly
     *       sub-millisecond {@code Instant}. An instance updated within the same millisecond as {@code updatedBefore}
     *       may therefore be excluded. A store may not be more <em>inclusive</em> than the exclusive boundary, so no
     *       instance at or after it is ever returned.</li>
     *   <li>The order is ascending by {@code updatedAt}, so the stalest instance comes first. That is the useful end
     *       for finding a stuck instance: the worst offenders arrive before {@code limit} truncates.</li>
     *   <li>{@code limit} is a <em>bound, not a page</em>. There is no cursor: {@code updatedAt} persists at
     *       millisecond precision, so instances saved in one executor tick tie, and resuming from the last row's
     *       timestamp would silently drop the rest of a tie group. A caller that needs to walk everything should
     *       raise {@code limit}, and one that needs true paging needs an ordering this method does not offer.</li>
     *   <li>An instance whose {@code updatedAt} is {@code null} is never returned. The executor always stamps it, so
     *       this only excludes a hand-built envelope, and it keeps a store whose query engine skips a missing field
     *       from disagreeing with one that could have treated {@code null} as matching.</li>
     * </ul>
     * Unlike {@link SagaStateStore#findWithDueTimers(Instant, int)} this reads whole instances, state included, because
     * {@link SagaInstance#currentStep()} cannot be answered without it. Enumerating flow-saga instances therefore
     * decodes their received logs, which is why {@code limit} is required rather than optional.
     * <p>
     * Because this is the observation path, an instance whose state can no longer be decoded must be <em>reported
     * without its state</em> rather than making the whole enumeration fail. One instance holding, say, a received event
     * whose class was renamed away would otherwise take down the progress view for every caller, at the exact moment
     * someone is looking into what went wrong. Such a row still answers every {@link SagaInstance} member except
     * {@link SagaInstance#currentStep()}. This is the opposite of {@link SagaStateStore#find(String)}, which must keep
     * failing loudly, because the executor loads an instance in order to fold and save it.
     *
     * @throws IllegalArgumentException if {@code limit} is not positive
     */
    List<SagaEnvelope<S>> findByStatus(SagaStatus status, Instant updatedBefore, int limit);
}
