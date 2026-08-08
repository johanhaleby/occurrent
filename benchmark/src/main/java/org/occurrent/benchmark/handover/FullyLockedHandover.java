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

package org.occurrent.benchmark.handover;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Reconstructs the live-delivery shape {@code BlockingHandover} had before ADR 108: every payload handed to
 * {@link #accept(Object)} is folded while holding the handover's own monitor, so concurrent callers queue behind
 * whichever thread is currently inside the fold.
 * <p>
 * Production {@code BlockingHandover} no longer looks like this (the dedup bookkeeping is still reserved under the
 * lock, but the fold itself now runs outside it, see
 * {@code org.occurrent.subscription.api.blocking.internal.BlockingHandover}), so there is nothing left in the
 * codebase to benchmark it against directly. This class exists only to give
 * {@link org.occurrent.benchmark.handover.BlockingHandoverThroughputBenchmark} a "current (locked)" baseline that
 * matches the one ADR 108 measured, restricted to what that measurement actually exercised: the live-accept path,
 * with de-dup and the fold both under one lock. It intentionally does not reproduce the replay/buffer/catch-up
 * machinery, which the ADR's benchmark did not touch either.
 */
final class FullyLockedHandover<T> {

    private final Consumer<T> deliver;
    private final Function<T, String> dedupId;
    private final Object lock = new Object();
    private final Set<String> deliveredIds = new HashSet<>();

    FullyLockedHandover(Consumer<T> deliver, Function<T, String> dedupId) {
        this.deliver = deliver;
        this.dedupId = dedupId;
    }

    void accept(T payload) {
        synchronized (lock) {
            String key = dedupId.apply(payload);
            if (deliveredIds.add(key)) {
                // The fold runs here, still inside the monitor, which is exactly what ADR 108 changed.
                deliver.accept(payload);
            }
        }
    }
}
