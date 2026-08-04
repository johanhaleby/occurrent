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

package org.occurrent.subscription.internal;

import org.jspecify.annotations.NullMarked;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

/**
 * Combines the failures collected while dispatching one event batch to several handlers into the single failure the
 * caller sees.
 * <p>
 * Lives here rather than in either subscription API module because the blocking and reactor stacks both report an
 * isolated dispatch the same way, and the two would otherwise drift.
 */
@NullMarked
public final class HandlerFailures {

    private HandlerFailures() {
    }

    /**
     * @param failures The failures to report, in the order the handlers failed.
     * @return Empty when nothing failed, the single failure when one did, otherwise the first with the rest attached
     * through {@link Throwable#addSuppressed(Throwable)}. Returning the first unchanged is what lets a caller keep
     * catching a specific type.
     */
    public static <T extends Throwable> Optional<T> combined(Collection<T> failures) {
        Iterator<T> collected = failures.iterator();
        if (!collected.hasNext()) {
            return Optional.empty();
        }
        T first = collected.next();
        // Skip the instance itself: two handlers can fail with one shared exception object, and addSuppressed refuses
        // self-suppression with an IllegalArgumentException that would replace both real failures.
        collected.forEachRemaining(failure -> {
            if (failure != first) {
                first.addSuppressed(failure);
            }
        });
        return Optional.of(first);
    }
}
