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

package org.occurrent.springboot.common;

import org.jspecify.annotations.NullMarked;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The catch-up failures of {@code @Projection(source = PUSH, startupMode = BACKGROUND)} projections, so an application
 * can see them.
 * <p>
 * Under {@code BACKGROUND} the whole point is that nobody waits for the replay, so a failure has nowhere to be thrown:
 * the context refreshed long before it happened. The projection is left with no history folded and no live events, and
 * the application starts healthy on top of an empty read model. That is the trade {@code BACKGROUND} makes, and this
 * is what makes it observable rather than silent. The failure is also logged at {@code ERROR}, which stays the
 * backstop for an application that injects nothing.
 * <p>
 * Written by the annotation processor and read by the application, the same shape as the blocking starter's
 * {@code ManualStartPushSources}. Inject it and check it from a health indicator or a readiness probe. It only ever
 * holds ids that were started in the background, so an empty result means either that every background catch-up is
 * still running or succeeded, or that there were none: it is not by itself a statement that a projection is caught up.
 * <p>
 * One class for both stacks, since a background catch-up failure means the same thing on each and the bean carries
 * nothing stack-specific. Each starter contributes it, so an application on either one injects the same type.
 */
@NullMarked
public final class BackgroundCatchupFailures {

    private final Map<String, Throwable> failures = new LinkedHashMap<>();

    /**
     * Record that the background catch-up of {@code id} failed. Public only because the annotation processors that
     * call it live in the two starter packages rather than this one. Application code reads this bean, it does not
     * write to it. A second failure for the same id replaces the first.
     */
    public void recordFailure(String id, Throwable failure) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(failure, "failure cannot be null");
        synchronized (failures) {
            failures.put(id, failure);
        }
    }

    /**
     * The failure that ended the background catch-up of the projection with this id, if it has failed.
     */
    public Optional<Throwable> failureFor(String id) {
        Objects.requireNonNull(id, "id cannot be null");
        synchronized (failures) {
            return Optional.ofNullable(failures.get(id));
        }
    }

    /**
     * Every background catch-up failure so far, keyed by projection id, in the order they were recorded.
     */
    public Map<String, Throwable> all() {
        synchronized (failures) {
            return Map.copyOf(failures);
        }
    }

    /**
     * @return {@code true} if no background catch-up has failed. See the class javadoc for why that is not the same as
     * every projection being caught up.
     */
    public boolean isEmpty() {
        synchronized (failures) {
            return failures.isEmpty();
        }
    }

    @Override
    public String toString() {
        synchronized (failures) {
            return "BackgroundCatchupFailures" + failures.keySet();
        }
    }
}
