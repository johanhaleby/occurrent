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

import org.jspecify.annotations.NullMarked;
import org.occurrent.filter.Filter;

import java.util.Objects;

/**
 * A capability-agnostic {@link SubscriptionFilter} that wraps a plain Occurrent {@link Filter}.
 * <p>
 * It is the neutral sibling of {@link StreamSubscriptionFilter} (stream capability) and
 * {@link DcbSubscriptionFilter} (DCB tags). Where {@link StreamSubscriptionFilter} scopes a subscription to the
 * {@code STREAM} capability, this marker signals that events of every capability should be delivered, filtered only by
 * the wrapped {@link Filter} (typically an event-type filter). On a store with both {@code STREAM} and {@code DCB}
 * capabilities it therefore delivers both stream-written and DCB-appended events, catching up over the unified global
 * {@code position} and resuming from the same {@link GlobalCheckpoint} the other position-based models use.
 */
@NullMarked
public record AgnosticSubscriptionFilter(Filter filter) implements SubscriptionFilter {

    public AgnosticSubscriptionFilter {
        Objects.requireNonNull(filter, Filter.class.getSimpleName() + " cannot be null");
    }

    public static AgnosticSubscriptionFilter filter(Filter filter) {
        return new AgnosticSubscriptionFilter(filter);
    }
}
