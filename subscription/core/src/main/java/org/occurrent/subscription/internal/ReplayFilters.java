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

import org.jspecify.annotations.Nullable;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.SubscriptionFilter;

/**
 * Derives the plain {@link Filter} that drives a catch-up replay's position-ordered read from a
 * {@link SubscriptionFilter}. Shared by the blocking and reactor {@code CatchupThenPushSubscriptionModel}s.
 */
public final class ReplayFilters {

    private ReplayFilters() {
    }

    /**
     * @param filter The subscription filter to translate, or {@code null} to replay everything.
     * @return {@link Filter#all()} for a {@code null} filter, or the plain filter behind a stream or
     * capability-agnostic subscription filter.
     * @throws IllegalArgumentException if {@code filter} is a kind that cannot be replayed in position order (for
     *                                  example a DCB subscription filter, which needs a different replay read).
     */
    public static Filter replayFilterFor(@Nullable SubscriptionFilter filter) {
        return switch (filter) {
            case null -> Filter.all();
            case StreamSubscriptionFilter streamSubscriptionFilter -> streamSubscriptionFilter.filter();
            case AgnosticSubscriptionFilter agnosticSubscriptionFilter -> agnosticSubscriptionFilter.filter();
            default ->
                    throw new IllegalArgumentException("Cannot catch-up-replay a " + filter.getClass().getSimpleName()
                            + ". Only a stream or capability-agnostic subscription filter can be replayed in position order.");
        };
    }
}
