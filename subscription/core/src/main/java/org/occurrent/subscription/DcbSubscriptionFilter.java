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
import org.occurrent.eventstore.api.dcb.DcbCriteria;

import java.util.Objects;

/**
 * A {@link SubscriptionFilter} for Dynamic Consistency Boundary (DCB) subscriptions, expressed as a {@link DcbCriteria}.
 * <p>
 * This is the DCB counterpart to {@link StreamSubscriptionFilter}: where the stream filter wraps an Occurrent
 * {@link org.occurrent.filter.Filter}, the DCB filter wraps a {@link DcbCriteria}. A subscription model that understands
 * it limits the delivered events to those matching the criteria, server-side where the backend supports it.
 */
@NullMarked
public record DcbSubscriptionFilter(DcbCriteria criteria) implements SubscriptionFilter {

    public DcbSubscriptionFilter {
        Objects.requireNonNull(criteria, DcbCriteria.class.getSimpleName() + " cannot be null");
    }

    public static DcbSubscriptionFilter filter(DcbCriteria criteria) {
        return new DcbSubscriptionFilter(criteria);
    }
}
