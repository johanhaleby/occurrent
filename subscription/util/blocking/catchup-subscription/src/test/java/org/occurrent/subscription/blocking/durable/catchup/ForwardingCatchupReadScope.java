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


package org.occurrent.subscription.blocking.durable.catchup;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.filter.Filter;

import java.util.stream.Stream;

/**
 * Passes every call on to the wrapped scope, so a test decorator overrides only the reads it delays or fails.
 */
class ForwardingCatchupReadScope implements CatchupReadScope {
    private final CatchupReadScope delegate;

    ForwardingCatchupReadScope(CatchupReadScope delegate) {
        this.delegate = delegate;
    }

    @Override
    public @Nullable String liveStart() {
        return delegate.liveStart();
    }

    @Override
    public long currentPosition() {
        return delegate.currentPosition();
    }

    @Override
    public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
        return delegate.readInPositionOrder(filter, range);
    }

    @Override
    public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
        return delegate.query(filter, skip, limit, sortBy);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
