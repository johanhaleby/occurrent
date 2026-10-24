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
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.PositionOrderedReader;
import org.occurrent.eventstore.api.blocking.internal.CatchupReads;
import org.occurrent.filter.Filter;

import java.util.stream.Stream;

/**
 * The catch-up reads of a fake store that has no position. A read in position order fails the test.
 */
final class TimeOrderedCatchupReads {

    private TimeOrderedCatchupReads() {
    }

    static CatchupReads over(EventStoreQueries queries) {
        return CatchupReads.unscoped(queries, new PositionOrderedReader() {
            @Override
            public Stream<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                throw new AssertionError("The time-ordered path must not read in position order");
            }

            @Override
            public long currentPosition() {
                throw new AssertionError("The time-ordered path must not read the position");
            }

            @Override
            public boolean writesPosition() {
                return false;
            }
        });
    }
}
