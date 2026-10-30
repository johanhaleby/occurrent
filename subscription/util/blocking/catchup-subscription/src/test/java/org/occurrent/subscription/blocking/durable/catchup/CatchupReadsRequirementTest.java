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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.SortBy;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A catch-up refuses an event store that does not implement the catch-up read scope when it is constructed. A decorator that only forwards the public store interfaces is such a store.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupReadsRequirementTest {

    private final InMemoryEventStore inMemory = new InMemoryEventStore();
    private final CheckpointAwareSubscriptionModel unused = unusedSubscriptionModel();
    private final CatchupSubscriptionModelConfig config = new CatchupSubscriptionModelConfig(100);
    private final DcbCriteria criteria = DcbCriteria.tags(Tag.parse("name:1"));

    @Test
    void a_dcb_catchup_refuses_a_dcb_event_store_decorator_that_does_not_implement_dcb_catchup_reads() {
        assertThatThrownBy(() -> new DcbCatchupSubscriptionModel(unused, new DcbOnly(inMemory), criteria, config))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot run a catch-up subscription over " + DcbOnly.class.getName())
                .hasMessageContaining("DcbCatchupReads");
    }

    @Test
    void the_dual_mode_catchup_refuses_a_dcb_event_store_decorator_that_does_not_implement_dcb_catchup_reads() {
        assertThatThrownBy(() -> new CatchupSubscriptionModel(unused, new DcbOnly(inMemory), criteria, config))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DcbOnly.class.getName())
                .hasMessageContaining("DcbCatchupReads");
    }

    @Test
    void the_dual_mode_catchup_refuses_an_event_store_queries_decorator_that_does_not_implement_catchup_reads() {
        assertThatThrownBy(() -> new CatchupSubscriptionModel(unused, new QueriesOnly(inMemory), config))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(QueriesOnly.class.getName())
                .hasMessageContaining("CatchupReads");
    }

    private static CheckpointAwareSubscriptionModel unusedSubscriptionModel() {
        return (CheckpointAwareSubscriptionModel) Proxy.newProxyInstance(CheckpointAwareSubscriptionModel.class.getClassLoader(), new Class<?>[]{CheckpointAwareSubscriptionModel.class},
                (proxy, method, args) -> {
                    if (method.isDefault()) {
                        return InvocationHandler.invokeDefault(proxy, method, args);
                    }
                    throw new AssertionError("The subscription model must not be used when the catch-up is refused: " + method);
                });
    }

    private static final class DcbOnly implements DcbEventStore {
        private final DcbEventStore delegate;

        private DcbOnly(DcbEventStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public DcbEventStream read(DcbCriteria criteria, DcbReadOptions options) {
            return delegate.read(criteria, options);
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events) {
            return delegate.append(events);
        }

        @Override
        public DcbAppendResult append(List<CloudEvent> events, DcbAppendCondition condition) {
            return delegate.append(events, condition);
        }
    }

    private static final class QueriesOnly implements EventStoreQueries {
        private final EventStoreQueries delegate;

        private QueriesOnly(EventStoreQueries delegate) {
            this.delegate = delegate;
        }

        @Override
        public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
            return delegate.query(filter, skip, limit, sortBy);
        }

        @Override
        public long count(Filter filter) {
            return delegate.count(filter);
        }

        @Override
        public boolean exists(Filter filter) {
            return delegate.exists(filter);
        }
    }
}
