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

package org.occurrent.subscription.reactor.durable.catchup;

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A catch-up refuses a store that does not implement the catch-up read scope when it is constructed. A decorator that only forwards the public store interface is such a store.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorCatchupReadsRequirementTest {

    private final CheckpointAwareSubscriptionModel unused = unusedSubscriptionModel();

    @Test
    void a_dcb_catchup_refuses_a_dcb_event_store_decorator_that_does_not_implement_dcb_catchup_reads() {
        assertThatThrownBy(() -> new ReactorDcbCatchupSubscriptionModel(unused, new DcbOnly()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot run a catch-up subscription over " + DcbOnly.class.getName())
                .hasMessageContaining("DcbCatchupReads");
    }

    @Test
    void the_dual_mode_catchup_refuses_a_dcb_event_store_decorator_that_does_not_implement_dcb_catchup_reads() {
        assertThatThrownBy(() -> new ReactorCatchupSubscriptionModel(unused, new DcbOnly(), (DcbCriteria) null))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(DcbOnly.class.getName())
                .hasMessageContaining("DcbCatchupReads");
    }

    @Test
    void the_dual_mode_catchup_refuses_a_position_ordered_reader_decorator_that_does_not_implement_catchup_reads() {
        assertThatThrownBy(() -> new ReactorCatchupSubscriptionModel(unused, new ReaderOnly(), (Filter) null))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ReaderOnly.class.getName())
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
        @Override
        public Mono<DcbEventStream> read(DcbCriteria criteria, DcbReadOptions options) {
            return Mono.error(new AssertionError("A refused catch-up must not read"));
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events) {
            return Mono.error(new AssertionError("A refused catch-up must not append"));
        }

        @Override
        public Mono<DcbAppendResult> append(List<CloudEvent> events, DcbAppendCondition condition) {
            return Mono.error(new AssertionError("A refused catch-up must not append"));
        }
    }

    private static final class ReaderOnly implements PositionOrderedReader {
        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.error(new AssertionError("A refused catch-up must not read"));
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.error(new AssertionError("A refused catch-up must not read"));
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }
}
