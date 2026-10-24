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
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A catch-up refuses a reader that does not implement the catch-up read scope when it is constructed. A decorator that only forwards the public reader interface is such a reader.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorStreamCatchupReadsRequirementTest {

    @Test
    void a_position_ordered_reader_decorator_that_does_not_implement_catchup_reads_is_refused_when_the_catchup_is_constructed() {
        assertThatThrownBy(() -> new ReactorStreamCatchupSubscriptionModel(unusedSubscriptionModel(), new ReaderOnly()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot run a catch-up subscription over " + ReaderOnly.class.getName())
                .hasMessageContaining("CatchupReads");
    }

    static CheckpointAwareSubscriptionModel unusedSubscriptionModel() {
        return (CheckpointAwareSubscriptionModel) Proxy.newProxyInstance(CheckpointAwareSubscriptionModel.class.getClassLoader(), new Class<?>[]{CheckpointAwareSubscriptionModel.class},
                (proxy, method, args) -> {
                    if (method.isDefault()) {
                        return InvocationHandler.invokeDefault(proxy, method, args);
                    }
                    throw new AssertionError("The subscription model must not be used when the catch-up is refused: " + method);
                });
    }

    static final class ReaderOnly implements PositionOrderedReader {
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
