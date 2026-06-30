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

package org.occurrent.application.service.reactor.dcb;

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbQuery;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Reactive counterpart of the blocking DCB application service. It runs domain decisions against events selected by a
 * Dynamic Consistency Boundary query, returning a {@link Mono} for the asynchronous read and append.
 * <p>
 * The domain function stays synchronous because a DCB read materializes its matching events into a list and the
 * decision is a pure function over them. Only the read and the append are reactive.
 */
@NullMarked
public interface DcbApplicationService<E> {

    /**
     * Executes a domain function for the events selected by {@code query}.
     *
     * @param query the DCB query that defines which existing events are relevant to the decision
     * @param functionThatCallsDomainModel receives the current matching domain events and returns new domain events to append
     * @return a {@link Mono} of the append result, or {@link Optional#empty()} when the domain function produced no new events
     */
    default Mono<Optional<DcbAppendResult>> execute(DcbQuery query, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel) {
        return execute(query, DcbExecuteOptions.empty(), functionThatCallsDomainModel);
    }

    /**
     * Executes a domain function for the events selected by {@code query}, with the supplied {@link DcbExecuteOptions}.
     * <p>
     * When the options carry a side-effect, it is invoked once with the newly produced domain events after they have
     * been appended successfully. It is not invoked when the domain function produced no new events.
     *
     * @param query the DCB query that defines which existing events are relevant to the decision
     * @param options the execute options (for example a post-append side-effect)
     * @param functionThatCallsDomainModel receives the current matching domain events and returns new domain events to append
     * @return a {@link Mono} of the append result, or {@link Optional#empty()} when the domain function produced no new events
     */
    Mono<Optional<DcbAppendResult>> execute(DcbQuery query, DcbExecuteOptions<E> options, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel);
}
