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

package org.occurrent.application.service.blocking.dcb;

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbQuery;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Runs domain decisions against events selected by a Dynamic Consistency Boundary query.
 * <p>
 * The service is the DCB counterpart to Occurrent's stream-based application service:
 * it reads the current matching domain events, invokes a domain function, converts any
 * new domain events to CloudEvents, tags them, and appends them with a DCB conflict
 * condition.
 */
@NullMarked
public interface DcbApplicationService<E> {

    /**
     * Executes a domain function for the events selected by {@code query}.
     *
     * @param query the DCB query that defines which existing events are relevant to the decision
     * @param functionThatCallsDomainModel receives the current matching domain events and returns new domain events to append
     * @return the append result, or {@link Optional#empty()} when the domain function produced no new events
     */
    Optional<DcbAppendResult> execute(DcbQuery query, Function<Stream<E>, Stream<E>> functionThatCallsDomainModel);
}
