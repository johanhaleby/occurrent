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

package org.occurrent.command.dcb;

import org.jspecify.annotations.Nullable;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.eventstore.api.dcb.DcbCriteria;

import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * The DCB twin of {@code org.occurrent.command.Invocation}: a command whose handling logic <i>is</i> the command, for a
 * write model bounded by a {@link DcbCriteria} rather than by a stream. Where the stream form names a stream id, this
 * names the read boundary the decision is made against and appended under.
 * <p>
 * Dispatch it with {@link DcbCommandDispatchers#invocation(org.occurrent.application.service.blocking.dcb.DcbApplicationService)}.
 * Because the application service re-reads the boundary before the function decides, a duplicated or stale invocation
 * is rejected by the domain's own rules, exactly as with {@code DcbCommandDispatchers.decider(...)}.
 * <p>
 * A {@link org.occurrent.dsl.dcb.DcbDecider} carries its own {@link TagGenerator}, and this carries the same thing
 * optionally. Leave it
 * {@code null} when the application service was built with a global tag generator or when the decision returns events
 * that already carry their tags; supply one to tag this invocation's events specifically, which is what
 * {@code DcbExecuteOptions.tagGenerator(...)} does for a decider.
 * <p>
 * As with the stream form, two invocations are equal only when they hold the same boundary and the very same function
 * instance, so assert on what {@link #decision()} does rather than on the value.
 *
 * @param criteria     the read boundary to fold, and the condition the decided events are appended under
 * @param tagGenerator tags for the decided events, or {@code null} to use the application service's global generator
 * @param decision     a <i>pure</i> function from the events inside the boundary to the events to append
 * @param <E>          the event type of the write model
 */
public record DcbInvocation<E>(DcbCriteria criteria, @Nullable TagGenerator<E> tagGenerator,
                               Function<List<E>, List<E>> decision) {

    public DcbInvocation {
        requireNonNull(criteria, "criteria cannot be null");
        requireNonNull(decision, "decision cannot be null");
    }

    /** An invocation that runs {@code decision} inside {@code criteria}, tagged by the application service's generator. */
    public static <E> DcbInvocation<E> to(DcbCriteria criteria, Function<List<E>, List<E>> decision) {
        return new DcbInvocation<>(criteria, null, decision);
    }

    /** An invocation that runs {@code decision} inside {@code criteria}, tagging its events with {@code tagGenerator}. */
    public static <E> DcbInvocation<E> to(DcbCriteria criteria, TagGenerator<E> tagGenerator, Function<List<E>, List<E>> decision) {
        return new DcbInvocation<>(criteria, requireNonNull(tagGenerator, "tagGenerator cannot be null"), decision);
    }

    /**
     * Only the criteria, because the decision is a lambda whose generated {@code toString} is a synthetic class name
     * that would otherwise fill every assertion failure and dispatch log line.
     */
    @Override
    public String toString() {
        return "DcbInvocation[criteria=" + criteria + "]";
    }
}
