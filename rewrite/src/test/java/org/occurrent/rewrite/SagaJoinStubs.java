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
package org.occurrent.rewrite;

/**
 * The 0.33.0 flow-saga types, shaped after the real ones, for the {@code join}-removal migration tests. Handed to the
 * parser as a compiled dependency rather than as rewritten sources, the same reason {@code SagaTimerNameStubs} exists:
 * the source under test is a 0.33.0 caller, unchanged, meeting the classpath the 0.34.0 jar gives it (minus the
 * removed {@code join}/{@code Expectation}, which stay here since a "before" source still calls them).
 */
final class SagaJoinStubs {

    private SagaJoinStubs() {
    }

    static final String CONTINUATION = """
            package org.occurrent.dsl.saga.flow;

            public interface Continuation {
                static Continuation next() {
                    return null;
                }

                static Continuation end() {
                    return null;
                }
            }
            """;

    static final String RECEIVED_EVENTS = """
            package org.occurrent.dsl.saga.flow;

            public interface ReceivedEvents<E> {
            }
            """;

    static final String STEP_CONDITION = """
            package org.occurrent.dsl.saga.flow;

            public interface StepCondition<E> {
                static <E, T extends E> StepCondition<E> event(Class<T> eventType) {
                    return null;
                }

                static <E, T extends E> StepCondition<E> event(Class<T> eventType, int count) {
                    return null;
                }

                @SafeVarargs
                static <E> StepCondition<E> allOf(StepCondition<? extends E> first, StepCondition<? extends E>... rest) {
                    return null;
                }
            }
            """;

    static final String EXPECTATION = """
            package org.occurrent.dsl.saga.flow;

            public final class Expectation<E> {
                public static <E> Expectation<E> of(Class<? extends E> eventType) {
                    return null;
                }

                public static <E> Expectation<E> of(Class<? extends E> eventType, int count) {
                    return null;
                }
            }
            """;

    static final String STEP_BUILDER = """
            package org.occurrent.dsl.saga.flow;

            import java.util.List;
            import java.util.function.Function;

            public final class StepBuilder<E, C> {
                public StepBuilder<E, C> join(List<Expectation<E>> expecting, Continuation then,
                                               Function<ReceivedEvents<E>, List<C>> whenFulfilled) {
                    return this;
                }

                public StepBuilder<E, C> join(List<Expectation<E>> expecting, Continuation then) {
                    return this;
                }

                public StepBuilder<E, C> on(StepCondition<? extends E> condition, Continuation then) {
                    return this;
                }

                public StepBuilder<E, C> on(StepCondition<? extends E> condition, Continuation then,
                                             Function<ReceivedEvents<E>, List<C>> whenFulfilled) {
                    return this;
                }
            }
            """;
}
