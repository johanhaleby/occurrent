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

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.occurrent.rewrite.SagaJoinStubs.*;
import static org.openrewrite.java.Assertions.java;
import static org.openrewrite.kotlin.Assertions.kotlin;

/**
 * Every case here is a real upgrade: the "before" source is 0.33.0 code written against the deprecated
 * {@code join}/{@code Expectation}, unchanged, meeting the classpath {@link SagaJoinStubs} stands in for.
 */
class MigrateSagaJoinToStepConditionTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new MigrateSagaJoinToStepCondition())
                .parser(JavaParser.fromJavaVersion().dependsOn(CONTINUATION, RECEIVED_EVENTS, STEP_CONDITION, EXPECTATION, STEP_BUILDER));
    }

    @Test
    void rewritesASingleExpectationJoinWithAReaction() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.flow.Continuation;
                        import org.occurrent.dsl.saga.flow.Expectation;
                        import org.occurrent.dsl.saga.flow.StepBuilder;

                        import java.util.List;

                        class Steps {
                            void configure(StepBuilder<Event, Command> step) {
                                step.join(List.of(Expectation.of(PlayerReady.class, 2)), Continuation.end(), received -> List.of());
                            }

                            interface Event {
                            }

                            static class PlayerReady implements Event {
                            }

                            interface Command {
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.flow.Continuation;
                        import org.occurrent.dsl.saga.flow.StepBuilder;
                        import org.occurrent.dsl.saga.flow.StepCondition;

                        import java.util.List;

                        class Steps {
                            void configure(StepBuilder<Event, Command> step) {
                                step.on(StepCondition.allOf(StepCondition.event(PlayerReady.class, 2)), Continuation.end(), received -> List.of());
                            }

                            interface Event {
                            }

                            static class PlayerReady implements Event {
                            }

                            interface Command {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void rewritesATwoArgumentJoinWithNoReaction() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.flow.Continuation;
                        import org.occurrent.dsl.saga.flow.Expectation;
                        import org.occurrent.dsl.saga.flow.StepBuilder;

                        import java.util.List;

                        class Steps {
                            void configure(StepBuilder<Event, Command> step) {
                                step.join(List.of(Expectation.of(PlayerReady.class, 2)), Continuation.end());
                            }

                            interface Event {
                            }

                            static class PlayerReady implements Event {
                            }

                            interface Command {
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.flow.Continuation;
                        import org.occurrent.dsl.saga.flow.StepBuilder;
                        import org.occurrent.dsl.saga.flow.StepCondition;

                        import java.util.List;

                        class Steps {
                            void configure(StepBuilder<Event, Command> step) {
                                step.on(StepCondition.allOf(StepCondition.event(PlayerReady.class, 2)), Continuation.end());
                            }

                            interface Event {
                            }

                            static class PlayerReady implements Event {
                            }

                            interface Command {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void rewritesSeveralDistinctTypedExpectationsInFirstAppearanceOrder() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.flow.Continuation;
                        import org.occurrent.dsl.saga.flow.Expectation;
                        import org.occurrent.dsl.saga.flow.StepBuilder;

                        import java.util.List;

                        class Steps {
                            void configure(StepBuilder<Event, Command> step) {
                                step.join(List.of(Expectation.of(Ready.class), Expectation.of(Note.class)), Continuation.end());
                            }

                            interface Event {
                            }

                            static class Ready implements Event {
                            }

                            static class Note implements Event {
                            }

                            interface Command {
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.flow.Continuation;
                        import org.occurrent.dsl.saga.flow.StepBuilder;
                        import org.occurrent.dsl.saga.flow.StepCondition;

                        import java.util.List;

                        class Steps {
                            void configure(StepBuilder<Event, Command> step) {
                                step.on(StepCondition.allOf(StepCondition.event(Ready.class), StepCondition.event(Note.class)), Continuation.end());
                            }

                            interface Event {
                            }

                            static class Ready implements Event {
                            }

                            static class Note implements Event {
                            }

                            interface Command {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void collapsesADuplicateTypedPairOfLiteralCountsToTheHigherOne() {
        // The exact shape FlowSagaTest exercises: two Ready expectations (1 and 3) in one join, which join's own
        // (removed) toConditions collapsed to the higher count. A naive per-element translation would instead emit
        // an allOf with two children matching the same events, which allOf refuses at build time.
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.flow.Continuation;
                        import org.occurrent.dsl.saga.flow.Expectation;
                        import org.occurrent.dsl.saga.flow.StepBuilder;

                        import java.util.List;

                        class Steps {
                            void configure(StepBuilder<Event, Command> step) {
                                step.join(List.of(Expectation.of(Ready.class, 1), Expectation.of(Note.class, 1), Expectation.of(Ready.class, 3)),
                                        Continuation.end());
                            }

                            interface Event {
                            }

                            static class Ready implements Event {
                            }

                            static class Note implements Event {
                            }

                            interface Command {
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.flow.Continuation;
                        import org.occurrent.dsl.saga.flow.StepBuilder;
                        import org.occurrent.dsl.saga.flow.StepCondition;

                        import java.util.List;

                        class Steps {
                            void configure(StepBuilder<Event, Command> step) {
                                step.on(StepCondition.allOf(StepCondition.event(Ready.class, 3), StepCondition.event(Note.class, 1)), Continuation.end());
                            }

                            interface Event {
                            }

                            static class Ready implements Event {
                            }

                            static class Note implements Event {
                            }

                            interface Command {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAJoinAloneWhenTheExpectingListIsNotALiteral() {
        // The recipe cannot see what a variable or a method call contains, so it leaves this alone. It will not
        // compile once join is removed, but that is the compiler pointing at it, not this recipe.
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.flow.Continuation;
                        import org.occurrent.dsl.saga.flow.Expectation;
                        import org.occurrent.dsl.saga.flow.StepBuilder;

                        import java.util.List;

                        class Steps {
                            void configure(StepBuilder<Event, Command> step, List<Expectation<Event>> expecting) {
                                step.join(expecting, Continuation.end());
                            }

                            interface Event {
                            }

                            interface Command {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAJoinAloneWhenADuplicateTypedCountIsNotLiteral() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.flow.Continuation;
                        import org.occurrent.dsl.saga.flow.Expectation;
                        import org.occurrent.dsl.saga.flow.StepBuilder;

                        import java.util.List;

                        class Steps {
                            void configure(StepBuilder<Event, Command> step, int extra) {
                                step.join(List.of(Expectation.of(Ready.class, 1), Expectation.of(Ready.class, extra)), Continuation.end());
                            }

                            interface Event {
                            }

                            static class Ready implements Event {
                            }

                            interface Command {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAKotlinJoinAlone() {
        // A self-contained Kotlin fixture shaped after the real StepScope.join (vararg expectations, a named then,
        // an optional trailing lambda). rewrite-kotlin roots a Kotlin file at K.CompilationUnit, not J.CompilationUnit,
        // so this recipe never even visits it. No change expected.
        rewriteRun(
                kotlin(
                        """
                        package com.example

                        interface Event
                        class PlayerReady : Event
                        interface Command

                        class Expectation<E>
                        class ReceivedEvents<E>

                        class Continuation private constructor() {
                            companion object {
                                fun end(): Continuation = Continuation()
                            }
                        }

                        class StepScope<E, C> {
                            fun <T : E> expect(count: Int = 1): Expectation<E> = Expectation()

                            fun join(
                                expecting: Expectation<E>,
                                vararg more: Expectation<E>,
                                then: Continuation,
                                whenFulfilled: (ReceivedEvents<E>) -> List<C> = { emptyList() }
                            ) {
                            }
                        }

                        class Steps {
                            fun configure(step: StepScope<Event, Command>) {
                                step.join(step.expect<PlayerReady>(2), then = Continuation.end()) { emptyList() }
                            }
                        }
                        """
                )
        );
    }
}
