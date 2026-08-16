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
import org.openrewrite.config.Environment;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.occurrent.rewrite.SagaJoinStubs.*;
import static org.openrewrite.java.Assertions.java;

/**
 * Verifies the umbrella {@code UpgradeToOccurrent_0_34} recipe resolves its one sub-recipe through a
 * classpath-scanning Environment, which is what proves the cross-file recipe reference actually links. What the
 * sub-recipe does is covered in {@link MigrateSagaJoinToStepConditionTest}, so one case is enough here.
 */
class UpgradeToOccurrent_0_34Test implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(Environment.builder()
                        .scanRuntimeClasspath("org.occurrent")
                        .build()
                        .activateRecipes("org.occurrent.UpgradeToOccurrent_0_34"))
                .parser(JavaParser.fromJavaVersion().dependsOn(CONTINUATION, RECEIVED_EVENTS, STEP_CONDITION, EXPECTATION, STEP_BUILDER));
    }

    @Test
    void rewritesASingleExpectationJoin() {
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
}
