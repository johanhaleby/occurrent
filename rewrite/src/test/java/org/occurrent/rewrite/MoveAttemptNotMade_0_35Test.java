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
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class MoveAttemptNotMade_0_35Test implements RewriteTest {

    private static final String OLD_ATTEMPT_NOT_MADE = """
            package org.occurrent.retry.internal;

            public final class RetryExecution {
                public static final class AttemptNotMade extends RuntimeException {
                    public AttemptNotMade(String message) {
                        super(message);
                    }
                }
            }
            """;

    private static final String NEW_ATTEMPT_NOT_MADE = """
            package org.occurrent.retry;

            public final class AttemptNotMade extends RuntimeException {
                public AttemptNotMade(String message) {
                    super(message);
                }
            }
            """;

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/attempt-not-made-0_35.yml", "org.occurrent.MoveAttemptNotMade_0_35");
    }

    @Test
    void importedNestedAttemptNotMadeIsMovedInThrowAndCatch() {
        rewriteRun(
                java(OLD_ATTEMPT_NOT_MADE),
                java(NEW_ATTEMPT_NOT_MADE),
                java(
                        """
                        package com.example;

                        import org.occurrent.retry.internal.RetryExecution.AttemptNotMade;

                        class Foo {
                            void attempt(boolean ready) {
                                try {
                                    if (!ready) {
                                        throw new AttemptNotMade("not ready");
                                    }
                                } catch (AttemptNotMade e) {
                                    System.out.println(e.getMessage());
                                }
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.retry.AttemptNotMade;

                        class Foo {
                            void attempt(boolean ready) {
                                try {
                                    if (!ready) {
                                        throw new AttemptNotMade("not ready");
                                    }
                                } catch (AttemptNotMade e) {
                                    System.out.println(e.getMessage());
                                }
                            }
                        }
                        """
                )
        );
    }

    @Test
    void referenceQualifiedByRetryExecutionIsMoved() {
        rewriteRun(
                java(OLD_ATTEMPT_NOT_MADE),
                java(NEW_ATTEMPT_NOT_MADE),
                java(
                        """
                        package com.example;

                        import org.occurrent.retry.internal.RetryExecution;

                        class Foo {
                            RetryExecution.AttemptNotMade last;

                            void attempt() {
                                last = new RetryExecution.AttemptNotMade("not ready");
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.retry.AttemptNotMade;

                        class Foo {
                            AttemptNotMade last;

                            void attempt() {
                                last = new AttemptNotMade("not ready");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void upgradeToOccurrent_0_35IncludesTheMove() {
        rewriteRun(
                spec -> spec.recipe(Environment.builder()
                        .scanRuntimeClasspath("org.occurrent")
                        .build()
                        .activateRecipes("org.occurrent.UpgradeToOccurrent_0_35")),
                java(OLD_ATTEMPT_NOT_MADE),
                java(NEW_ATTEMPT_NOT_MADE),
                java(
                        """
                        package com.example;

                        import org.occurrent.retry.internal.RetryExecution.AttemptNotMade;

                        class Foo {
                            void attempt() {
                                throw new AttemptNotMade("not ready");
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.retry.AttemptNotMade;

                        class Foo {
                            void attempt() {
                                throw new AttemptNotMade("not ready");
                            }
                        }
                        """
                )
        );
    }
}
