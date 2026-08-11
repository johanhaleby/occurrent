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
import org.openrewrite.test.TypeValidation;

import static org.occurrent.rewrite.SagaTimerNameStubs.*;
import static org.openrewrite.java.Assertions.java;

/**
 * Every case here is a real upgrade. The 0.33.0 timer types arrive through the parser's classpath, the way they do
 * once a build picks up the new jar, and the source in the "before" block is the 0.32.0 code that was written
 * against the old ones. Declaring the new shape in the source instead would only prove that already-migrated code
 * is left alone.
 */
class MigrateSagaTimerNameTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/saga-timer-name-0_33.yml",
                        "org.occurrent.MigrateSagaTimerName_0_33")
                .parser(JavaParser.fromJavaVersion().dependsOn(TIMER_NAME, SAGA_TIMEOUT, SAGA_EFFECT));
    }

    // A 0.32.0 constructor call does not resolve against the 0.33.0 constructor, which is the whole reason it
    // needs migrating, so the parsed call carries no constructor type. Validating it would only assert that the
    // source this recipe exists for does not compile.
    private static final TypeValidation UNRESOLVED_CONSTRUCTOR =
            TypeValidation.builder().constructorInvocations(false).build();

    @Test
    void readsTheStringHandedToTheSagaTimeoutConstructorThroughParse() {
        rewriteRun(
                spec -> spec.typeValidationOptions(UNRESOLVED_CONSTRUCTOR),
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        class FireTimer {
                            SagaTimeout paymentTimedOut() {
                                return new SagaTimeout("order-1", "payment");
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;
                        import org.occurrent.dsl.saga.TimerName;

                        class FireTimer {
                            SagaTimeout paymentTimedOut() {
                                return new SagaTimeout("order-1", TimerName.parse("payment"));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void readsANamespacedNameAndAStringVariableThroughParseToo() {
        // The namespaced literal is what proves parse is the right rewrite rather than a wrapper that changes
        // meaning. A flow step's timer was named by writing the prefix out, and parse gives back the qualified name
        // that string already was, so the timer keeps matching the step that armed it. Any String expression counts,
        // not only a literal, since parse reads them all the same way.
        rewriteRun(
                spec -> spec.typeValidationOptions(UNRESOLVED_CONSTRUCTOR),
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        class FireStepTimer {
                            SagaTimeout awaitingPlayersTimedOut() {
                                return new SagaTimeout("game-1", "step:awaiting-players");
                            }

                            SagaTimeout timedOut(String timerName) {
                                return new SagaTimeout("game-1", timerName);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;
                        import org.occurrent.dsl.saga.TimerName;

                        class FireStepTimer {
                            SagaTimeout awaitingPlayersTimedOut() {
                                return new SagaTimeout("game-1", TimerName.parse("step:awaiting-players"));
                            }

                            SagaTimeout timedOut(String timerName) {
                                return new SagaTimeout("game-1", TimerName.parse(timerName));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAConstructorThatAlreadyTakesATimerNameAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;
                        import org.occurrent.dsl.saga.TimerName;

                        class AlreadyMigrated {
                            SagaTimeout paymentTimedOut() {
                                return new SagaTimeout("order-1", TimerName.of("step", "awaiting-players"));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void encodesAnAccessorReadIntoADeclaredStringVariable() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        class ReadName {
                            void handle(SagaTimeout timeout) {
                                String name = timeout.timerName();
                                System.out.println(name);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        class ReadName {
                            void handle(SagaTimeout timeout) {
                                String name = timeout.timerName().encode();
                                System.out.println(name);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void encodesAnAccessorAssignedToAStringField() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        class RememberName {
                            private String lastTimer;

                            void handle(SagaTimeout timeout) {
                                lastTimer = timeout.timerName();
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        class RememberName {
                            private String lastTimer;

                            void handle(SagaTimeout timeout) {
                                lastTimer = timeout.timerName().encode();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void encodesAnAccessorReturnedFromAStringMethod() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        class NameOf {
                            String nameOf(SagaTimeout timeout) {
                                return timeout.timerName();
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        class NameOf {
                            String nameOf(SagaTimeout timeout) {
                                return timeout.timerName().encode();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAReturnFromALambdaToTheReviewComment() {
        // The enclosing method returns a String, but the return belongs to the lambda, which does not. Reading the
        // method's return type here would append encode() to a name the lambda hands back as a TimerName.
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        import java.util.function.Supplier;

                        class NameSupplier {
                            String nameOf(SagaTimeout timeout) {
                                Supplier<Object> name = () -> {
                                    return timeout.timerName();
                                };
                                return name.get().toString();
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        import java.util.function.Supplier;

                        class NameSupplier {
                            String nameOf(SagaTimeout timeout) {
                                Supplier<Object> name = () -> {
                                    return /* TODO [Occurrent 0.33 upgrade]: timerName() is a TimerName now, call encode() for the string. See doc/migration/upgrading-to-0.33.0.md. */ timeout.timerName();
                                };
                                return name.get().toString();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void marksAnAccessorWhoseWantedTypeIsNotWrittenDown() {
        // An argument position. The parameter here takes an Object, so the call still compiles and now prints the
        // same text through toString, and a caller reading it as a String does not compile at all. The recipe
        // cannot tell those apart, so it says so rather than guessing.
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        class LogName {
                            void handle(SagaTimeout timeout) {
                                System.out.println(timeout.timerName());
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        class LogName {
                            void handle(SagaTimeout timeout) {
                                System.out.println(/* TODO [Occurrent 0.33 upgrade]: timerName() is a TimerName now, call encode() for the string. See doc/migration/upgrading-to-0.33.0.md. */ timeout.timerName());
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAnAlreadyMarkedAccessorAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        class LogName {
                            void handle(SagaTimeout timeout) {
                                System.out.println(/* TODO [Occurrent 0.33 upgrade]: timerName() is a TimerName now, call encode() for the string. See doc/migration/upgrading-to-0.33.0.md. */ timeout.timerName());
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAnAccessorAlreadyReadThroughEncodeAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaTimeout;

                        class ReadName {
                            void handle(SagaTimeout timeout) {
                                String name = timeout.timerName().encode();
                                System.out.println(name);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesARecordPatternForAHuman() {
        // A record pattern's binding type is a judgement about what the code inside the case then does with the
        // name, so the recipe does not touch it and the compiler points at every one. Section 7 of the migration
        // guide covers it by hand.
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.saga.SagaEffect;

                        class ReadEffect {
                            String nameOf(SagaEffect<String> effect) {
                                if (effect instanceof SagaEffect.CancelTimeout<String>(String name)) {
                                    return name;
                                }
                                return null;
                            }
                        }
                        """
                )
        );
    }
}
