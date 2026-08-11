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
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;
import org.openrewrite.test.TypeValidation;

import static org.openrewrite.kotlin.Assertions.kotlin;

/**
 * Proves the recipe is Java only, and so proves the claim section 7 of the migration guide makes about a Kotlin
 * caller. rewrite-kotlin builds a Kotlin call out of the same J nodes the Java LST uses, so a type check alone would
 * match one, and the templates this recipe inserts are Java syntax. A Kotlin caller does the same two edits by hand,
 * see doc/migration/upgrading-to-0.33.0.md.
 */
class MigrateSagaTimerNameKotlinTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/saga-timer-name-0_33.yml",
                        "org.occurrent.MigrateSagaTimerName_0_33")
                // Type attribution on stubbed types is looser under the Kotlin parser than the real compiled types;
                // the printed source is what matters for a real run, so relax kind validation here.
                .typeValidationOptions(TypeValidation.builder().identifiers(false).methodInvocations(false).build());
    }

    @Test
    void leavesAKotlinCallerOfTheTwoStringConstructorUntouched() {
        rewriteRun(
                kotlin(
                        """
                        package org.occurrent.dsl.saga

                        interface TimerName {
                            fun encode(): String
                        }
                        """
                ),
                kotlin(
                        """
                        package org.occurrent.dsl.saga

                        data class SagaTimeout(val sagaId: String, val timerName: TimerName)
                        """
                ),
                kotlin(
                        """
                        package com.example

                        import org.occurrent.dsl.saga.SagaTimeout

                        fun paymentTimedOut(timeout: SagaTimeout): String {
                            val fired = SagaTimeout("order-1", "payment")
                            return timeout.timerName
                        }
                        """
                )
        );
    }

    @Test
    void leavesAKotlinCallerOfTheDirectTimerEffectConstructorsUntouched() {
        rewriteRun(
                kotlin(
                        """
                        package org.occurrent.dsl.saga

                        interface TimerName {
                            fun encode(): String
                        }
                        """
                ),
                kotlin(
                        """
                        package org.occurrent.dsl.saga

                        sealed interface SagaEffect<C> {
                            data class CancelTimeout<C>(val timerName: TimerName) : SagaEffect<C>
                        }
                        """
                ),
                kotlin(
                        """
                        package com.example

                        import org.occurrent.dsl.saga.SagaEffect

                        fun paymentCancelled(): SagaEffect.CancelTimeout<Any> {
                            return SagaEffect.CancelTimeout("payment")
                        }
                        """
                )
        );
    }
}
