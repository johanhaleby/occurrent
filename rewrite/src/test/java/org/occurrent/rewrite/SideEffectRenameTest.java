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

import static org.openrewrite.java.Assertions.java;

class SideEffectRenameTest extends MechanicalRenamesRecipeTest {

    @Override
    public void defaults(RecipeSpec spec) {
        // The stubbed PolicySideEffect interface self-references its own renamed type; see the base class constant.
        super.defaults(spec);
        spec.typeValidationOptions(RELAXED_FOR_SELF_REFERENCING_RENAME);
    }

    @Test
    void policySideEffectIsRenamedToSideEffectAndExecutePolicyToExecuteSideEffect() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.application.service.blocking;
                        public interface PolicySideEffect {
                            void executePolicy();
                            PolicySideEffect andThenExecuteAnotherPolicy(PolicySideEffect other);
                        }
                        """,
                        """
                        package org.occurrent.application.service.blocking;
                        public interface PolicySideEffect {
                            void executeSideEffect();
                            SideEffect andThenExecuteAnotherSideEffect(SideEffect other);
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.application.service.blocking.PolicySideEffect;

                        class Foo {
                            void run(PolicySideEffect sideEffect) {
                                sideEffect.executePolicy();
                                sideEffect.andThenExecuteAnotherPolicy(sideEffect);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.application.service.blocking.SideEffect;

                        class Foo {
                            void run(SideEffect sideEffect) {
                                sideEffect.executeSideEffect();
                                sideEffect.andThenExecuteAnotherSideEffect(sideEffect);
                            }
                        }
                        """
                )
        );
    }
}
