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
import org.openrewrite.test.TypeValidation;

import static org.openrewrite.kotlin.Assertions.kotlin;

/**
 * Proves the mechanical-renames recipe also rewrites Kotlin sources, not only Java. The rename recipes are declarative
 * ChangeType/ChangeMethodName, which are language-agnostic once the Kotlin parser is on the classpath.
 */
class KotlinRenameTest extends MechanicalRenamesRecipeTest {

    @Override
    public void defaults(RecipeSpec spec) {
        super.defaults(spec);
        // Type attribution on stubbed types is looser under the Kotlin parser than the real compiled types; the printed
        // source is what matters for a real run, so relax kind validation here.
        spec.typeValidationOptions(TypeValidation.builder().identifiers(false).methodInvocations(false).build());
    }

    @Test
    void subscriptionPositionIsRenamedToCheckpointInKotlin() {
        rewriteRun(
                // The declaration stays unchanged (the recipe uses ignoreDefinition: true); only the reference is rewritten.
                kotlin(
                        """
                        package org.occurrent.subscription
                        interface SubscriptionPosition
                        """
                ),
                kotlin(
                        """
                        package com.example

                        import org.occurrent.subscription.SubscriptionPosition

                        class Foo {
                            fun use(p: SubscriptionPosition) {
                            }
                        }
                        """,
                        """
                        package com.example

                        import org.occurrent.subscription.Checkpoint

                        class Foo {
                            fun use(p: Checkpoint) {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void checkpointAwareCloudEventInstanceMethodIsRenamedInKotlin() {
        rewriteRun(
                kotlin(
                        """
                        package org.occurrent.subscription
                        interface CheckpointAwareCloudEvent {
                            fun getSubscriptionPosition(): Any?
                        }
                        """,
                        """
                        package org.occurrent.subscription
                        interface CheckpointAwareCloudEvent {
                            fun getCheckpoint(): Any?
                        }
                        """
                ),
                kotlin(
                        """
                        package com.example

                        import org.occurrent.subscription.CheckpointAwareCloudEvent

                        class Foo {
                            fun read(e: CheckpointAwareCloudEvent): Any? = e.getSubscriptionPosition()
                        }
                        """,
                        """
                        package com.example

                        import org.occurrent.subscription.CheckpointAwareCloudEvent

                        class Foo {
                            fun read(e: CheckpointAwareCloudEvent): Any? = e.getCheckpoint()
                        }
                        """
                )
        );
    }
}
