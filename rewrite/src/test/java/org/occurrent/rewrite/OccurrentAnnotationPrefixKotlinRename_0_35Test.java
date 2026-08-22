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
 * Proves the annotation prefix renames also rewrite Kotlin sources, not only Java, which is what the 0.35.0
 * migration guide promises. The renames are declarative ChangeType, which is language-agnostic once the Kotlin
 * parser is on the classpath, the same way {@link AnnotationEnumKotlinRenameTest} shows for the 0.31.0 recipe.
 */
class OccurrentAnnotationPrefixKotlinRename_0_35Test implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/renames-annotations-0_35.yml",
                        "org.occurrent.MigrateOccurrentAnnotationRenames_0_35")
                // Type attribution on stubbed types is looser under the Kotlin parser than the real compiled types,
                // the printed source is what matters for a real run.
                .typeValidationOptions(TypeValidation.builder().identifiers(false).methodInvocations(false).build());
    }

    @Test
    void projectionAnnotationGetsTheOccurrentPrefixInKotlin() {
        rewriteRun(
                kotlin(
                        """
                        package org.occurrent.annotation
                        annotation class Projection(val id: String)
                        """
                ),
                kotlin(
                        """
                        package com.example

                        import org.occurrent.annotation.Projection

                        class OrderStatusProjection {
                            @Projection(id = "orderStatus")
                            fun orderStatus(): Any? = null
                        }
                        """,
                        """
                        package com.example

                        import org.occurrent.annotation.OccurrentProjection

                        class OrderStatusProjection {
                            @OccurrentProjection(id = "orderStatus")
                            fun orderStatus(): Any? = null
                        }
                        """
                )
        );
    }

    @Test
    void subscriptionAnnotationGetsTheOccurrentPrefixInKotlin() {
        rewriteRun(
                kotlin(
                        """
                        package org.occurrent.annotation
                        annotation class Subscription(val id: String)
                        """
                ),
                kotlin(
                        """
                        package com.example

                        import org.occurrent.annotation.Subscription

                        class Notifications {
                            @Subscription(id = "notifyCustomer")
                            fun notifyCustomer(event: Any) {
                            }
                        }
                        """,
                        """
                        package com.example

                        import org.occurrent.annotation.OccurrentSubscription

                        class Notifications {
                            @OccurrentSubscription(id = "notifyCustomer")
                            fun notifyCustomer(event: Any) {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void streamSubscriptionAndItsNestedStartPositionGetTheOccurrentPrefixInKotlin() {
        rewriteRun(
                // ChangeType cannot tell the renamed nested enum from the declaration it was told to leave alone,
                // so the stub's own kinds no longer match its type information. Same stubbing artifact the Java
                // test relaxes.
                spec -> spec.typeValidationOptions(TypeValidation.builder()
                        .identifiers(false)
                        .methodInvocations(false)
                        .classDeclarations(false)
                        .build()),
                // The stub stands in for the 0.34.0 annotation, and it is the one file that also references the
                // nested enum from inside the declaration being renamed. An application never has that file, the
                // call site below is what a reader of this test should compare against.
                kotlin(
                        """
                        package org.occurrent.annotation
                        annotation class StreamSubscription(val id: String, val startAt: StartPosition = StartPosition.DEFAULT) {
                            enum class StartPosition {
                                BEGINNING_OF_TIME, NOW, DEFAULT
                            }
                        }
                        """,
                        """
                        package org.occurrent.annotation
                        annotation class StreamSubscription(val id: String, val startAt: OccurrentStreamSubscription.StartPosition = StartPosition.DEFAULT) {
                            enum class StartPosition {
                                BEGINNING_OF_TIME, NOW, DEFAULT
                            }
                        }
                        """
                ),
                kotlin(
                        """
                        package com.example

                        import org.occurrent.annotation.StreamSubscription

                        class Notifications {
                            @StreamSubscription(id = "notifyCustomer", startAt = StreamSubscription.StartPosition.BEGINNING_OF_TIME)
                            fun notifyCustomer(event: Any) {
                            }
                        }
                        """,
                        """
                        package com.example

                        import org.occurrent.annotation.OccurrentStreamSubscription
                        import org.occurrent.annotation.OccurrentStreamSubscription.StartPosition

                        class Notifications {
                            @OccurrentStreamSubscription(id = "notifyCustomer", startAt = OccurrentStreamSubscription.StartPosition.BEGINNING_OF_TIME)
                            fun notifyCustomer(event: Any) {
                            }
                        }
                        """
                )
        );
    }
}
