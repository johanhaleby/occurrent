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

class AnnotationEnumRenameTest extends AnnotationEnumRenamesRecipeTest {

    @Override
    public void defaults(RecipeSpec spec) {
        // The stubbed annotations self-reference their own renamed nested enum as a member type; see the base class
        // constant.
        super.defaults(spec);
        spec.typeValidationOptions(RELAXED_FOR_SELF_REFERENCING_RENAME);
    }

    @Test
    void subscriptionResumeBehaviorReferenceIsRewrittenFromNestedToTopLevel() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.annotation;

                        public @interface Subscription {
                            ResumeBehavior resumeBehavior() default ResumeBehavior.DEFAULT;

                            enum ResumeBehavior {
                                DEFAULT, SAME_AS_START_AT
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.annotation.Subscription;

                        @Subscription(resumeBehavior = Subscription.ResumeBehavior.DEFAULT)
                        class Foo {
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.annotation.ResumeBehavior;
                        import org.occurrent.annotation.Subscription;

                        @Subscription(resumeBehavior = ResumeBehavior.DEFAULT)
                        class Foo {
                        }
                        """
                )
        );
    }

    @Test
    void streamSubscriptionStartupModeReferenceIsRewrittenFromNestedToTopLevel() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.annotation;

                        public @interface StreamSubscription {
                            StartupMode startupMode() default StartupMode.DEFAULT;

                            enum StartupMode {
                                DEFAULT, WAIT_UNTIL_STARTED, BACKGROUND
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.annotation.StreamSubscription;

                        @StreamSubscription(startupMode = StreamSubscription.StartupMode.BACKGROUND)
                        class Bar {
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.annotation.StartupMode;
                        import org.occurrent.annotation.StreamSubscription;

                        @StreamSubscription(startupMode = StartupMode.BACKGROUND)
                        class Bar {
                        }
                        """
                )
        );
    }
}
