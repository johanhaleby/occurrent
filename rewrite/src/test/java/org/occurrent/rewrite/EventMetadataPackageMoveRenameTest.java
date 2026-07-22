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

import static org.openrewrite.java.Assertions.java;
import static org.openrewrite.kotlin.Assertions.kotlin;

/**
 * Proves the 0.31.0 relocation of {@code EventMetadata} out of {@code org.occurrent.dsl.subscription} and into
 * {@code org.occurrent.cloudevents} is rewritten for both Java and Kotlin callers.
 */
class EventMetadataPackageMoveRenameTest extends AnnotationEnumRenamesRecipeTest {

    @Override
    public void defaults(RecipeSpec spec) {
        super.defaults(spec);
        spec.typeValidationOptions(TypeValidation.builder().identifiers(false).methodInvocations(false).build());
    }

    @Test
    void eventMetadataMovesFromDslSubscriptionToCloudeventsPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.dsl.subscription;
                        public class EventMetadata {
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.subscription.EventMetadata;

                        class Foo {
                            EventMetadata metadata;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.cloudevents.EventMetadata;

                        class Foo {
                            EventMetadata metadata;
                        }
                        """
                )
        );
    }

    @Test
    void eventMetadataMovesFromDslSubscriptionToCloudeventsPackageInKotlin() {
        rewriteRun(
                kotlin(
                        """
                        package org.occurrent.dsl.subscription
                        class EventMetadata
                        """
                ),
                kotlin(
                        """
                        package com.example

                        import org.occurrent.dsl.subscription.EventMetadata

                        class Foo {
                            var metadata: EventMetadata? = null
                        }
                        """,
                        """
                        package com.example

                        import org.occurrent.cloudevents.EventMetadata

                        class Foo {
                            var metadata: EventMetadata? = null
                        }
                        """
                )
        );
    }
}
