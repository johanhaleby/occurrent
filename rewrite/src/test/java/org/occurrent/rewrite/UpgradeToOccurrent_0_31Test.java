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
import org.openrewrite.test.TypeValidation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.openrewrite.java.Assertions.java;
import static org.openrewrite.maven.Assertions.pomXml;

/**
 * Verifies the umbrella {@code UpgradeToOccurrent_0_31} recipe resolves its sub-recipes through a classpath-scanning
 * Environment, which is what proves the cross-file recipe references actually link. The individual transforms are
 * covered exhaustively in {@link AnnotationEnumRenameTest}, {@link AnnotationEnumKotlinRenameTest}, and
 * {@link CoordinateRename_0_31Test}; here one rename per sub-recipe is enough to show the umbrella recipe runs them.
 */
class UpgradeToOccurrent_0_31Test implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(Environment.builder()
                        .scanRuntimeClasspath("org.occurrent")
                        .build()
                        .activateRecipes("org.occurrent.UpgradeToOccurrent_0_31"))
                .typeValidationOptions(TypeValidation.builder().identifiers(false).classDeclarations(false).build());
    }

    @Test
    void appliesTheAnnotationEnumRenameFromItsSubRecipe() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.annotation;

                        public @interface Projection {
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

                        import org.occurrent.annotation.Projection;

                        @Projection(resumeBehavior = Projection.ResumeBehavior.DEFAULT)
                        class Foo {
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.annotation.Projection;
                        import org.occurrent.annotation.ResumeBehavior;

                        @Projection(resumeBehavior = ResumeBehavior.DEFAULT)
                        class Foo {
                        }
                        """
                )
        );
    }

    @Test
    void appliesTheCheckpointStorageCoordinateRenameFromItsSubRecipe() {
        rewriteRun(
                pomXml(
                        """
                        <project>
                            <modelVersion>4.0.0</modelVersion>
                            <groupId>com.example</groupId>
                            <artifactId>app</artifactId>
                            <version>1.0.0</version>
                            <dependencies>
                                <dependency>
                                    <groupId>org.occurrent</groupId>
                                    <artifactId>occurrent-subscription-mongodb-native-blocking-position-storage</artifactId>
                                    <version>0.30.0</version>
                                </dependency>
                            </dependencies>
                        </project>
                        """,
                        spec -> spec.after(actual -> {
                            assertThat(actual)
                                    .contains("<artifactId>occurrent-subscription-mongodb-native-blocking-checkpoint-storage</artifactId>")
                                    .doesNotContain("position-storage");
                            return actual;
                        })
                )
        );
    }
}
