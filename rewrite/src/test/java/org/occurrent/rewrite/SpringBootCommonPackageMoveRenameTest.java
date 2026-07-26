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
 * Proves the 0.31.0 relocation of the Spring Boot annotation machinery out of
 * {@code org.occurrent.springboot.mongo.common} and into {@code org.occurrent.springboot.common} is rewritten for
 * both Java and Kotlin callers. The machinery is no longer MongoDB-specific, see issue #409.
 */
class SpringBootCommonPackageMoveRenameTest extends AnnotationEnumRenamesRecipeTest {

    @Override
    public void defaults(RecipeSpec spec) {
        super.defaults(spec);
        spec.typeValidationOptions(TypeValidation.builder().identifiers(false).methodInvocations(false).build());
    }

    @Test
    void occurrentPropertiesMovesFromSpringbootMongoCommonToSpringbootCommonPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.mongo.common;
                        public class OccurrentProperties {
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.mongo.common.OccurrentProperties;

                        class Foo {
                            OccurrentProperties properties;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.springboot.common.OccurrentProperties;

                        class Foo {
                            OccurrentProperties properties;
                        }
                        """
                )
        );
    }

    @Test
    void occurrentPropertiesMovesFromSpringbootMongoCommonToSpringbootCommonPackageInKotlin() {
        rewriteRun(
                kotlin(
                        """
                        package org.occurrent.springboot.mongo.common
                        class OccurrentProperties
                        """
                ),
                kotlin(
                        """
                        package com.example

                        import org.occurrent.springboot.mongo.common.OccurrentProperties

                        class Foo {
                            var properties: OccurrentProperties? = null
                        }
                        """,
                        """
                        package com.example

                        import org.occurrent.springboot.common.OccurrentProperties

                        class Foo {
                            var properties: OccurrentProperties? = null
                        }
                        """
                )
        );
    }
}
