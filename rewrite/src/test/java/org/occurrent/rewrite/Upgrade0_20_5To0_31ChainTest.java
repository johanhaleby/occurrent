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

import static org.openrewrite.java.Assertions.java;

/**
 * Proves that a user upgrading straight from Occurrent 0.20.5, who runs both the {@code UpgradeToOccurrent_0_30} and
 * {@code UpgradeToOccurrent_0_31} umbrella recipes, lands on the final 0.31 package rather than stalling on the
 * intermediate 0.30 one. {@code OccurrentProperties} has a 0.30 rename (still 0.20.5 -> 0.30.0, {@code
 * org.occurrent.springboot.mongo.blocking} -> {@code org.occurrent.springboot.mongo.common}) chained into a 0.31
 * rename (0.30.0 -> 0.31.0, {@code org.occurrent.springboot.mongo.common} -> {@code org.occurrent.springboot.common}),
 * so this is the one case in the whole rewrite module where two independently-shipped umbrella recipes must compose
 * for a type.
 *
 * <p>The same two-hop shape exists on paper for the Spring Boot autoconfigure module coordinate
 * ({@code MigrateCoordinates_0_30} maps {@code spring-boot-autoconfigure-mongodb-common} to
 * {@code occurrent-mongodb-spring-boot-autoconfigure}, and {@code MigrateCoordinates_0_31} maps that to
 * {@code occurrent-spring-boot-autoconfigure}), but it cannot be exercised here: {@code
 * spring-boot-autoconfigure-mongodb-common} was never published to Maven Central under any version. Its module was
 * only introduced during 0.30.0 development, in commit 7838fb02b ("Extract stack-neutral Occurrent autoconfiguration
 * into a shared module (#256)"), which post-dates the {@code occurrent-0.20.5} tag, and it was renamed to {@code
 * occurrent-mongodb-spring-boot-autoconfigure} before the 0.30.0 release was ever cut, so the coordinate the 0.30
 * recipe renames from never existed as a real dependency a user could declare. A {@code pomXml} test needs the
 * "before" coordinate to resolve against a real repository, which is impossible here; the composition is instead
 * covered at the single-hop level by {@link CoordinateRename_0_31Test} and the equivalent 0.30 test.
 */
class Upgrade0_20_5To0_31ChainTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(Environment.builder()
                        .scanRuntimeClasspath("org.occurrent")
                        .build()
                        .activateRecipes("org.occurrent.UpgradeToOccurrent_0_30", "org.occurrent.UpgradeToOccurrent_0_31"))
                .typeValidationOptions(TypeValidation.builder().identifiers(false).methodInvocations(false).build());
    }

    @Test
    void occurrentPropertiesConvergesFromThe0_20_5PackageToTheFinal0_31PackageInOneInvocation() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.mongo.blocking;
                        public class OccurrentProperties {
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.mongo.blocking.OccurrentProperties;

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
}
