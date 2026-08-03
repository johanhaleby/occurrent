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

import static org.openrewrite.properties.Assertions.properties;
import static org.openrewrite.yaml.Assertions.yaml;

/**
 * Verifies the umbrella {@code UpgradeToOccurrent_0_32} recipe resolves its sub-recipe through a classpath-scanning
 * Environment, which is what proves the cross-file recipe reference actually links. The transforms themselves are
 * covered in {@link SubscriptionModePropertyRenameTest}, so one case per format is enough here.
 */
class UpgradeToOccurrent_0_32Test implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(Environment.builder()
                .scanRuntimeClasspath("org.occurrent")
                .build()
                .activateRecipes("org.occurrent.UpgradeToOccurrent_0_32"));
    }

    @Test
    void migratesTheSubscriptionModePropertyInProperties() {
        rewriteRun(
                properties(
                        "occurrent.subscription.enabled=false",
                        "occurrent.subscription.mode=disabled"
                )
        );
    }

    @Test
    void migratesTheSubscriptionModePropertyInYaml() {
        rewriteRun(
                yaml(
                        """
                        occurrent:
                          subscription:
                            enabled: true
                        """,
                        """
                        occurrent:
                          subscription:
                            mode: auto
                        """
                )
        );
    }
}
