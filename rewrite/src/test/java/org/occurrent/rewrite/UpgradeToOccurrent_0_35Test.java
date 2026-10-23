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
import org.openrewrite.Recipe;
import org.openrewrite.config.Environment;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@code UpgradeToOccurrent_0_35} recipe loads through a classpath-scanning Environment and passes
 * validation. It has no sub-recipes yet, so there is nothing to rewrite. Add a case per sub-recipe here as they
 * are added.
 */
class UpgradeToOccurrent_0_35Test {

    @Test
    void theRecipeLoadsAndValidates() {
        Recipe recipe = Environment.builder()
                .scanRuntimeClasspath("org.occurrent")
                .build()
                .activateRecipes("org.occurrent.UpgradeToOccurrent_0_35");

        assertThat(recipe.validate().isValid()).isTrue();
        assertThat(recipe.getName()).isEqualTo("org.occurrent.UpgradeToOccurrent_0_35");
    }
}
