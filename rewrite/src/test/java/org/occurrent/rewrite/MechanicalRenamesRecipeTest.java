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

import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;
import org.openrewrite.test.TypeValidation;

/**
 * Base for the per-family tests of the {@code MigrateOccurrentRenames_0_30} recipe: they all activate the same recipe
 * from the same resource, so it is wired once here. A subclass whose stub self-references its own renamed type must
 * relax type validation with {@link #RELAXED_FOR_SELF_REFERENCING_RENAME}.
 */
abstract class MechanicalRenamesRecipeTest implements RewriteTest {

    /**
     * A stubbed type that self-references its own (renamed) type as a return/parameter type trips ChangeType: it has no
     * way to know the synthetic renamed type is an interface (nothing declares it on the test classpath), so it defaults
     * the kind to Class, which conflicts with the untouched ({@code ignoreDefinition=true}) declaration. That mismatch
     * is an artifact of stubbing rather than using the real compiled 0.30.0 types, so identifier/class-declaration kind
     * validation is relaxed for the tests that hit it.
     */
    protected static final TypeValidation RELAXED_FOR_SELF_REFERENCING_RENAME =
            TypeValidation.builder().identifiers(false).classDeclarations(false).build();

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/occurrent.yml", "org.occurrent.MigrateOccurrentRenames_0_30");
    }
}
