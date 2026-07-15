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
 * Base for the per-family tests of the {@code MigrateOccurrentRenames_0_31} recipe: they all activate the same
 * recipe from the same resource, so it is wired once here. A subclass whose stub self-references its own renamed
 * type (the annotation declaring both the nested enum and a member of that type) must relax type validation with
 * {@link #RELAXED_FOR_SELF_REFERENCING_RENAME}.
 */
abstract class AnnotationEnumRenamesRecipeTest implements RewriteTest {

    /**
     * The old and new simple names are identical here (both are called {@code ResumeBehavior}/{@code StartupMode});
     * only the enclosing scope moves from nested to top-level. A stub that references its own nested enum as an
     * annotation member type has no way to know, on the test classpath, that the two are actually different declared
     * types, so identifier/class-declaration kind validation is relaxed for the tests that hit it. See
     * {@link CheckpointStorageConfigRenameTest} for the same rationale on the 0.30.0 recipe's nested-type case.
     */
    protected static final TypeValidation RELAXED_FOR_SELF_REFERENCING_RENAME =
            TypeValidation.builder().identifiers(false).classDeclarations(false).build();

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/annotation-enums.yml", "org.occurrent.MigrateOccurrentRenames_0_31");
    }
}
