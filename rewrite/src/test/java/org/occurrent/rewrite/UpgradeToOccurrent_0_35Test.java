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
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;
import org.openrewrite.test.TypeValidation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.rewrite.AppendResultStubs.APPEND_ID;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.CLOUD_EVENT;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.DCB_APPEND_RESULT;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.DCB_CONSISTENCY_TOKEN;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.DCB_EVENT_STREAM;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.DCB_MARKER_VERSIONS;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.DCB_READ_OPTIONS;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.POSITION_RANGE;
import static org.openrewrite.java.Assertions.java;

/**
 * Verifies the {@code UpgradeToOccurrent_0_35} recipe resolves its sub-recipes through a classpath-scanning
 * Environment, which is what proves the cross-file recipe references actually link. What each sub-recipe does is
 * covered in {@link MigrateDcbMarkerVersionsRecordPatternTest}, so one case per sub-recipe is enough here. Add a case
 * per sub-recipe as they are added.
 * <p>
 * Identifier type validation is off for the same reason as in {@link MigrateDcbMarkerVersionsRecordPatternTest}.
 */
class UpgradeToOccurrent_0_35Test implements RewriteTest {

    private static final String DCB_MARKER_VERSIONS_RECORD_PATTERN = "org.occurrent.MigrateDcbMarkerVersionsRecordPattern_0_35";

    @Override
    public void defaults(RecipeSpec spec) {
        spec.parser(JavaParser.fromJavaVersion().dependsOn(APPEND_ID, CLOUD_EVENT, POSITION_RANGE, DCB_CONSISTENCY_TOKEN,
                        DCB_MARKER_VERSIONS, DCB_APPEND_RESULT, DCB_EVENT_STREAM, DCB_READ_OPTIONS))
                .typeValidationOptions(TypeValidation.builder().identifiers(false).build());
    }

    @Test
    void theRecipeLoadsAndValidates() {
        Recipe recipe = activate("org.occurrent.UpgradeToOccurrent_0_35");

        assertThat(recipe.validate().isValid()).isTrue();
        assertThat(recipe.getName()).isEqualTo("org.occurrent.UpgradeToOccurrent_0_35");
    }

    @Test
    void theRecipeListsTheMarkerVersionsRecordPatternSubRecipe() {
        Recipe recipe = activate("org.occurrent.UpgradeToOccurrent_0_35");

        assertThat(recipe.getRecipeList()).extracting(Recipe::getName).contains(DCB_MARKER_VERSIONS_RECORD_PATTERN);
    }

    @Test
    void theMarkerVersionsRecordPatternSubRecipeLoadsAndValidates() {
        Recipe recipe = activate(DCB_MARKER_VERSIONS_RECORD_PATTERN);

        assertThat(recipe.validate().isValid()).isTrue();
        assertThat(recipe.getName()).isEqualTo(DCB_MARKER_VERSIONS_RECORD_PATTERN);
        assertThat(recipe.getRecipeList()).extracting(Recipe::getName)
                .containsExactly("org.occurrent.rewrite.MigrateDcbMarkerVersionsRecordPattern");
    }

    @Test
    void addsTheMarkerVersionsBindingToADcbAppendResultPattern() {
        rewriteRun(
                spec -> spec.recipe(activate("org.occurrent.UpgradeToOccurrent_0_35")),
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbAppendResult(var first, var last, var count, var appendId) -> first + ".." + last + " (" + count + ")";
                                    default -> "unknown";
                                };
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbAppendResult(var first, var last, var count, var appendId, var markerVersions) -> first + ".." + last + " (" + count + ")";
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    /**
     * A caller who skipped 0.34.0 runs both umbrellas in one invocation. A {@code DcbAppendResult} pattern of the
     * 0.33.0 arity of three must come out at the 0.35.0 arity of five, the 0.34.0 recipe adding the append id and the
     * 0.35.0 recipe the marker versions, in the order the umbrellas are given.
     */
    @Test
    void aThreeBindingDcbAppendResultPatternEndsWithFiveBindingsWhenBothUmbrellasRunInOneInvocation() {
        rewriteRun(
                spec -> spec.recipe(activate("org.occurrent.UpgradeToOccurrent_0_34", "org.occurrent.UpgradeToOccurrent_0_35")),
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbAppendResult(var first, var last, var count) -> first + ".." + last + " (" + count + ")";
                                    default -> "unknown";
                                };
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbAppendResult(var first, var last, var count, var appendId, var markerVersions) -> first + ".." + last + " (" + count + ")";
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    private static Recipe activate(String... recipeNames) {
        return Environment.builder()
                .scanRuntimeClasspath("org.occurrent")
                .build()
                .activateRecipes(recipeNames);
    }
}
