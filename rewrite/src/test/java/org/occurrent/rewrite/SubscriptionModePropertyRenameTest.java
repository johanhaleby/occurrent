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

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.properties.Assertions.properties;
import static org.openrewrite.yaml.Assertions.yaml;

/**
 * Covers the {@code occurrent.subscription.enabled} to {@code occurrent.subscription.mode} migration in both
 * configuration formats. The rename is value-dependent, {@code false} becoming {@code disabled} and {@code true}
 * becoming {@code auto}, so the cases that matter are the ones where a plain key rename would produce a file that no
 * longer binds.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SubscriptionModePropertyRenameTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/subscription-mode-0_32.yml", "org.occurrent.MigrateSubscriptionModeProperty_0_32");
    }

    @Test
    void false_becomes_disabled_in_properties() {
        rewriteRun(
                properties(
                        "occurrent.subscription.enabled=false",
                        "occurrent.subscription.mode=disabled"
                )
        );
    }

    @Test
    void true_becomes_auto_in_properties() {
        rewriteRun(
                properties(
                        "occurrent.subscription.enabled=true",
                        "occurrent.subscription.mode=auto"
                )
        );
    }

    @Test
    void false_becomes_disabled_in_yaml_at_the_same_depth() {
        rewriteRun(
                yaml(
                        """
                        occurrent:
                          subscription:
                            enabled: false
                        """,
                        """
                        occurrent:
                          subscription:
                            mode: disabled
                        """
                )
        );
    }

    @Test
    void true_becomes_auto_in_yaml() {
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

    @Test
    void a_sibling_property_under_the_same_prefix_is_left_alone() {
        rewriteRun(
                yaml(
                        """
                        occurrent:
                          subscription:
                            enabled: false
                            collection: checkpoints
                        """,
                        """
                        occurrent:
                          subscription:
                            mode: disabled
                            collection: checkpoints
                        """
                )
        );
    }

    @Test
    void a_different_enabled_property_is_left_alone() {
        rewriteRun(
                properties("occurrent.event-store.enabled=false"),
                yaml(
                        """
                        occurrent:
                          event-store:
                            enabled: false
                        """
                )
        );
    }

    @Test
    void a_value_that_cannot_be_mapped_keeps_the_deprecated_key() {
        // Neither true nor false, so there is no mode to rewrite it to. Leaving the whole entry alone is safe because
        // the deprecated key still works, where renaming only the key would leave a value that does not bind.
        rewriteRun(
                properties("occurrent.subscription.enabled=${SUBSCRIPTIONS_ON}"),
                yaml(
                        """
                        occurrent:
                          subscription:
                            enabled: ${SUBSCRIPTIONS_ON}
                        """
                )
        );
    }

    @Test
    void a_file_setting_both_keys_keeps_only_the_new_one() {
        rewriteRun(
                properties(
                        """
                        occurrent.subscription.mode=manual
                        occurrent.subscription.enabled=true
                        """,
                        """
                        occurrent.subscription.mode=manual
                        """
                ),
                yaml(
                        """
                        occurrent:
                          subscription:
                            mode: manual
                            enabled: true
                        """,
                        """
                        occurrent:
                          subscription:
                            mode: manual
                        """
                )
        );
    }

    @Test
    void the_false_half_leaves_a_profile_holding_the_other_value_untouched() {
        // Regression test for #834: MigrateSubscriptionEnabledFalseInYaml_0_32's precondition used to be checked
        // over the whole file, so the default profile below setting enabled: false licensed ChangePropertyKey to
        // rename enabled to mode in the prod profile too, even though ChangePropertyValue correctly left the prod
        // profile's true value alone. The prod profile ended up with mode: true, an enum that does not bind to a
        // boolean.
        rewriteRun(
                spec -> spec.recipeFromResource(
                        "/META-INF/rewrite/subscription-mode-0_32.yml",
                        "org.occurrent.MigrateSubscriptionEnabledFalseInYaml_0_32"),
                yaml(
                        """
                        occurrent:
                          subscription:
                            enabled: false
                        ---
                        spring:
                          config:
                            activate:
                              on-profile: prod
                        occurrent:
                          subscription:
                            enabled: true
                        """,
                        """
                        occurrent:
                          subscription:
                            mode: disabled
                        ---
                        spring:
                          config:
                            activate:
                              on-profile: prod
                        occurrent:
                          subscription:
                            enabled: true
                        """
                )
        );
    }

    @Test
    void the_true_half_leaves_a_profile_holding_the_other_value_untouched() {
        // Regression test for #834, the mirror of the false half above: a document setting enabled: true used to
        // license the key rename in a profile that set enabled: false, leaving that profile with mode: false.
        rewriteRun(
                spec -> spec.recipeFromResource(
                        "/META-INF/rewrite/subscription-mode-0_32.yml",
                        "org.occurrent.MigrateSubscriptionEnabledTrueInYaml_0_32"),
                yaml(
                        """
                        occurrent:
                          subscription:
                            enabled: true
                        ---
                        spring:
                          config:
                            activate:
                              on-profile: prod
                        occurrent:
                          subscription:
                            enabled: false
                        """,
                        """
                        occurrent:
                          subscription:
                            mode: auto
                        ---
                        spring:
                          config:
                            activate:
                              on-profile: prod
                        occurrent:
                          subscription:
                            enabled: false
                        """
                )
        );
    }

    @Test
    void a_multi_document_file_where_one_profile_sets_the_deprecated_key_and_another_sets_the_new_key() {
        // Regression test for #828: DropRedundantSubscriptionEnabledInYaml_0_32 must check the replacement key
        // per document, not per file. The default profile below sets only the deprecated key and has no mode of
        // its own; the prod profile sets only the new key. A file-wide precondition would see the prod profile's
        // mode and delete enabled from the default profile before it gets a chance to be renamed, dropping the
        // default profile's only key and, with it, the whole document.
        rewriteRun(
                yaml(
                        """
                        occurrent:
                          subscription:
                            enabled: false
                        ---
                        spring:
                          config:
                            activate:
                              on-profile: prod
                        occurrent:
                          subscription:
                            mode: manual
                        """,
                        """
                        occurrent:
                          subscription:
                            mode: disabled
                        ---
                        spring:
                          config:
                            activate:
                              on-profile: prod
                        occurrent:
                          subscription:
                            mode: manual
                        """
                )
        );
    }

    @Test
    void a_file_setting_neither_key_is_untouched() {
        rewriteRun(
                properties("occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:example"),
                yaml(
                        """
                        occurrent:
                          cloud-event-converter:
                            cloud-event-source: urn:occurrent:example
                        """
                )
        );
    }
}
