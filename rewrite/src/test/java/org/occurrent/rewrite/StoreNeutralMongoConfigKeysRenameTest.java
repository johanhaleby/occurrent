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
 * Covers the four MongoDB-only keys (occurrent.event-store.collection, occurrent.event-store.time-representation,
 * occurrent.subscription.collection, occurrent.subscription.restart-on-change-stream-history-lost) renaming to their
 * mongodb-qualified equivalents. Unlike {@link SubscriptionModePropertyRenameTest}, no value transformation is
 * involved, so each is a plain key rename, and the case that matters per key is the one where a file already sets
 * both the deprecated and the mongodb-qualified key.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class StoreNeutralMongoConfigKeysRenameTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/store-neutral-mongodb-config-0_34.yml", "org.occurrent.MigrateStoreNeutralMongoConfigKeys_0_34");
    }

    @Test
    void event_store_collection_is_renamed_in_properties() {
        rewriteRun(
                properties(
                        "occurrent.event-store.collection=events-v2",
                        "occurrent.event-store.mongodb.collection=events-v2"
                )
        );
    }

    // ChangePropertyKey renames the leaf key in place rather than expanding it into a nested mapping, so
    // "mongodb.collection" lands as one dotted YAML key alongside collection's old siblings, not as a nested
    // "mongodb:" block. Spring's relaxed binding flattens either shape into the same "...mongodb.collection"
    // property name, so this is a difference in file layout, not in what the key resolves to.
    @Test
    void event_store_collection_is_renamed_in_yaml() {
        rewriteRun(
                yaml(
                        """
                        occurrent:
                          event-store:
                            collection: events-v2
                        """,
                        """
                        occurrent:
                          event-store:
                            mongodb.collection: events-v2
                        """
                )
        );
    }

    @Test
    void event_store_time_representation_is_renamed_in_yaml() {
        rewriteRun(
                yaml(
                        """
                        occurrent:
                          event-store:
                            time-representation: RFC_3339_STRING
                        """,
                        """
                        occurrent:
                          event-store:
                            mongodb.time-representation: RFC_3339_STRING
                        """
                )
        );
    }

    @Test
    void subscription_collection_is_renamed_in_yaml() {
        rewriteRun(
                yaml(
                        """
                        occurrent:
                          subscription:
                            collection: subscriptions-v2
                        """,
                        """
                        occurrent:
                          subscription:
                            mongodb.collection: subscriptions-v2
                        """
                )
        );
    }

    @Test
    void restart_on_change_stream_history_lost_is_renamed_in_yaml() {
        rewriteRun(
                yaml(
                        """
                        occurrent:
                          subscription:
                            restart-on-change-stream-history-lost: false
                        """,
                        """
                        occurrent:
                          subscription:
                            mongodb.restart-on-change-stream-history-lost: false
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
                            collection: checkpoints
                            enabled: false
                        """,
                        """
                        occurrent:
                          subscription:
                            mongodb.collection: checkpoints
                            enabled: false
                        """
                )
        );
    }

    @Test
    void a_file_setting_both_the_deprecated_and_the_mongodb_qualified_key_keeps_only_the_new_one() {
        rewriteRun(
                properties(
                        """
                        occurrent.event-store.mongodb.collection=events-v2
                        occurrent.event-store.collection=events-v1
                        """,
                        """
                        occurrent.event-store.mongodb.collection=events-v2
                        """
                ),
                yaml(
                        """
                        occurrent:
                          subscription:
                            mongodb:
                              restart-on-change-stream-history-lost: false
                            restart-on-change-stream-history-lost: true
                        """,
                        """
                        occurrent:
                          subscription:
                            mongodb:
                              restart-on-change-stream-history-lost: false
                        """
                )
        );
    }

    @Test
    void a_multi_document_file_where_one_profile_sets_the_deprecated_key_and_another_sets_the_mongodb_qualified_key() {
        // Regression test for the same document-scoping defect #828 found in DropRedundantSubscriptionEnabledInYaml_0_32:
        // each DropRedundant*InYaml_0_34 recipe must check the mongodb-qualified key per document, not per file. The
        // default profile below sets only the deprecated key and has no mongodb-qualified key of its own; the prod
        // profile sets only the mongodb-qualified key. A file-wide precondition would see the prod profile's key and
        // delete the deprecated key from the default profile before it gets a chance to be renamed, dropping the
        // default profile's only key and, with it, the whole document.
        rewriteRun(
                yaml(
                        """
                        occurrent:
                          event-store:
                            collection: events-v1
                        ---
                        spring:
                          config:
                            activate:
                              on-profile: prod
                        occurrent:
                          event-store:
                            mongodb.collection: events-v2
                        """,
                        """
                        occurrent:
                          event-store:
                            mongodb.collection: events-v1
                        ---
                        spring:
                          config:
                            activate:
                              on-profile: prod
                        occurrent:
                          event-store:
                            mongodb.collection: events-v2
                        """
                )
        );
    }

    @Test
    void a_file_setting_none_of_the_four_keys_is_untouched() {
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
