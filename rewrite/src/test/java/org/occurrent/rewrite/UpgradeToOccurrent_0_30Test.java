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

import static org.occurrent.rewrite.StreamWriteStubs.*;
import static org.openrewrite.java.Assertions.java;

/**
 * Verifies the umbrella recipe resolves and composes both sub-recipes, which live in separate resource files
 * (occurrent.yml and stream-to-list.yml). Activating it through a classpath-scanning Environment is what proves
 * the cross-file recipe references actually link. The individual transforms are covered exhaustively in the
 * per-recipe tests; here one rename and one Stream-to-List rewrite are enough to show both halves ran.
 */
class UpgradeToOccurrent_0_30Test implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(Environment.builder()
                        .scanRuntimeClasspath("org.occurrent")
                        .build()
                        .activateRecipes("org.occurrent.UpgradeToOccurrent_0_30"))
                .typeValidationOptions(STUB_ONLY_VALIDATION);
    }

    @Test
    void appliesARenameFromTheFirstRecipe() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription;
                        public interface SubscriptionPosition {}
                        """
                ),
                java(
                        """
                        package org.occurrent.subscription;
                        public interface Checkpoint {}
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.SubscriptionPosition;

                        class Foo {
                            Class<?> type() {
                                return SubscriptionPosition.class;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;

                        class Foo {
                            Class<?> type() {
                                return Checkpoint.class;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void appliesAStreamToListRewriteFromTheSecondRecipe() {
        rewriteRun(
                java(CLOUD_EVENT),
                java(EVENT_STORE),
                java(
                        """
                        package com.example;

                        import io.cloudevents.CloudEvent;
                        import org.occurrent.eventstore.api.blocking.EventStore;
                        import java.util.stream.Stream;

                        class Foo {
                            void run(EventStore eventStore, CloudEvent e1, CloudEvent e2) {
                                eventStore.write("id", Stream.of(e1, e2));
                            }
                        }
                        """,
                        """
                        package com.example;

                        import io.cloudevents.CloudEvent;
                        import org.occurrent.eventstore.api.blocking.EventStore;

                        import java.util.List;

                        class Foo {
                            void run(EventStore eventStore, CloudEvent e1, CloudEvent e2) {
                                eventStore.write("id", List.of(e1, e2));
                            }
                        }
                        """
                )
        );
    }
}
