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
import org.openrewrite.test.RewriteTest;

import static org.occurrent.rewrite.StreamWriteStubs.*;
import static org.openrewrite.java.Assertions.java;

class StreamToListTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/stream-to-list.yml", "org.occurrent.MigrateStreamToList_0_30")
                .typeValidationOptions(STUB_ONLY_VALIDATION);
    }

    @Test
    void rewritesStreamOfToListOfInWriteArgument() {
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

    @Test
    void rewritesStreamEmptyToCollectionsEmptyListInWriteArgument() {
        rewriteRun(
                java(CLOUD_EVENT),
                java(EVENT_STORE),
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.blocking.EventStore;
                        import java.util.stream.Stream;

                        class Foo {
                            void run(EventStore eventStore) {
                                eventStore.write("id", Stream.empty());
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.blocking.EventStore;

                        import java.util.Collections;

                        class Foo {
                            void run(EventStore eventStore) {
                                eventStore.write("id", Collections.emptyList());
                            }
                        }
                        """
                )
        );
    }

    @Test
    void flagsANonLiteralStreamWriteArgumentWithAReviewComment() {
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
                            void run(EventStore eventStore, Stream<CloudEvent> events) {
                                eventStore.write("id", events);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import io.cloudevents.CloudEvent;
                        import org.occurrent.eventstore.api.blocking.EventStore;
                        import java.util.stream.Stream;

                        class Foo {
                            void run(EventStore eventStore, Stream<CloudEvent> events) {
                                /* TODO [Occurrent 0.30 upgrade]: EventStore.write(...) now takes List<CloudEvent> instead of Stream<CloudEvent>. Convert this argument (and any Stream operations feeding it) to a List manually. */
                                eventStore.write("id", events);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAnAlreadyMigratedListWriteArgumentUntouched() {
        rewriteRun(
                java(CLOUD_EVENT),
                java(EVENT_STORE),
                // No change expected: the argument is already a List, so it is neither rewritten nor flagged.
                java(
                        """
                        package com.example;

                        import io.cloudevents.CloudEvent;
                        import org.occurrent.eventstore.api.blocking.EventStore;
                        import java.util.List;

                        class Foo {
                            void run(EventStore eventStore, CloudEvent e1) {
                                eventStore.write("id", List.of(e1));
                            }
                        }
                        """
                )
        );
    }

    @Test
    void retypesStreamCommandCompositionToListCommandComposition() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.application.composition.command;
                        public final class StreamCommandComposition {}
                        """
                ),
                java(
                        """
                        package org.occurrent.application.composition.command;
                        public final class ListCommandComposition {}
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.application.composition.command.StreamCommandComposition;

                        class Foo {
                            Class<?> type() {
                                return StreamCommandComposition.class;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.application.composition.command.ListCommandComposition;

                        class Foo {
                            Class<?> type() {
                                return ListCommandComposition.class;
                            }
                        }
                        """
                )
        );
    }
}
