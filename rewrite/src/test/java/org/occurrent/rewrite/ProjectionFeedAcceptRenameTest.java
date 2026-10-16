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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.openrewrite.config.Environment;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.occurrent.rewrite.ProjectionFeedStubs.*;
import static org.openrewrite.java.Assertions.java;

/**
 * Verifies the {@code UpgradeToOccurrent_0_34} recipe renames {@code accept(..)} to {@code acceptAndWait(..)} on the
 * blocking and reactor {@code DomainEventFeed} and {@code CatchupProjectionFeed}, in both overloads, and leaves an
 * {@code accept(..)} on any other type alone.
 */
class ProjectionFeedAcceptRenameTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(Environment.builder()
                        .scanRuntimeClasspath("org.occurrent")
                        .build()
                        .activateRecipes("org.occurrent.UpgradeToOccurrent_0_34"))
                .parser(JavaParser.fromJavaVersion().dependsOn(
                        EVENT_METADATA, MONO,
                        BLOCKING_DOMAIN_EVENT_FEED, BLOCKING_CATCHUP_PROJECTION_FEED,
                        REACTOR_DOMAIN_EVENT_FEED, REACTOR_CATCHUP_PROJECTION_FEED,
                        PUSH_SUBSCRIPTION_MODEL));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            "org.occurrent.dsl.projection.blocking.DomainEventFeed",
            "org.occurrent.dsl.projection.blocking.CatchupProjectionFeed",
            "org.occurrent.dsl.projection.reactor.DomainEventFeed",
            "org.occurrent.dsl.projection.reactor.CatchupProjectionFeed"
    })
    void renamesBothAcceptOverloadsToAcceptAndWait(String feedType) {
        rewriteRun(
                java(
                        callingFeed(feedType, "accept"),
                        callingFeed(feedType, "acceptAndWait")
                )
        );
    }

    @Test
    void leavesAcceptOnOtherTypesAloneNextToARenamedFeedCall() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.cloudevents.EventMetadata;
                        import org.occurrent.dsl.projection.blocking.DomainEventFeed;
                        import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

                        import java.util.function.Consumer;

                        class Listener {
                            void onEvent(DomainEventFeed<Object> feed, PushSubscriptionModel model, Consumer<Object> consumer, EventMetadata metadata, Object event) {
                                feed.accept(event);
                                feed.accept(metadata, event);
                                model.accept(event);
                                consumer.accept(event);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.cloudevents.EventMetadata;
                        import org.occurrent.dsl.projection.blocking.DomainEventFeed;
                        import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

                        import java.util.function.Consumer;

                        class Listener {
                            void onEvent(DomainEventFeed<Object> feed, PushSubscriptionModel model, Consumer<Object> consumer, EventMetadata metadata, Object event) {
                                feed.acceptAndWait(event);
                                feed.acceptAndWait(metadata, event);
                                model.accept(event);
                                consumer.accept(event);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void changesNothingWhenAcceptIsOnlyCalledOnOtherTypes() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.push.blocking.PushSubscriptionModel;

                        import java.util.function.Consumer;

                        class Listener {
                            void onEvent(PushSubscriptionModel model, Consumer<Object> consumer, Object event) {
                                model.accept(event);
                                consumer.accept(event);
                            }
                        }
                        """
                )
        );
    }

    private static String callingFeed(String feedType, String method) {
        String simpleName = feedType.substring(feedType.lastIndexOf('.') + 1);
        return """
                package com.example;

                import org.occurrent.cloudevents.EventMetadata;
                import %s;

                class Listener {
                    void onEvent(%s<Object> feed, EventMetadata metadata, Object event) {
                        feed.%s(event);
                        feed.%s(metadata, event);
                    }
                }
                """.formatted(feedType, simpleName, method, method);
    }
}
