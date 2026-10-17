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

/**
 * The projection feeds as 0.34.0 declares them, with the deprecated {@code accept(..)} next to its
 * {@code acceptAndWait(..)} replacement, handed to the parser as compiled dependencies. The source under test is a
 * 0.33.0 caller of {@code accept(..)}, compiled against these.
 */
final class ProjectionFeedStubs {

    private ProjectionFeedStubs() {
    }

    static final String EVENT_METADATA = """
            package org.occurrent.cloudevents;

            public class EventMetadata {
            }
            """;

    static final String MONO = """
            package reactor.core.publisher;

            public abstract class Mono<T> {
            }
            """;

    static final String BLOCKING_DOMAIN_EVENT_FEED = blockingFeed("org.occurrent.dsl.projection.blocking", "DomainEventFeed");

    static final String BLOCKING_CATCHUP_PROJECTION_FEED = blockingFeed("org.occurrent.dsl.projection.blocking", "CatchupProjectionFeed");

    static final String REACTOR_DOMAIN_EVENT_FEED = reactorFeed("org.occurrent.dsl.projection.reactor", "DomainEventFeed");

    static final String REACTOR_CATCHUP_PROJECTION_FEED = reactorFeed("org.occurrent.dsl.projection.reactor", "CatchupProjectionFeed");

    /**
     * The blocking {@code PushSubscriptionModel}, whose {@code accept(..)} takes a CloudEvent and is not renamed. The
     * parameter is simplified to {@code Object} to keep the CloudEvent library out of the stubs.
     */
    static final String PUSH_SUBSCRIPTION_MODEL = """
            package org.occurrent.subscription.push.blocking;

            public class PushSubscriptionModel {
                public void accept(Object cloudEvent) {
                }
            }
            """;

    private static String blockingFeed(String packageName, String simpleName) {
        return """
                package %s;

                import org.occurrent.cloudevents.EventMetadata;

                public class %s<E> {
                    @Deprecated
                    public void accept(E event) {
                    }

                    @Deprecated
                    public void accept(EventMetadata metadata, E event) {
                    }

                    public void acceptAndWait(E event) {
                    }

                    public void acceptAndWait(EventMetadata metadata, E event) {
                    }
                }
                """.formatted(packageName, simpleName);
    }

    private static String reactorFeed(String packageName, String simpleName) {
        return """
                package %s;

                import org.occurrent.cloudevents.EventMetadata;
                import reactor.core.publisher.Mono;

                public class %s<E> {
                    @Deprecated
                    public Mono<Void> accept(E event) {
                        return null;
                    }

                    @Deprecated
                    public Mono<Void> accept(EventMetadata metadata, E event) {
                        return null;
                    }

                    public Mono<Void> acceptAndWait(E event) {
                        return null;
                    }

                    public Mono<Void> acceptAndWait(EventMetadata metadata, E event) {
                        return null;
                    }
                }
                """.formatted(packageName, simpleName);
    }
}
