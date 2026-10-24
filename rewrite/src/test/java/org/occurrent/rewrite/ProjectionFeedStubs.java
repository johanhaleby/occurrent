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
 * The projection feeds as three releases declare them, handed to the parser as compiled dependencies. The
 * plain constants are the 0.34.0 feeds, with the deprecated {@code accept(..)} next to its {@code acceptAndWait(..)}
 * replacement. The {@code _0_33} constants declare {@code accept(..)} only. The {@code _0_35} constants declare
 * {@code acceptAndWait(..)} and {@code acceptOrDefer(..)} and no {@code accept(..)}. A parser takes one version at a
 * time, since the versions share their type names.
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

    static final String BLOCKING_DOMAIN_EVENT_FEED_0_33 = blockingFeed0_33("org.occurrent.dsl.projection.blocking", "DomainEventFeed");

    static final String BLOCKING_CATCHUP_PROJECTION_FEED_0_33 = blockingFeed0_33("org.occurrent.dsl.projection.blocking", "CatchupProjectionFeed");

    static final String REACTOR_DOMAIN_EVENT_FEED_0_33 = reactorFeed0_33("org.occurrent.dsl.projection.reactor", "DomainEventFeed");

    static final String REACTOR_CATCHUP_PROJECTION_FEED_0_33 = reactorFeed0_33("org.occurrent.dsl.projection.reactor", "CatchupProjectionFeed");

    static final String ROUTING_OUTCOME = """
            package org.occurrent.subscription;

            public enum RoutingOutcome {
                DELIVERED, FILTERED, DEFERRED, UNAVAILABLE, NOT_DELIVERABLE, REFUSED
            }
            """;

    static final String BLOCKING_DOMAIN_EVENT_FEED_0_35 = blockingFeed0_35("org.occurrent.dsl.projection.blocking", "DomainEventFeed");

    static final String BLOCKING_CATCHUP_PROJECTION_FEED_0_35 = blockingFeed0_35("org.occurrent.dsl.projection.blocking", "CatchupProjectionFeed");

    static final String REACTOR_DOMAIN_EVENT_FEED_0_35 = reactorFeed0_35("org.occurrent.dsl.projection.reactor", "DomainEventFeed");

    static final String REACTOR_CATCHUP_PROJECTION_FEED_0_35 = reactorFeed0_35("org.occurrent.dsl.projection.reactor", "CatchupProjectionFeed");

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

    private static String blockingFeed0_33(String packageName, String simpleName) {
        return """
                package %s;

                import org.occurrent.cloudevents.EventMetadata;

                public final class %s<E> {
                    public void accept(E event) {
                    }

                    public void accept(EventMetadata metadata, E event) {
                    }
                }
                """.formatted(packageName, simpleName);
    }

    private static String reactorFeed0_33(String packageName, String simpleName) {
        return """
                package %s;

                import org.occurrent.cloudevents.EventMetadata;
                import reactor.core.publisher.Mono;

                public final class %s<E> {
                    public Mono<Void> accept(E event) {
                        return null;
                    }

                    public Mono<Void> accept(EventMetadata metadata, E event) {
                        return null;
                    }
                }
                """.formatted(packageName, simpleName);
    }

    private static String blockingFeed0_35(String packageName, String simpleName) {
        return """
                package %s;

                import org.occurrent.cloudevents.EventMetadata;
                import org.occurrent.subscription.RoutingOutcome;

                public final class %s<E> {
                    public void acceptAndWait(E event) {
                    }

                    public void acceptAndWait(EventMetadata metadata, E event) {
                    }

                    public RoutingOutcome acceptOrDefer(E event) {
                        return null;
                    }

                    public RoutingOutcome acceptOrDefer(EventMetadata metadata, E event) {
                        return null;
                    }
                }
                """.formatted(packageName, simpleName);
    }

    private static String reactorFeed0_35(String packageName, String simpleName) {
        return """
                package %s;

                import org.occurrent.cloudevents.EventMetadata;
                import org.occurrent.subscription.RoutingOutcome;
                import reactor.core.publisher.Mono;

                public final class %s<E> {
                    public Mono<Void> acceptAndWait(E event) {
                        return null;
                    }

                    public Mono<Void> acceptAndWait(EventMetadata metadata, E event) {
                        return null;
                    }

                    public Mono<RoutingOutcome> acceptOrDefer(E event) {
                        return null;
                    }

                    public Mono<RoutingOutcome> acceptOrDefer(EventMetadata metadata, E event) {
                        return null;
                    }
                }
                """.formatted(packageName, simpleName);
    }
}
