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
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.occurrent.rewrite.ReactorCancelSubscriptionStubs.CANCELLABLE_SUBSCRIPTIONS;
import static org.occurrent.rewrite.ReactorCancelSubscriptionStubs.DCB_SUBSCRIPTION_MODEL;
import static org.occurrent.rewrite.ReactorCancelSubscriptionStubs.MONO;
import static org.openrewrite.java.Assertions.java;

class MigrateReactorCancelSubscriptionReturnTypeTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new MigrateReactorCancelSubscriptionReturnType())
                .parser(JavaParser.fromJavaVersion().dependsOn(MONO, CANCELLABLE_SUBSCRIPTIONS, DCB_SUBSCRIPTION_MODEL));
    }

    @Test
    void returnsAnEmptyMonoFromAnImplementationThatRanOffTheEnd() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                        import java.util.HashSet;
                        import java.util.Set;

                        class Model implements CancellableSubscriptions {
                            private final Set<String> ids = new HashSet<>();

                            @Override
                            public void cancelSubscription(String subscriptionId) {
                                ids.remove(subscriptionId);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                        import reactor.core.publisher.Mono;

                        import java.util.HashSet;
                        import java.util.Set;

                        class Model implements CancellableSubscriptions {
                            private final Set<String> ids = new HashSet<>();

                            @Override
                            public Mono<Void> cancelSubscription(String subscriptionId) {
                                ids.remove(subscriptionId);
                                return Mono.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void returnsAnEmptyMonoFromAnEmptyBody() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                        class Model implements CancellableSubscriptions {
                            @Override
                            public void cancelSubscription(String subscriptionId) {
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                        import reactor.core.publisher.Mono;

                        class Model implements CancellableSubscriptions {
                            @Override
                            public Mono<Void> cancelSubscription(String subscriptionId) {
                                return Mono.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void turnsAnEarlyReturnIntoAnEmptyMonoAndLeavesTheReturnInALambdaAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                        import java.util.HashSet;
                        import java.util.Set;

                        class Model implements CancellableSubscriptions {
                            private final Set<String> ids = new HashSet<>();

                            @Override
                            public void cancelSubscription(String subscriptionId) {
                                if (!ids.contains(subscriptionId)) {
                                    return;
                                }
                                Runnable remove = () -> {
                                    if (subscriptionId.isEmpty()) {
                                        return;
                                    }
                                    ids.remove(subscriptionId);
                                };
                                remove.run();
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                        import reactor.core.publisher.Mono;

                        import java.util.HashSet;
                        import java.util.Set;

                        class Model implements CancellableSubscriptions {
                            private final Set<String> ids = new HashSet<>();

                            @Override
                            public Mono<Void> cancelSubscription(String subscriptionId) {
                                if (!ids.contains(subscriptionId)) {
                                    return Mono.empty();
                                }
                                Runnable remove = () -> {
                                    if (subscriptionId.isEmpty()) {
                                        return;
                                    }
                                    ids.remove(subscriptionId);
                                };
                                remove.run();
                                return Mono.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void addsNothingAfterAThrow() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                        class Model implements CancellableSubscriptions {
                            @Override
                            public void cancelSubscription(String subscriptionId) {
                                throw new UnsupportedOperationException("Cancelling is not supported");
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                        import reactor.core.publisher.Mono;

                        class Model implements CancellableSubscriptions {
                            @Override
                            public Mono<Void> cancelSubscription(String subscriptionId) {
                                throw new UnsupportedOperationException("Cancelling is not supported");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void changesADcbSubscriptionModelImplementationToo() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.DcbSubscriptionModel;

                        class Model implements DcbSubscriptionModel {
                            @Override
                            public void cancelSubscription(String subscriptionId) {
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.DcbSubscriptionModel;
                        import reactor.core.publisher.Mono;

                        class Model implements DcbSubscriptionModel {
                            @Override
                            public Mono<Void> cancelSubscription(String subscriptionId) {
                                return Mono.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAClassThatImplementsNeitherInterfaceAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        class Registry {
                            public void cancelSubscription(String subscriptionId) {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAnImplementationThatAlreadyReturnsAMonoAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                        import reactor.core.publisher.Mono;

                        class Model implements CancellableSubscriptions {
                            @Override
                            public Mono<Void> cancelSubscription(String subscriptionId) {
                                return Mono.empty();
                            }
                        }
                        """
                )
        );
    }
}
