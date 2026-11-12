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
import org.openrewrite.test.TypeValidation;

import static org.occurrent.rewrite.NativeMongoSubscriptionStubs.NATIVE_MONGO_SUBSCRIPTION;
import static org.openrewrite.java.Assertions.java;

/**
 * Every case here is an actual 0.34.0 caller's record pattern, unchanged, meeting the 0.35.0
 * {@link NativeMongoSubscriptionStubs} classpath whose canonical arity it no longer matches.
 * <p>
 * Identifier type validation is off for the same reason as in {@link MigrateDcbMarkerVersionsRecordPatternTest}. This
 * version's parser leaves a binding pattern's own identifier unattributed inside a {@code switch} case label, and
 * that is not something this recipe causes.
 */
class MigrateNativeMongoSubscriptionRecordPatternTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new MigrateNativeMongoSubscriptionRecordPattern())
                .parser(JavaParser.fromJavaVersion().dependsOn(NATIVE_MONGO_SUBSCRIPTION))
                .typeValidationOptions(TypeValidation.builder().identifiers(false).build());
    }

    @Test
    void addsTheStartFailureBindingToAVarNativeMongoSubscriptionSwitchPattern() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscription;

                        class Reader {
                            String describe(Object subscription) {
                                return switch (subscription) {
                                    case NativeMongoSubscription(var subscriptionId, var latch) -> subscriptionId + ":" + latch.getCount();
                                    default -> "unknown";
                                };
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscription;

                        class Reader {
                            String describe(Object subscription) {
                                return switch (subscription) {
                                    case NativeMongoSubscription(var subscriptionId, var latch, var startFailure) -> subscriptionId + ":" + latch.getCount();
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void addsTheStartFailureBindingToAnExplicitlyTypedNativeMongoSubscriptionInstanceofPattern() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscription;

                        import java.util.concurrent.CountDownLatch;

                        class Reader {
                            String idOf(Object subscription) {
                                if (subscription instanceof NativeMongoSubscription(String subscriptionId, CountDownLatch latch)) {
                                    return subscriptionId;
                                }
                                return "unknown";
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscription;

                        import java.util.concurrent.CountDownLatch;

                        class Reader {
                            String idOf(Object subscription) {
                                if (subscription instanceof NativeMongoSubscription(String subscriptionId, CountDownLatch latch, var startFailure)) {
                                    return subscriptionId;
                                }
                                return "unknown";
                            }
                        }
                        """
                )
        );
    }

    @Test
    void generatesACollisionSafeNameWhenAnEnclosingLocalIsAlreadyCalledStartFailure() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscription;

                        class Reader {
                            String describe(Object subscription) {
                                String startFailure = "unrelated";
                                if (subscription instanceof NativeMongoSubscription(var subscriptionId, var latch)) {
                                    return startFailure + ":" + subscriptionId;
                                }
                                return startFailure;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscription;

                        class Reader {
                            String describe(Object subscription) {
                                String startFailure = "unrelated";
                                if (subscription instanceof NativeMongoSubscription(var subscriptionId, var latch, var startFailure1)) {
                                    return startFailure + ":" + subscriptionId;
                                }
                                return startFailure;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesANativeMongoSubscriptionPatternWithThreeBindingsAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscription;

                        class Reader {
                            String describe(Object subscription) {
                                return switch (subscription) {
                                    case NativeMongoSubscription(var subscriptionId, var latch, var startFailure) -> subscriptionId + ":" + latch.getCount();
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesARecordNamedLikeTheTargetInAnotherPackageAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import java.util.concurrent.CountDownLatch;

                        class Reader {
                            record NativeMongoSubscription(String subscriptionId, CountDownLatch subscriptionStartedLatch) {
                            }

                            String describe(Object o) {
                                return switch (o) {
                                    case NativeMongoSubscription(var subscriptionId, var latch) -> subscriptionId + ":" + latch.getCount();
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }
}
