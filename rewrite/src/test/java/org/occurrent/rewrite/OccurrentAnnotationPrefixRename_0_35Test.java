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
import org.openrewrite.test.TypeValidation;

import static org.openrewrite.java.Assertions.java;

/**
 * Covers the seven annotation renames in {@code MigrateOccurrentAnnotationRenames_0_35}. The recipe renames the
 * annotation type and nothing else, so an attribute the new annotation no longer declares is still written out
 * verbatim, which is what the last test asserts and what the migration guide sends a reader to fix by hand.
 */
class OccurrentAnnotationPrefixRename_0_35Test implements RewriteTest {

    /**
     * The stubbed {@code StreamSubscription} declares its own nested {@code StartPosition} and uses it as an
     * attribute type, so ChangeType cannot tell the renamed nested enum from the declaration it was told to leave
     * alone. Same stubbing artifact as {@link AnnotationEnumRenamesRecipeTest}.
     */
    private static final TypeValidation RELAXED_FOR_SELF_REFERENCING_RENAME =
            TypeValidation.builder().identifiers(false).classDeclarations(false).build();

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/renames-annotations-0_35.yml",
                "org.occurrent.MigrateOccurrentAnnotationRenames_0_35");
    }

    @Test
    void projectionAnnotationGetsTheOccurrentPrefix() {
        rewriteRun(
                java(PROJECTION_STUB),
                java(
                        """
                        package com.example;

                        import org.occurrent.annotation.Projection;

                        class OrderStatusProjection {
                            @Projection(id = "orderStatus")
                            Object orderStatus() {
                                return null;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.annotation.OccurrentProjection;

                        class OrderStatusProjection {
                            @OccurrentProjection(id = "orderStatus")
                            Object orderStatus() {
                                return null;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void sagaAnnotationGetsTheOccurrentPrefix() {
        rewriteRun(
                java(SAGA_STUB),
                java(
                        """
                        package com.example;

                        import org.occurrent.annotation.Saga;

                        class OrderFulfillment {
                            @Saga(id = "orderFulfillment")
                            Object orderFulfillment() {
                                return null;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.annotation.OccurrentSaga;

                        class OrderFulfillment {
                            @OccurrentSaga(id = "orderFulfillment")
                            Object orderFulfillment() {
                                return null;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void snapshotAnnotationGetsTheOccurrentPrefix() {
        rewriteRun(
                java(SNAPSHOT_STUB),
                java(
                        """
                        package com.example;

                        import org.occurrent.annotation.Snapshot;

                        class AccountSnapshot {
                            @Snapshot(id = "accountSnapshot")
                            Object accountSnapshot() {
                                return null;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.annotation.OccurrentSnapshot;

                        class AccountSnapshot {
                            @OccurrentSnapshot(id = "accountSnapshot")
                            Object accountSnapshot() {
                                return null;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void subscriptionAnnotationGetsTheOccurrentPrefix() {
        rewriteRun(
                java(SUBSCRIPTION_STUB),
                java(
                        """
                        package com.example;

                        import org.occurrent.annotation.Subscription;

                        class Notifications {
                            @Subscription(id = "notifyCustomer")
                            void notifyCustomer(Object event) {
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.annotation.OccurrentSubscription;

                        class Notifications {
                            @OccurrentSubscription(id = "notifyCustomer")
                            void notifyCustomer(Object event) {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void dcbSubscriptionAnnotationGetsTheOccurrentPrefix() {
        rewriteRun(
                java(DCB_SUBSCRIPTION_STUB),
                java(
                        """
                        package com.example;

                        import org.occurrent.annotation.DcbSubscription;

                        class CourseDashboard {
                            @DcbSubscription(id = "courseDashboard")
                            void onEvent(Object event) {
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.annotation.OccurrentDcbSubscription;

                        class CourseDashboard {
                            @OccurrentDcbSubscription(id = "courseDashboard")
                            void onEvent(Object event) {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void synchronousSubscriptionAnnotationGetsTheOccurrentPrefix() {
        rewriteRun(
                java(SYNCHRONOUS_SUBSCRIPTION_STUB),
                java(
                        """
                        package com.example;

                        import org.occurrent.annotation.SynchronousSubscription;

                        class ReadModel {
                            @SynchronousSubscription(id = "updateReadModel")
                            void updateReadModel(Object event) {
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.annotation.OccurrentSynchronousSubscription;

                        class ReadModel {
                            @OccurrentSynchronousSubscription(id = "updateReadModel")
                            void updateReadModel(Object event) {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void streamSubscriptionAnnotationAndItsNestedStartPositionGetTheOccurrentPrefix() {
        rewriteRun(
                spec -> spec.typeValidationOptions(RELAXED_FOR_SELF_REFERENCING_RENAME),
                // The stub stands in for the 0.34.0 annotation, and it is the one file that also references the
                // nested enum from inside the declaration being renamed. An application never has that file, the
                // call site below is what a reader of this test should compare against.
                java(STREAM_SUBSCRIPTION_STUB, STREAM_SUBSCRIPTION_STUB_AFTER),
                java(
                        """
                        package com.example;

                        import org.occurrent.annotation.StreamSubscription;

                        class Notifications {
                            @StreamSubscription(id = "notifyCustomer", startAt = StreamSubscription.StartPosition.BEGINNING_OF_TIME)
                            void notifyCustomer(Object event) {
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.annotation.OccurrentStreamSubscription;
                        import org.occurrent.annotation.OccurrentStreamSubscription.StartPosition;

                        class Notifications {
                            @OccurrentStreamSubscription(id = "notifyCustomer", startAt = StartPosition.BEGINNING_OF_TIME)
                            void notifyCustomer(Object event) {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void aDeclaredEventTypesIsRenamedAlongWithTheAnnotationAndLeftForTheReaderToRemove() {
        rewriteRun(
                java(SUBSCRIPTION_STUB),
                java(
                        """
                        package com.example;

                        import org.occurrent.annotation.Subscription;

                        class Notifications {
                            @Subscription(id = "notifyCustomer", eventTypes = {String.class})
                            void notifyCustomer(Object event) {
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.annotation.OccurrentSubscription;

                        class Notifications {
                            @OccurrentSubscription(id = "notifyCustomer", eventTypes = {String.class})
                            void notifyCustomer(Object event) {
                            }
                        }
                        """
                )
        );
    }

    private static final String PROJECTION_STUB = """
            package org.occurrent.annotation;

            public @interface Projection {
                String id();
            }
            """;

    private static final String SAGA_STUB = """
            package org.occurrent.annotation;

            public @interface Saga {
                String id();
            }
            """;

    private static final String SNAPSHOT_STUB = """
            package org.occurrent.annotation;

            public @interface Snapshot {
                String id();
            }
            """;

    private static final String SUBSCRIPTION_STUB = """
            package org.occurrent.annotation;

            public @interface Subscription {
                String id();

                Class<?>[] eventTypes() default {};
            }
            """;

    private static final String DCB_SUBSCRIPTION_STUB = """
            package org.occurrent.annotation;

            public @interface DcbSubscription {
                String id();

                Class<?>[] eventTypes() default {};

                String[] tags() default {};
            }
            """;

    private static final String SYNCHRONOUS_SUBSCRIPTION_STUB = """
            package org.occurrent.annotation;

            public @interface SynchronousSubscription {
                String id();

                Class<?>[] eventTypes() default {};
            }
            """;

    private static final String STREAM_SUBSCRIPTION_STUB = """
            package org.occurrent.annotation;

            public @interface StreamSubscription {
                String id();

                StartPosition startAt() default StartPosition.DEFAULT;

                enum StartPosition {
                    BEGINNING_OF_TIME, NOW, DEFAULT
                }
            }
            """;

    private static final String STREAM_SUBSCRIPTION_STUB_AFTER = """
            package org.occurrent.annotation;

            public @interface StreamSubscription {
                String id();

                OccurrentStreamSubscription.StartPosition startAt() default OccurrentStreamSubscription.StartPosition.DEFAULT;

                enum StartPosition {
                    BEGINNING_OF_TIME, NOW, DEFAULT
                }
            }
            """;
}
