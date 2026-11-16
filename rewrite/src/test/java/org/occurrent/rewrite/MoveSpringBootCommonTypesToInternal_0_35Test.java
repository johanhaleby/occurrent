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

import static org.openrewrite.java.Assertions.java;

class MoveSpringBootCommonTypesToInternal_0_35Test implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/spring-boot-common-internal-0_35.yml", "org.occurrent.MoveSpringBootCommonTypesToInternal_0_35");
    }

    @Test
    void movesAnImportOfOnlyANestedSubscriptionAnnotationsTypeToTheInternalPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common;

                        public class SubscriptionAnnotations {
                            public record StreamSubscriptionDefinition(String name) {
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.common.SubscriptionAnnotations.StreamSubscriptionDefinition;

                        class Foo {
                            StreamSubscriptionDefinition definition = new StreamSubscriptionDefinition("stream");
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.springboot.common.internal.SubscriptionAnnotations;

                        class Foo {
                            SubscriptionAnnotations.StreamSubscriptionDefinition definition = new SubscriptionAnnotations.StreamSubscriptionDefinition("stream");
                        }
                        """
                )
        );
    }

    @Test
    void movesAnImportOfOnlyTheNestedSubscriptionAnnotationsHandlerParameterKindEnumToTheInternalPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common;

                        public class SubscriptionAnnotations {
                            public enum HandlerParameterKind {
                                EVENT
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.common.SubscriptionAnnotations.HandlerParameterKind;

                        class Foo {
                            HandlerParameterKind kind = HandlerParameterKind.EVENT;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.springboot.common.internal.SubscriptionAnnotations;

                        class Foo {
                            SubscriptionAnnotations.HandlerParameterKind kind = SubscriptionAnnotations.HandlerParameterKind.EVENT;
                        }
                        """
                )
        );
    }

    @Test
    void movesAnImportOfOnlyTheNestedSubscriptionAnnotationsResolvedTypeFilterRecordToTheInternalPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common;

                        public class SubscriptionAnnotations {
                            public record ResolvedTypeFilter(String name) {
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.common.SubscriptionAnnotations.ResolvedTypeFilter;

                        class Foo {
                            ResolvedTypeFilter filter = new ResolvedTypeFilter("filter");
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.springboot.common.internal.SubscriptionAnnotations;

                        class Foo {
                            SubscriptionAnnotations.ResolvedTypeFilter filter = new SubscriptionAnnotations.ResolvedTypeFilter("filter");
                        }
                        """
                )
        );
    }

    @Test
    void keepsAnImportOfAnInternalNestedSubscriptionAnnotationsTypeThatIsUsedBySimpleName() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common.internal;

                        public class SubscriptionAnnotations {
                            public enum HandlerParameterKind {
                                EVENT
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.common.internal.SubscriptionAnnotations.HandlerParameterKind;

                        class Foo {
                            HandlerParameterKind kind = HandlerParameterKind.EVENT;
                        }
                        """
                )
        );
    }

    @Test
    void keepsAnImportOfAnInternalNestedSubscriptionAnnotationsTypeWhoseOnlyUseIsTheTargetOfAnEnumConstantAccess() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common.internal;

                        public class SubscriptionAnnotations {
                            public enum HandlerParameterKind {
                                EVENT
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.common.internal.SubscriptionAnnotations.HandlerParameterKind;

                        class Foo {
                            Object kind = HandlerParameterKind.EVENT;
                        }
                        """
                )
        );
    }

    @Test
    void keepsAWildcardImportOfTheNestedTypesOfTheInternalSubscriptionAnnotationsThatTheFileUses() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common.internal;

                        public class SubscriptionAnnotations {
                            public enum HandlerParameterKind {
                                EVENT
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.common.internal.SubscriptionAnnotations.*;

                        class Foo {
                            Object kind = HandlerParameterKind.EVENT;
                        }
                        """
                )
        );
    }

    @Test
    void movesTheSubscriptionAnnotationsImportsToTheInternalPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common;

                        public class SubscriptionAnnotations {
                            public record HandlerParameter(String name) {
                            }

                            public static boolean shouldWaitUntilStarted(boolean replaysHistory) {
                                return !replaysHistory;
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.common.SubscriptionAnnotations;
                        import org.occurrent.springboot.common.SubscriptionAnnotations.HandlerParameter;

                        class Foo {
                            HandlerParameter parameter = new HandlerParameter("event");

                            boolean wait(boolean replaysHistory) {
                                return SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.springboot.common.internal.SubscriptionAnnotations;
                        import org.occurrent.springboot.common.internal.SubscriptionAnnotations.HandlerParameter;

                        class Foo {
                            HandlerParameter parameter = new HandlerParameter("event");

                            boolean wait(boolean replaysHistory) {
                                return SubscriptionAnnotations.shouldWaitUntilStarted(replaysHistory);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void movesAStaticImportOfAMemberOfSubscriptionAnnotationsToTheInternalPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common;

                        public class SubscriptionAnnotations {
                            public static boolean shouldWaitUntilStarted(boolean replaysHistory) {
                                return !replaysHistory;
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import static org.occurrent.springboot.common.SubscriptionAnnotations.shouldWaitUntilStarted;

                        class Foo {
                            boolean wait(boolean replaysHistory) {
                                return shouldWaitUntilStarted(replaysHistory);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import static org.occurrent.springboot.common.internal.SubscriptionAnnotations.shouldWaitUntilStarted;

                        class Foo {
                            boolean wait(boolean replaysHistory) {
                                return shouldWaitUntilStarted(replaysHistory);
                            }
                        }
                        """
                )
        );
    }

    @Test
    void movesThePushCatchupStatusImplImportToTheInternalPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common;

                        public class PushCatchupStatusImpl {
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.common.PushCatchupStatusImpl;

                        class Foo {
                            PushCatchupStatusImpl status = new PushCatchupStatusImpl();
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.springboot.common.internal.PushCatchupStatusImpl;

                        class Foo {
                            PushCatchupStatusImpl status = new PushCatchupStatusImpl();
                        }
                        """
                )
        );
    }

    @Test
    void expandsAWildcardImportOfTheOldPackageWhenOneOfItsTypesMoves() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common;

                        public class AsynchronousSubscribables {
                        }
                        """
                ),
                java(
                        """
                        package org.occurrent.springboot.common;

                        public class OccurrentProperties {
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.common.*;

                        class Foo {
                            AsynchronousSubscribables subscribables = new AsynchronousSubscribables();
                            OccurrentProperties properties = new OccurrentProperties();
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.springboot.common.OccurrentProperties;
                        import org.occurrent.springboot.common.internal.AsynchronousSubscribables;

                        class Foo {
                            AsynchronousSubscribables subscribables = new AsynchronousSubscribables();
                            OccurrentProperties properties = new OccurrentProperties();
                        }
                        """
                )
        );
    }

    @Test
    void movesAFullyQualifiedReferenceToThePushCatchupStatusImplToTheInternalPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common;

                        public class PushCatchupStatusImpl {
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        class Foo {
                            org.occurrent.springboot.common.PushCatchupStatusImpl status = new org.occurrent.springboot.common.PushCatchupStatusImpl();
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.springboot.common.internal.PushCatchupStatusImpl;

                        class Foo {
                            PushCatchupStatusImpl status = new PushCatchupStatusImpl();
                        }
                        """
                )
        );
    }

    @Test
    void movesAFullyQualifiedReferenceToANestedSubscriptionAnnotationsTypeToTheInternalPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common;

                        public class SubscriptionAnnotations {
                            public record HandlerParameter(String name) {
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        class Foo {
                            org.occurrent.springboot.common.SubscriptionAnnotations.HandlerParameter parameter;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.springboot.common.internal.SubscriptionAnnotations;

                        class Foo {
                            SubscriptionAnnotations.HandlerParameter parameter;
                        }
                        """
                )
        );
    }

    @Test
    void movesTheAsynchronousSubscribablesImportToTheInternalPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common;

                        public class AsynchronousSubscribables {
                            public static String resolve() {
                                return "subscribable";
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.common.AsynchronousSubscribables;

                        class Foo {
                            String resolve() {
                                return AsynchronousSubscribables.resolve();
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.springboot.common.internal.AsynchronousSubscribables;

                        class Foo {
                            String resolve() {
                                return AsynchronousSubscribables.resolve();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void movesTheAppliedAppendRecordingRegistryImportToTheInternalPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common;

                        public class AppliedAppendRecordingRegistry {
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.common.AppliedAppendRecordingRegistry;

                        class Foo {
                            AppliedAppendRecordingRegistry registry = new AppliedAppendRecordingRegistry();
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.springboot.common.internal.AppliedAppendRecordingRegistry;

                        class Foo {
                            AppliedAppendRecordingRegistry registry = new AppliedAppendRecordingRegistry();
                        }
                        """
                )
        );
    }

    @Test
    void movesThePolledCatchupSignalsImportToTheInternalPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.common;

                        public class PolledCatchupSignals {
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.common.PolledCatchupSignals;

                        class Foo {
                            PolledCatchupSignals signals = new PolledCatchupSignals();
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.springboot.common.internal.PolledCatchupSignals;

                        class Foo {
                            PolledCatchupSignals signals = new PolledCatchupSignals();
                        }
                        """
                )
        );
    }
}
