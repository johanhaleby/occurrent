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

import static org.openrewrite.java.Assertions.java;

class PackageMoveRenameTest extends MechanicalRenamesRecipeTest {

    @Test
    void executeFilterMovesOutOfTheBlockingPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.application.service.blocking;
                        public interface ExecuteFilter {
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.application.service.blocking.ExecuteFilter;

                        class Foo {
                            ExecuteFilter filter;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.application.service.ExecuteFilter;

                        class Foo {
                            ExecuteFilter filter;
                        }
                        """
                )
        );
    }

    @Test
    void occurrentPropertiesMovesFromBlockingToCommonPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.springboot.mongo.blocking;
                        public class OccurrentProperties {
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.springboot.mongo.blocking.OccurrentProperties;

                        class Foo {
                            OccurrentProperties properties;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.springboot.mongo.common.OccurrentProperties;

                        class Foo {
                            OccurrentProperties properties;
                        }
                        """
                )
        );
    }

    @Test
    void startAtNestedSubscriptionPositionTypeIsRenamedToCheckpoint() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription;
                        public sealed interface StartAt {
                            final class StartAtSubscriptionPosition implements StartAt {}
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.StartAt;

                        class Foo {
                            String describe(StartAt startAt) {
                                if (startAt instanceof StartAt.StartAtSubscriptionPosition) {
                                    return "position";
                                }
                                return "other";
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.StartAt;

                        class Foo {
                            String describe(StartAt startAt) {
                                if (startAt instanceof StartAt.StartAtCheckpoint) {
                                    return "position";
                                }
                                return "other";
                            }
                        }
                        """
                )
        );
    }

    @Test
    void eventMetadataMovesOutOfTheSubscriptionBlockingPackage() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.dsl.subscription.blocking;
                        public class EventMetadata {
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.dsl.subscription.blocking.EventMetadata;

                        class Foo {
                            EventMetadata metadata;
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.dsl.subscription.EventMetadata;

                        class Foo {
                            EventMetadata metadata;
                        }
                        """
                )
        );
    }
}
