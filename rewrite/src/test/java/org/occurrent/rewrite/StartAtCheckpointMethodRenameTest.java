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

class StartAtCheckpointMethodRenameTest extends MechanicalRenamesRecipeTest {

    @Test
    void startAtSubscriptionPositionIsRenamedToCheckpoint() {
        rewriteRun(
                java(
                        """
                        package org.occurrent.subscription;
                        public class StartAt {
                            public static StartAt subscriptionPosition(Object position) {
                                return null;
                            }
                        }
                        """,
                        """
                        package org.occurrent.subscription;
                        public class StartAt {
                            public static StartAt checkpoint(Object position) {
                                return null;
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.StartAt;

                        class Foo {
                            StartAt at = StartAt.subscriptionPosition(new Object());
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.StartAt;

                        class Foo {
                            StartAt at = StartAt.checkpoint(new Object());
                        }
                        """
                )
        );
    }
}
