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

import static org.assertj.core.api.Assertions.assertThat;
import static org.openrewrite.maven.Assertions.pomXml;

/**
 * Covers the {@code MigrateCoordinates_0_30} recipe: the 0.30.0 artifact-coordinate rename. A prefix-only
 * rename and a reordering rename (the starter) are both exercised, and an unrenamed coordinate (an
 * aggregator POM name) is asserted to stay put.
 */
class CoordinateRenameTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/coordinates.yml", "org.occurrent.MigrateCoordinates_0_30");
    }

    @Test
    void renamesPublishedCoordinatesAndLeavesUnpublishedOnesAlone() {
        rewriteRun(
                pomXml(
                        """
                        <project>
                            <modelVersion>4.0.0</modelVersion>
                            <groupId>com.example</groupId>
                            <artifactId>app</artifactId>
                            <version>1.0.0</version>
                            <dependencies>
                                <dependency>
                                    <groupId>org.occurrent</groupId>
                                    <artifactId>subscription-inmemory</artifactId>
                                    <version>0.20.5</version>
                                </dependency>
                                <dependency>
                                    <groupId>org.occurrent</groupId>
                                    <artifactId>spring-boot-starter-mongodb</artifactId>
                                    <version>0.20.5</version>
                                </dependency>
                                <dependency>
                                    <groupId>org.jetbrains</groupId>
                                    <artifactId>annotations</artifactId>
                                    <version>24.0.0</version>
                                </dependency>
                            </dependencies>
                        </project>
                        """,
                        // The after is a function rather than a literal so the assertion tolerates the
                        // "unable to download POM" marker rewrite-maven injects for the new coordinates,
                        // which do not exist on Maven Central until 0.30.0 is published.
                        spec -> spec.after(actual -> {
                            assertThat(actual)
                                    // prefix-only rename
                                    .contains("<artifactId>occurrent-subscription-inmemory</artifactId>")
                                    // reordering rename (starter to Spring's third-party convention)
                                    .contains("<artifactId>occurrent-mongodb-spring-boot-starter</artifactId>")
                                    // old coordinates are gone
                                    .doesNotContain("<artifactId>subscription-inmemory</artifactId>")
                                    .doesNotContain("<artifactId>spring-boot-starter-mongodb</artifactId>");
                            // a same-named third-party artifact under a different groupId is left alone
                            assertThat(actual).contains("<groupId>org.jetbrains</groupId>");
                            assertThat(actual).doesNotContain("occurrent-annotations");
                            return actual;
                        })
                )
        );
    }
}
