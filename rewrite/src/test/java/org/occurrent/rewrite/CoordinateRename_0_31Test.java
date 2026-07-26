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
 * Covers the {@code MigrateCoordinates_0_31} recipe: the 0.31.0 rename of the four subscription
 * checkpoint-storage artifacts from -position-storage to -checkpoint-storage, and the rename of the Spring Boot
 * autoconfigure artifact from {@code occurrent-mongodb-spring-boot-autoconfigure} to
 * {@code occurrent-spring-boot-autoconfigure}. A renamed coordinate and an unrelated Occurrent coordinate (left
 * alone) are both exercised for each.
 */
class CoordinateRename_0_31Test implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/coordinates-0_31.yml", "org.occurrent.MigrateCoordinates_0_31");
    }

    @Test
    void renamesCheckpointStorageCoordinatesAndLeavesOthersAlone() {
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
                                    <artifactId>occurrent-subscription-mongodb-native-blocking-position-storage</artifactId>
                                    <version>0.30.0</version>
                                </dependency>
                                <dependency>
                                    <groupId>org.occurrent</groupId>
                                    <artifactId>occurrent-subscription-redis-spring-blocking-position-storage</artifactId>
                                    <version>0.30.0</version>
                                </dependency>
                                <dependency>
                                    <groupId>org.occurrent</groupId>
                                    <artifactId>occurrent-subscription-inmemory</artifactId>
                                    <version>0.30.0</version>
                                </dependency>
                            </dependencies>
                        </project>
                        """,
                        // The after is a function rather than a literal so the assertion tolerates the
                        // "unable to download POM" marker rewrite-maven injects for the new coordinates,
                        // which do not exist on Maven Central until 0.31.0 is published.
                        spec -> spec.after(actual -> {
                            assertThat(actual)
                                    .contains("<artifactId>occurrent-subscription-mongodb-native-blocking-checkpoint-storage</artifactId>")
                                    .contains("<artifactId>occurrent-subscription-redis-spring-blocking-checkpoint-storage</artifactId>")
                                    .doesNotContain("position-storage")
                                    // an unrelated Occurrent coordinate is untouched
                                    .contains("<artifactId>occurrent-subscription-inmemory</artifactId>");
                            return actual;
                        })
                )
        );
    }

    @Test
    void renamesSpringBootAutoconfigureCoordinateAndLeavesOthersAlone() {
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
                                    <artifactId>occurrent-mongodb-spring-boot-autoconfigure</artifactId>
                                    <version>0.30.0</version>
                                </dependency>
                                <dependency>
                                    <groupId>org.occurrent</groupId>
                                    <artifactId>occurrent-subscription-inmemory</artifactId>
                                    <version>0.30.0</version>
                                </dependency>
                            </dependencies>
                        </project>
                        """,
                        spec -> spec.after(actual -> {
                            assertThat(actual)
                                    .contains("<artifactId>occurrent-spring-boot-autoconfigure</artifactId>")
                                    .doesNotContain("occurrent-mongodb-spring-boot-autoconfigure")
                                    // an unrelated Occurrent coordinate is untouched
                                    .contains("<artifactId>occurrent-subscription-inmemory</artifactId>");
                            return actual;
                        })
                )
        );
    }
}
