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

package org.occurrent.tck.internal.scaffolding;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The scaffolding in this package is internal to the stack that fixes issues 1230 and 1297, so a release must not
 * contain it. The fix layer that clears the last use of {@code ExpectedGuaranteeViolation} deletes the package and this
 * test with it.
 * <p>
 * The version comes from a surefire system property set in this module's pom. The test fails when the property is
 * missing, so a build that stops passing it cannot make the rule pass by reading nothing.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the internal scaffolding")
class ScaffoldingNeverShipsInAReleaseTest {

    private static final String VERSION_PROPERTY = "occurrent.tck.project.version";
    private static final String SCAFFOLDING = "org.occurrent.tck.internal.scaffolding.ExpectedGuaranteeViolation";

    @Test
    void is_given_the_project_version_by_the_build() {
        assertThat(System.getProperty(VERSION_PROPERTY))
                .describedAs("the pom passes the project version as a system property, and without it the check below "
                        + "reads nothing and could not fail")
                .isNotBlank()
                .doesNotContain("${");
    }

    @Test
    void is_only_on_the_classpath_of_a_snapshot_build() {
        String version = System.getProperty(VERSION_PROPERTY, "");

        assertThat(shipsInARelease(version, scaffoldingIsOnTheClasspath()))
                .describedAs("version %s", version)
                .isEmpty();
    }

    @Test
    void the_rule_it_applies_rejects_a_release_version_while_the_scaffolding_is_present() {
        assertThat(shipsInARelease("0.35.0", true))
                .describedAs("without this the check above passes whatever the rule says")
                .isPresent();
        assertThat(shipsInARelease("", true)).isPresent();
    }

    @Test
    void the_rule_it_applies_accepts_a_snapshot_version_and_a_release_without_the_scaffolding() {
        assertThat(shipsInARelease("0.35.0-SNAPSHOT", true)).isEmpty();
        assertThat(shipsInARelease("0.35.0", false)).isEmpty();
    }

    @Test
    void is_on_the_classpath_of_this_module() {
        assertThat(scaffoldingIsOnTheClasspath())
                .describedAs("the check above passes by finding nothing to guard when this is false, so this fails "
                        + "if the class is renamed or moved without the check being updated")
                .isTrue();
    }

    private static Optional<String> shipsInARelease(String version, boolean scaffoldingIsPresent) {
        if (scaffoldingIsPresent && !version.endsWith("-SNAPSHOT")) {
            return Optional.of("Version '" + version + "' is not a -SNAPSHOT but " + SCAFFOLDING
                    + " is on the classpath. The scaffolding is internal to the stack fixing issues 1230 and 1297 and "
                    + "must be deleted before a release.");
        }
        return Optional.empty();
    }

    private static boolean scaffoldingIsOnTheClasspath() {
        try {
            Class.forName(SCAFFOLDING, false, ScaffoldingNeverShipsInAReleaseTest.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
