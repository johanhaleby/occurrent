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

package org.occurrent.tck.eventstore.blocking;

import org.junit.jupiter.api.Assumptions;

/**
 * The one class in this module that does the thing {@link SkipMechanismScan} looks for, so that a scan finding nothing
 * can be told apart from a scan incapable of finding anything. {@link SuiteNeverSkipsTest} points the scan at this
 * class and requires it to be reported.
 * <p>
 * It is compiled into {@code target/test-classes} rather than {@code target/classes}, a different code source from the
 * suites, so it can never leak into the scan that matters. It is not named {@code *Test} either, so Surefire does not
 * run it.
 */
final class SkipsOnPurpose {

    private SkipsOnPurpose() {
    }

    static void skip() {
        Assumptions.assumeTrue(false, "the scan is supposed to notice this");
    }
}
