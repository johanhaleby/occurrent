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

import org.jspecify.annotations.NullMarked;

import static java.util.Objects.requireNonNull;

/**
 * Thrown by a guarantee check in a late-commit suite when the store under test does not give the guarantee.
 * <p>
 * Internal to the stack that fixes issues 1230 and 1297, never part of a release, and deleted by the fix layer that
 * clears the last use of {@link ExpectedGuaranteeViolation}. It is a distinct type so the extension can tell a
 * guarantee the store does not give from every other way a test can fail, such as a hold that never engaged.
 */
@NullMarked
public final class GuaranteeViolated extends AssertionError {

    private static final long serialVersionUID = 1L;

    private final String expected;
    private final String seen;

    public GuaranteeViolated(String expected, String seen) {
        super("Guarantee violated. Expected: " + requireNonNull(expected, "expected cannot be null")
                + ". Seen: " + requireNonNull(seen, "seen cannot be null"));
        this.expected = expected;
        this.seen = seen;
    }

    /**
     * What the guarantee requires.
     */
    public String expected() {
        return expected;
    }

    /**
     * What the caller saw instead.
     */
    public String seen() {
        return seen;
    }
}
