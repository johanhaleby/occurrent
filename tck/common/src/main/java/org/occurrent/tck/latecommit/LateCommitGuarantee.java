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

package org.occurrent.tck.latecommit;

import org.jspecify.annotations.NullMarked;
import org.occurrent.tck.internal.scaffolding.GuaranteeViolated;

import java.util.function.Supplier;

/**
 * The one place a late-commit suite states that a guarantee holds. A guarantee that does not hold throws
 * {@link GuaranteeViolated}, which is how a store still carrying its open violation is told apart from a test that
 * failed for another reason.
 * <p>
 * Internal to the stack that fixes issues 1230 and 1297. The fix layer that deletes the scaffolding package rewrites
 * this method to an ordinary assertion.
 */
@NullMarked
public final class LateCommitGuarantee {

    private LateCommitGuarantee() {
    }

    public static void require(boolean holds, String expected, Supplier<String> seen) {
        if (!holds) {
            throw new GuaranteeViolated(expected, seen.get());
        }
    }
}
