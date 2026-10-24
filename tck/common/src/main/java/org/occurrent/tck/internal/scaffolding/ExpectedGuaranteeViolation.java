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

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks tests of a conformance suite that are expected to throw {@link GuaranteeViolated} because the store under test
 * does not give the guarantee yet.
 * <ul>
 *     <li>A listed test passes when it throws {@link GuaranteeViolated}.</li>
 *     <li>A listed test that completes without throwing it fails, saying the guarantee now holds and that the entry for
 *     {@link #issue()} must be removed.</li>
 *     <li>Any other throwable from a listed test propagates unchanged, so a hold that never engaged still fails.</li>
 *     <li>A test that is not listed is untouched.</li>
 *     <li>A listed name that is not a test method of the annotated class fails the class.</li>
 * </ul>
 * <p>
 * Internal to the stack that fixes issues 1230 and 1297, never part of a release, and deleted by the fix layer that
 * clears its last use. A store declines a suite by not extending it, and this annotation is how a store runs a suite
 * while the violation is still open, so no fixture interface carries an opt-out.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@ExtendWith(ExpectedGuaranteeViolationExtension.class)
public @interface ExpectedGuaranteeViolation {

    /**
     * The names of the test methods that are expected to throw {@link GuaranteeViolated}.
     */
    String[] tests();

    /**
     * The issue the violation is tracked in, quoted in the failure that asks for the entry to be removed.
     */
    String issue();
}
