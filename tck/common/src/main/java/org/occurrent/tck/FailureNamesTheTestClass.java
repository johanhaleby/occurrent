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

package org.occurrent.tck;

import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestInstances;
import org.opentest4j.AssertionFailedError;

import java.util.List;
import java.util.Optional;

/**
 * Puts the name of the test class that ran a suite into the assertion failures that suite produces.
 * <p>
 * A suite keeps its tests in {@code @Nested} classes of the suite itself, so every implementation that extends it runs
 * those tests under the suite's class name rather than under its own. Surefire reports a test by that name, so two
 * implementations in one module report the same test twice. When one of them fails and the other passes, the run
 * report says {@code Run 1: <failure>} and {@code Run 2: PASS} for what looks like a single test, which is also what a
 * retry of a flaky test looks like, and nothing in the failure says which implementation produced it. The failure text
 * is the one place that can carry the name, so this puts it there.
 * <p>
 * Only an {@link AssertionFailedError} is rewritten. Anything else thrown out of a test came from the implementation
 * and already names it in the stack trace.
 */
@NullMarked
public final class FailureNamesTheTestClass implements TestExecutionExceptionHandler {

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        if (!(throwable instanceof AssertionFailedError failure)) {
            throw throwable;
        }
        throw testClass(context).<Throwable>map(testClass -> named(failure, testClass)).orElse(throwable);
    }

    /**
     * The outermost instance is the one written by the implementation, the inner ones are the suite's own
     * {@code @Nested} classes.
     */
    private static Optional<Class<?>> testClass(ExtensionContext context) {
        return context.getTestInstances()
                .map(TestInstances::getAllInstances)
                .filter(instances -> !instances.isEmpty())
                .map(List::getFirst)
                .map(Object::getClass);
    }

    private static AssertionFailedError named(AssertionFailedError failure, Class<?> testClass) {
        String message = failure.getMessage() == null ? "" : failure.getMessage().stripLeading();
        String named = "Run by " + testClass.getSimpleName() + ". " + message;
        // The expected and actual values are carried over when the original had them, so an IDE can still show the
        // two side by side. Passing them when it had none would claim a comparison that never happened.
        AssertionFailedError renamed = failure.isExpectedDefined() && failure.isActualDefined()
                ? new AssertionFailedError(named, failure.getExpected(), failure.getActual())
                : new AssertionFailedError(named);
        renamed.setStackTrace(failure.getStackTrace());
        return renamed;
    }
}
