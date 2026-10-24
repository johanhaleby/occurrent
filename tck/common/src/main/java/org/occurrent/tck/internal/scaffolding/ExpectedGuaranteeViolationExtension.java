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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.junit.platform.commons.support.AnnotationSupport;
import org.opentest4j.AssertionFailedError;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The behaviour behind {@link ExpectedGuaranteeViolation}. Intercepts the test method itself, so the only throwable it
 * ever swallows is a {@link GuaranteeViolated} thrown by a listed test. Anything thrown by a setup or teardown callback,
 * or by a test that is not listed, never reaches it.
 */
@NullMarked
final class ExpectedGuaranteeViolationExtension implements BeforeAllCallback, InvocationInterceptor {

    @Override
    public void beforeAll(ExtensionContext context) {
        Class<?> testClass = context.getRequiredTestClass();
        // Only the class that carries the annotation validates it, a nested class of it has other test methods.
        Optional<ExpectedGuaranteeViolation> expectation =
                AnnotationSupport.findAnnotation(testClass, ExpectedGuaranteeViolation.class);
        if (expectation.isEmpty()) {
            return;
        }
        List<String> listed = Arrays.asList(expectation.get().tests());
        if (listed.isEmpty()) {
            throw new IllegalStateException("@ExpectedGuaranteeViolation on " + testClass.getName() + " lists no tests");
        }
        Set<String> testMethods = testMethodNamesOf(testClass);
        for (String name : listed) {
            if (!testMethods.contains(name)) {
                throw new IllegalStateException("@ExpectedGuaranteeViolation on " + testClass.getName() + " lists '"
                        + name + "', which is not a test method of that class. The test methods are " + testMethods);
            }
        }
    }

    @Override
    public void interceptTestMethod(Invocation<Void> invocation,
                                    ReflectiveInvocationContext<Method> invocationContext,
                                    ExtensionContext extensionContext) throws Throwable {
        Class<?> testClass = extensionContext.getRequiredTestClass();
        Method testMethod = invocationContext.getExecutable();
        Optional<ExpectedGuaranteeViolation> expectation = expectationOf(testClass)
                .filter(listing -> Arrays.asList(listing.tests()).contains(testMethod.getName()));
        if (expectation.isEmpty()) {
            invocation.proceed();
            return;
        }

        try {
            invocation.proceed();
        } catch (GuaranteeViolated expected) {
            return;
        }
        throw new AssertionFailedError(testClass.getSimpleName() + "." + testMethod.getName()
                + " completed without throwing GuaranteeViolated, so the guarantee now holds. The expected violation for "
                + expectation.get().issue() + " must be removed from the @ExpectedGuaranteeViolation of "
                + testClass.getSimpleName() + ".");
    }

    /**
     * Looks on the test class and then outwards through the classes it is nested in, so a nested test class of an
     * annotated class is covered by the same listing.
     */
    private static Optional<ExpectedGuaranteeViolation> expectationOf(Class<?> testClass) {
        for (Class<?> candidate = testClass; candidate != null; candidate = candidate.getEnclosingClass()) {
            Optional<ExpectedGuaranteeViolation> found =
                    AnnotationSupport.findAnnotation(candidate, ExpectedGuaranteeViolation.class);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    private static Set<String> testMethodNamesOf(Class<?> testClass) {
        Set<String> names = new LinkedHashSet<>();
        for (Class<?> type = testClass; type != null; type = type.getSuperclass()) {
            for (Method method : type.getDeclaredMethods()) {
                if (AnnotationSupport.isAnnotated(method, Test.class)) {
                    names.add(method.getName());
                }
            }
        }
        return names;
    }
}
