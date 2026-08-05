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

package org.occurrent.testing.springboot;

import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Registers the blocking configuration, the reactor configuration, or both, depending on which neutral testing
 * artifact is on the classpath, so {@link EnableOccurrentTesting} works on a blocking application, a reactive one, or
 * one using both stacks at once without failing to start on the stack it does not use.
 * <p>
 * Neither {@code occurrent-testing-junit-jupiter-blocking} nor {@code occurrent-testing-junit-jupiter-reactor} is a
 * required dependency of this module; adding one, or both, is how an application opts into that stack's wiring. That
 * is the same classpath-probing pattern the starters use for an optional artifact (ADR 87, ADR 95), done here with a
 * plain {@link ImportSelector} because this module has no autoconfiguration machinery to build a
 * {@code @ConditionalOnClass} on top of.
 */
final class OccurrentTestingImportSelector implements ImportSelector {

    private static final String BLOCKING_EXTENSION = "org.occurrent.testing.junit.blocking.OccurrentSubscriptionsExtension";
    private static final String REACTOR_EXTENSION = "org.occurrent.testing.junit.reactor.OccurrentSubscriptionsExtension";

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        ClassLoader classLoader = getClass().getClassLoader();
        List<String> imports = new ArrayList<>();
        if (ClassUtils.isPresent(BLOCKING_EXTENSION, classLoader)) {
            imports.add(OccurrentTestingConfiguration.class.getName());
        }
        if (ClassUtils.isPresent(REACTOR_EXTENSION, classLoader)) {
            imports.add(OccurrentReactorTestingConfiguration.class.getName());
        }
        if (imports.isEmpty()) {
            throw new IllegalStateException("@" + EnableOccurrentTesting.class.getSimpleName() + " found neither "
                    + "occurrent-testing-junit-jupiter-blocking nor occurrent-testing-junit-jupiter-reactor on the "
                    + "classpath, so it has nothing to wire. Add one of them, or both, as a test dependency.");
        }
        return imports.toArray(new String[0]);
    }
}
