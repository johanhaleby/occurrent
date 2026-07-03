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

package org.occurrent.springboot.mongo.common;

import org.occurrent.application.service.dcb.TagGenerator;
import org.slf4j.Logger;

/**
 * Shared rationale and diagnostics for the auto-configured {@code DcbApplicationService} bean, identical on the
 * blocking and reactive starters.
 * <p>
 * The bean resolves its {@link TagGenerator} through an {@link org.springframework.beans.factory.ObjectProvider}
 * rather than gating existence with {@code @ConditionalOnBean(TagGenerator.class)}. {@code @EnableOccurrent} and
 * {@code @EnableOccurrentReactive} import the auto-configuration with a plain {@code @Import}, not the
 * {@code DeferredImportSelector} machinery real auto-configuration processing uses, so this class does not get the
 * "runs after user bean definitions" ordering guarantee {@code @ConditionalOnBean}'s documentation promises: its
 * condition can be evaluated before the user's own {@code @SpringBootApplication} class has registered its
 * {@link TagGenerator} bean (confirmed empirically against a real {@code @SpringBootTest} context, not just
 * {@code ApplicationContextRunner} slice tests, where the ordering coincidentally looks safe). A custom
 * {@link org.springframework.context.annotation.Condition} would hit the identical evaluation-order problem, since
 * nothing distinguishes a hand-written condition from a built-in one at that point in bean-definition processing.
 * {@code ObjectProvider.getIfAvailable()} instead resolves at bean-instantiation time, after all definitions exist, so
 * it finds a user-defined {@link TagGenerator} reliably regardless of declaration order or import style.
 * <p>
 * Returning {@code null} when no {@link TagGenerator} exists behaves like the bean never having been registered for
 * the common consumer path: Spring resolves a null-returning {@code @Bean} to a {@code NullBean} sentinel, and
 * {@code @Autowired}/constructor injection of this type then fails with the same
 * {@code NoSuchBeanDefinitionException}-style error as a genuinely absent bean. The bean definition itself is
 * unconditional, though, so anything that resolves by name or by raw type introspection can still observe it: by-type
 * introspection (for example {@code getBeanNamesForType}, as used by Actuator's beans endpoint) reports the bean
 * name, and name-based lookups such as {@code containsBean("occurrentDcbApplicationService")} or
 * {@code getBean("occurrentDcbApplicationService")} succeed and return the {@code NullBean} sentinel instead of
 * throwing {@code NoSuchBeanDefinitionException}. That is the accepted, narrow cost of making this a normal,
 * generically-typed {@code @Bean} that IDEs can statically resolve, instead of the {@code BeanFactoryPostProcessor}
 * this replaced.
 */
public final class DcbApplicationServiceDiagnostics {

    private DcbApplicationServiceDiagnostics() {
    }

    /**
     * Logs that {@code dcbApplicationServiceType} was not auto-configured because no {@link TagGenerator} bean was
     * found. Call this from the {@code @Bean} method's null-return branch described in the class-level javadoc.
     */
    public static void warnTagGeneratorMissing(Logger log, Class<?> dcbApplicationServiceType) {
        log.warn("Occurrent DCB event-store capability is enabled but no {} bean was found, so a {} is not auto-configured. " +
                        "Define a {} bean (it derives the DCB tags written with each event) to enable auto-configuration, or provide your own {} bean.",
                TagGenerator.class.getName(), dcbApplicationServiceType.getName(), TagGenerator.class.getSimpleName(), dcbApplicationServiceType.getSimpleName());
    }
}
