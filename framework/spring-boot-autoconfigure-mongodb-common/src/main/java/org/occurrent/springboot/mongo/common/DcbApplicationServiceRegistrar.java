/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.common;

import org.occurrent.application.service.dcb.TagGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;

import java.util.function.Function;

/**
 * Builds the {@link BeanFactoryPostProcessor} that registers a {@code DcbApplicationService} bean when the DCB
 * event-store capability is enabled and a {@link TagGenerator} bean exists. Shared by the blocking and reactive
 * starters, which differ only in the concrete {@code DcbApplicationService} type they register and how the instance is
 * built, so the bean-name lookup, the TagGenerator-presence check, the warn-log message, and the bean-definition wiring
 * live here once.
 */
public final class DcbApplicationServiceRegistrar {

    private static final Logger log = LoggerFactory.getLogger(DcbApplicationServiceRegistrar.class);

    private DcbApplicationServiceRegistrar() {
    }

    /**
     * @param dcbApplicationServiceType the concrete {@code DcbApplicationService} type to register (blocking or reactive)
     * @param beanName                  the name to register the bean under
     * @param factory                   builds the {@code DcbApplicationService} instance from the bean factory
     */
    public static <T> BeanFactoryPostProcessor registrar(Class<T> dcbApplicationServiceType, String beanName, Function<ConfigurableListableBeanFactory, ? extends T> factory) {
        return beanFactory -> {
            if (!(beanFactory instanceof BeanDefinitionRegistry registry)) {
                return;
            }
            boolean hasDcbApplicationService = beanFactory.getBeanNamesForType(dcbApplicationServiceType, false, false).length > 0;
            boolean hasTagGenerator = beanFactory.getBeanNamesForType(TagGenerator.class, false, false).length > 0;
            if (hasDcbApplicationService) {
                return;
            }
            if (!hasTagGenerator) {
                log.warn("Occurrent DCB event-store capability is enabled but no {} bean was found, so a {} is not auto-configured. " +
                                "Define a {} bean (it derives the DCB tags written with each event) to enable auto-configuration, or provide your own {} bean.",
                        TagGenerator.class.getName(), dcbApplicationServiceType.getName(), TagGenerator.class.getSimpleName(), dcbApplicationServiceType.getSimpleName());
                return;
            }
            RootBeanDefinition beanDefinition = new RootBeanDefinition(dcbApplicationServiceType);
            beanDefinition.setInstanceSupplier(() -> factory.apply(beanFactory));
            registry.registerBeanDefinition(beanName, beanDefinition);
        };
    }
}
