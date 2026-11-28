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

package org.occurrent.springboot.blocking;

import org.occurrent.springboot.common.OccurrentProperties;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;

import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Makes a mocked {@link ApplicationContext} answer what a real one does when no event store bean has recorded a start
 * boundary. It gives the default {@link OccurrentProperties} and finds no event store to pick.
 */
final class ContextWithoutBoundaryStore {

    private ContextWithoutBoundaryStore() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static ApplicationContext on(ApplicationContext applicationContext) {
        ObjectProvider<OccurrentProperties> properties = mock(ObjectProvider.class);
        when(properties.getIfAvailable(any(Supplier.class))).thenAnswer(invocation -> ((Supplier<OccurrentProperties>) invocation.getArgument(0)).get());
        when(applicationContext.getBeanProvider(OccurrentProperties.class)).thenReturn(properties);
        AutowireCapableBeanFactory beanFactory = mock(AutowireCapableBeanFactory.class);
        when(beanFactory.resolveNamedBean(any(Class.class))).thenThrow(new NoSuchBeanDefinitionException("event store"));
        when(applicationContext.getAutowireCapableBeanFactory()).thenReturn(beanFactory);
        when(applicationContext.getBeanProvider(CompetingConsumerStrategy.class)).thenReturn(mock(ObjectProvider.class));
        return applicationContext;
    }

    static ApplicationContext mocked() {
        return on(mock(ApplicationContext.class));
    }
}
