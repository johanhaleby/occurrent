/*
 *
 *  Copyright 2023 Johan Haleby
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

package org.occurrent.springboot.mongo.blocking;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.GenericTypeResolver;
import org.springframework.core.ResolvableType;
import org.springframework.core.type.AnnotationMetadata;

import java.lang.reflect.ParameterizedType;
import java.util.Optional;

@Configuration
public class OccurrentMongoBootstrap implements ApplicationContextAware, ImportBeanDefinitionRegistrar {

    private Class<?> eventType;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Optional<String> customAutoConfigurationBeanName = applicationContext.getBeansWithAnnotation(EnableOccurrent.class)
                .keySet().stream().findFirst();


        EnableOccurrent enableOccurrent =
                applicationContext.findAnnotationOnBean(customAutoConfigurationBeanName.orElseThrow(),
                        EnableOccurrent.class);

        eventType = enableOccurrent.eventType();
    }

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
//        ResolvableType resolvableType = ResolvableType.forClass(eventType);
        Class<?> eventT;
        try {
            eventT = Class.forName("org.occurrent.example.domain.rps.decidermodel.GameEvent");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }


        Class<?> occurrentMongoAutoConfig = GenericTypeResolver.resolveTypeArgument(OccurrentMongoAutoConfiguration.class, eventT);
        GenericBeanDefinition definition = new GenericBeanDefinition();
        definition.setBeanClass(occurrentMongoAutoConfig);
        registry.registerBeanDefinition("occurrentAutoConfiguration", definition);
    }
}
