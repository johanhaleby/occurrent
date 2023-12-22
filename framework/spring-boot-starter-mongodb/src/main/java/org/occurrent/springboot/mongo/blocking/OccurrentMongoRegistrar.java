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

import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

import static org.springframework.beans.factory.config.BeanDefinition.ROLE_SUPPORT;

public class OccurrentMongoRegistrar implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(AnnotationMetadata metadata, @NotNull BeanDefinitionRegistry registry) {
//        AnnotationAttributes attributes = AnnotationAttributes.fromMap(metadata.getAnnotationAttributes(EnableOccurrent.class.getName(), true));
//        Class<?> eventT = null;
//        try {
//            eventT = Class.forName(Objects.requireNonNull(attributes).getString("eventType"));
//        } catch (ClassNotFoundException e) {
//            throw new RuntimeException(e);
//        }

        RootBeanDefinition definition = new RootBeanDefinition(OccurrentMongoAutoConfiguration.class);
        definition.setSource(metadata);
        definition.setRole(ROLE_SUPPORT);
        definition.setLazyInit(false);
        registry.registerBeanDefinition("occurrentMongoAutoConfiguration", definition);
    }
}