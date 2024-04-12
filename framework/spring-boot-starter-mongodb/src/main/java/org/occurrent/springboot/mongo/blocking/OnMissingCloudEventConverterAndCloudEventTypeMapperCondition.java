/*
 *
 *  Copyright 2024 Johan Haleby
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
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * The reason for this condition is to check the that BOTH CloudEventTypeMapper AND CloudEventConverter are missing.
 * If just doing:
 *
 * <pre lang="java">
 * @ConditionalOnMissingBean({CloudEventTypeMapper.class, CloudEventConverter.class})
 * </pre>
 *
 * <p>
 * It'll match when ANY of the beans are missing
 * </p>
 */
class OnMissingCloudEventConverterAndCloudEventTypeMapperCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, @NotNull AnnotatedTypeMetadata metadata) {
        ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
        //noinspection DataFlowIssue
        boolean cloudEventTypeMapperMissing = beanFactory.getBeanNamesForType(CloudEventTypeMapper.class).length == 0;
        boolean cloudEventConverterMissing = beanFactory.getBeanNamesForType(CloudEventConverter.class).length == 0;
        return cloudEventTypeMapperMissing && cloudEventConverterMissing;
    }
}