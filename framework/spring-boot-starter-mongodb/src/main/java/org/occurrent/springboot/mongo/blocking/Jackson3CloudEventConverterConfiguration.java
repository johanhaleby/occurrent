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

package org.occurrent.springboot.mongo.blocking;

import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Fallback;
import org.springframework.context.annotation.Lazy;
import tools.jackson.databind.ObjectMapper;

import java.util.Optional;

@Configuration(proxyBeanMethods = false)
class Jackson3CloudEventConverterConfiguration {

    @Bean
    // Keep the fallback converter lazy so composed libraries can supply their own
    // CloudEventConverter without forcing the starter's default bean to instantiate.
    @Lazy
    @Fallback
    @ConditionalOnMissingBean(CloudEventConverter.class)
    public <E> CloudEventConverter<E> occurrentCloudEventConverter(Optional<ObjectMapper> objectMapper, OccurrentProperties occurrentProperties, Optional<CloudEventTypeMapper<E>> cloudEventTypeMapper) {
        ObjectMapper om = objectMapper.orElseGet(ObjectMapper::new);
        CloudEventTypeMapper<E> typeMapper = cloudEventTypeMapper.orElseGet(ReflectionCloudEventTypeMapper::qualified);
        return new JacksonCloudEventConverter.Builder<E>(om, occurrentProperties.getCloudEventConverter().getCloudEventSource())
                .typeMapper(typeMapper)
                .build();
    }
}
