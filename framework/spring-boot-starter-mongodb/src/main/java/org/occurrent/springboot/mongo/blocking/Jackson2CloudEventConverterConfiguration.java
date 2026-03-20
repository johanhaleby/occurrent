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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;

@Configuration(proxyBeanMethods = false)
@ConditionalOnMissingBean(type = "tools.jackson.databind.ObjectMapper")
class Jackson2CloudEventConverterConfiguration {

    @Bean
    @ConditionalOnMissingBean(CloudEventConverter.class)
    public <E> CloudEventConverter<E> occurrentCloudEventConverter(Optional<ObjectMapper> objectMapper, OccurrentProperties occurrentProperties, CloudEventTypeMapper<E> cloudEventTypeMapper) {
        ObjectMapper om = objectMapper.orElseGet(ObjectMapper::new);
        return new JacksonCloudEventConverter.Builder<E>(om, occurrentProperties.getCloudEventConverter().getCloudEventSource())
                .typeMapper(cloudEventTypeMapper)
                .build();
    }
}
