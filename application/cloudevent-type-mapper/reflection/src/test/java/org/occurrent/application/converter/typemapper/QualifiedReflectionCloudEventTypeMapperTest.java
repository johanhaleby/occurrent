/*
 *
 *  Copyright 2021 Johan Haleby
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

package org.occurrent.application.converter.typemapper;

import org.junit.jupiter.api.Test;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;

import static org.assertj.core.api.Assertions.assertThat;

public class QualifiedReflectionCloudEventTypeMapperTest {

    @Test
    void getDomainEventType_returns_domain_event_type_from_fully_qualified_class_name() {
        // Given
        ReflectionCloudEventTypeMapper<DomainEvent> typeMapper = ReflectionCloudEventTypeMapper.qualified();

        // When
        Class<DomainEvent> domainEventType = typeMapper.getDomainEventType(NameDefined.class.getName());

        // Then
        assertThat(domainEventType).isEqualTo(NameDefined.class);
    }

    @Test
    void getCloudEventType_returns_fully_qualified_class_name_from_domain_event_type() {
        // Given
        ReflectionCloudEventTypeMapper<DomainEvent> typeMapper = ReflectionCloudEventTypeMapper.qualified();

        // When
        String cloudEventType = typeMapper.getCloudEventType(NameDefined.class);

        // Then
        assertThat(cloudEventType).isEqualTo(NameDefined.class.getName());
    }
}
