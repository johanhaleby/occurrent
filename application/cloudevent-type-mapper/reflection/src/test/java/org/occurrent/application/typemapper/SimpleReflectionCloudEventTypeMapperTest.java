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

package org.occurrent.application.typemapper;

import org.junit.jupiter.api.Test;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.application.typemapper.ClassName.simple;

public class SimpleReflectionCloudEventTypeMapperTest {

    @Test
    void getDomainEventType_returns_domain_event_type_from_simple_name_when_specifying_domain_event_class() {
        // Given
        ReflectionCloudEventTypeMapper<DomainEvent> typeMapper = ReflectionCloudEventTypeMapper.simple(DomainEvent.class);

        // When
        Class<DomainEvent> domainEventType = typeMapper.getDomainEventType(NameDefined.class.getSimpleName());

        // Then
        assertThat(domainEventType).isEqualTo(NameDefined.class);
    }

    @Test
    void getDomainEventType_returns_domain_event_type_from_simple_name_when_specifying_domain_event_package() {
        // Given
        ReflectionCloudEventTypeMapper<DomainEvent> typeMapper = ReflectionCloudEventTypeMapper.fromClassName(simple(DomainEvent.class.getPackage()));

        // When
        Class<DomainEvent> domainEventType = typeMapper.getDomainEventType(NameDefined.class.getSimpleName());

        // Then
        assertThat(domainEventType).isEqualTo(NameDefined.class);
    }


    @Test
    void getDomainEventType_returns_domain_event_type_from_simple_name_when_specifying_domain_event_package_name() {
        // Given
        ReflectionCloudEventTypeMapper<DomainEvent> typeMapper = ReflectionCloudEventTypeMapper.fromClassName(simple(DomainEvent.class.getPackage().getName()));

        // When
        Class<DomainEvent> domainEventType = typeMapper.getDomainEventType(NameDefined.class.getSimpleName());

        // Then
        assertThat(domainEventType).isEqualTo(NameDefined.class);
    }

    @Test
    void getDomainEventType_returns_domain_event_type_from_simple_name_when_specifying_domain_event_package_name_that_ends_with_dot() {
        // Given
        ReflectionCloudEventTypeMapper<DomainEvent> typeMapper = ReflectionCloudEventTypeMapper.fromClassName(simple(DomainEvent.class.getPackage().getName() + "."));

        // When
        Class<DomainEvent> domainEventType = typeMapper.getDomainEventType(NameDefined.class.getSimpleName());

        // Then
        assertThat(domainEventType).isEqualTo(NameDefined.class);
    }

    @Test
    void getDomainEventType_returns_domain_event_type_from_simple_name_when_specifying_domain_event_mapping_function() {
        // Given
        ReflectionCloudEventTypeMapper<DomainEvent> typeMapper = ReflectionCloudEventTypeMapper.simple(name -> {
            final Class<? extends DomainEvent> domainEventType;
            switch (name) {
                case "NameDefined":
                    domainEventType = NameDefined.class;
                    break;
                case "NameChanged":
                    domainEventType = NameWasChanged.class;
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + name);
            }
            return domainEventType;
        });

        // When
        Class<DomainEvent> domainEventType = typeMapper.getDomainEventType(NameDefined.class.getSimpleName());

        // Then
        assertThat(domainEventType).isEqualTo(NameDefined.class);
    }

    @Test
    void getCloudEventType_returns_simple_class_name_from_domain_event_type() {
        // Given
        ReflectionCloudEventTypeMapper<DomainEvent> typeMapper = ReflectionCloudEventTypeMapper.qualified();

        // When
        String cloudEventType = typeMapper.getCloudEventType(NameDefined.class);

        // Then
        assertThat(cloudEventType).isEqualTo(NameDefined.class.getName());
    }
}
