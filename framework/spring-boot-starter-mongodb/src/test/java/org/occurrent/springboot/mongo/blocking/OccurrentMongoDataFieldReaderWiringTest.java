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

package org.occurrent.springboot.mongo.blocking;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.inmemory.filtermatching.DataFieldReader;
import org.occurrent.inmemory.filtermatching.jackson.JacksonDataFieldReader;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The payload reader is contributed only when {@code occurrent-common-inmemory-filter-matching-jackson} is present,
 * which is an optional dependency of this starter. The case worth a test is its absence: the auto-configuration class
 * names {@code JacksonDataFieldReader} inside a bean method, so this proves the class still loads and the rest of the
 * context still wires when that type is not on the classpath.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class OccurrentMongoDataFieldReaderWiringTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OccurrentMongoAutoConfiguration.class))
            .withUserConfiguration(EnableOccurrentConfiguration.class, TypeMapperConfiguration.class)
            .withBean(MongoDatabaseFactory.class, () -> mock(MongoDatabaseFactory.class))
            .withBean(MongoTemplate.class, () -> mock(MongoTemplate.class))
            .withPropertyValues(
                    "occurrent.event-store.enabled=false",
                    // Subscriptions need an EventStoreQueries bean, which the line above removes. Turning both on is
                    // more scaffolding than a bean-presence test needs, and the reactive starter's wiring test already
                    // covers the absent-reader case with subscriptions running.
                    "occurrent.subscription.enabled=false",
                    "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:test"
            );

    @Test
    void a_data_field_reader_is_contributed_when_the_jackson_reader_is_on_the_classpath() {
        contextRunner.run(context ->
                assertThat(context).getBean(DataFieldReader.class).isInstanceOf(JacksonDataFieldReader.class));
    }

    @Test
    void an_application_supplied_data_field_reader_wins() {
        DataFieldReader own = (cloudEvent, path) -> Optional.empty();

        contextRunner.withBean(DataFieldReader.class, () -> own).run(context ->
                assertThat(context).getBean(DataFieldReader.class).isSameAs(own));
    }

    @Test
    void no_data_field_reader_bean_exists_and_the_auto_configuration_still_loads_without_the_jackson_reader() {
        contextRunner
                .withClassLoader(new FilteredClassLoader(JacksonDataFieldReader.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(DataFieldReader.class);
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableOccurrent
    static class EnableOccurrentConfiguration {
    }

    @Configuration(proxyBeanMethods = false)
    static class TypeMapperConfiguration {
        @Bean
        CloudEventTypeMapper testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }
    }
}
