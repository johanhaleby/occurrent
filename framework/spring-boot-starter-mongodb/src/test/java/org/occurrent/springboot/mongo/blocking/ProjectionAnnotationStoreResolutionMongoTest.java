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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves how the blocking {@code @Projection} registrar resolves the read-model store by type ({@code store = X.class})
 * and by name ({@code storeName}). A unique bean of the type resolves, several beans of the type without a
 * {@code storeName} fail fast, several beans with a {@code storeName} disambiguate, an unmatched type fails fast rather
 * than silently falling back to the Mongo default, and a named bean of the wrong shape fails fast.
 */
@DisplayName("Projection annotation store resolution")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class ProjectionAnnotationStoreResolutionMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer;

    static {
        mongoDBContainer = new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        mongoDBContainer.withReuse(true);
        mongoDBContainer.setPortBindings(List.of("27017:27017"));
    }

    private static String[] bootArgs(String databaseName) {
        return new String[]{
                "--spring.data.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=dcb",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName
        };
    }

    @Test
    void a_store_type_with_no_matching_bean_fails_fast() {
        assertThatThrownBy(() -> SpringApplication.run(NoBeanOfTypeApplication.class, bootArgs("blocking-store-nobean")))
                .hasStackTraceContaining("found no bean of type");
    }

    @Test
    void several_beans_of_the_store_type_without_a_storeName_fails_fast() {
        assertThatThrownBy(() -> SpringApplication.run(AmbiguousStoreApplication.class, bootArgs("blocking-store-ambiguous")))
                .hasStackTraceContaining("Disambiguate with storeName");
    }

    @Test
    void several_beans_of_the_store_type_are_disambiguated_by_a_storeName() {
        try (ConfigurableApplicationContext ctx = SpringApplication.run(DisambiguatedStoreApplication.class, bootArgs("blocking-store-disambig"))) {
            assertThat(ctx.isRunning()).isTrue();
        }
    }

    @Test
    void a_named_store_bean_of_the_wrong_shape_fails_fast() {
        assertThatThrownBy(() -> SpringApplication.run(WrongShapeStoreApplication.class, bootArgs("blocking-store-wrongshape")))
                .hasStackTraceContaining("must be a MaterializedView, a ViewStateRepository, or a Spring Data CrudRepository");
    }

    private static CloudEventConverter<TestEvent> newConverter(CloudEventTypeMapper<TestEvent> typeMapper, String source) {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create(source))
                .typeMapper(typeMapper)
                .idMapper(TestEvent::eventId)
                .build();
    }

    private static DcbProjection<Integer, TestEvent, String> countProjection() {
        Projection<Integer, TestEvent, String> projection = Projection.<Integer, TestEvent, String>builder(0)
                .id(event -> "k")
                .on(Registered.class, (state, event) -> state + 1)
                .build();
        return new DcbProjection<>(projection, DcbCriteria.all());
    }

    // A store type with no bean of that type declared, so resolution must fail fast instead of using the Mongo default.
    @SpringBootApplication
    @EnableOccurrent
    static class NoBeanOfTypeApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> converter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper, "urn:occurrent:blocking-store-nobean");
        }

        @Bean
        NoBeanOfTypeProvider provider() {
            return new NoBeanOfTypeProvider();
        }
    }

    @Component
    static class NoBeanOfTypeProvider {
        @org.occurrent.annotation.Projection(id = "no-bean", store = CountStore.class)
        DcbProjection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    // Two beans of the same store type and no storeName, so resolution cannot pick one.
    @SpringBootApplication
    @EnableOccurrent
    static class AmbiguousStoreApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> converter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper, "urn:occurrent:blocking-store-ambiguous");
        }

        @Bean
        CountStore storeA() {
            return new CountStore();
        }

        @Bean
        CountStore storeB() {
            return new CountStore();
        }

        @Bean
        AmbiguousStoreProvider provider() {
            return new AmbiguousStoreProvider();
        }
    }

    @Component
    static class AmbiguousStoreProvider {
        @org.occurrent.annotation.Projection(id = "ambiguous", store = CountStore.class)
        DcbProjection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    // Two beans of the same store type, disambiguated by storeName, so the context starts cleanly.
    @SpringBootApplication
    @EnableOccurrent
    static class DisambiguatedStoreApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> converter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper, "urn:occurrent:blocking-store-disambig");
        }

        @Bean
        CountStore storeA() {
            return new CountStore();
        }

        @Bean
        CountStore storeB() {
            return new CountStore();
        }

        @Bean
        DisambiguatedStoreProvider provider() {
            return new DisambiguatedStoreProvider();
        }
    }

    @Component
    static class DisambiguatedStoreProvider {
        @org.occurrent.annotation.Projection(id = "disambiguated", store = CountStore.class, storeName = "storeA")
        DcbProjection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    // A named bean that is not a store shape, so it cannot be adapted into a MaterializedView.
    @SpringBootApplication
    @EnableOccurrent
    static class WrongShapeStoreApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> converter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper, "urn:occurrent:blocking-store-wrongshape");
        }

        @Bean
        String notAStore() {
            return "not a store";
        }

        @Bean
        WrongShapeStoreProvider provider() {
            return new WrongShapeStoreProvider();
        }
    }

    @Component
    static class WrongShapeStoreProvider {
        @org.occurrent.annotation.Projection(id = "wrong-shape", storeName = "notAStore")
        DcbProjection<Integer, TestEvent, String> projection() {
            return countProjection();
        }
    }

    static class CountStore implements ViewStateRepository<Integer, String> {
        private final ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();

        @Override
        public Optional<Integer> findById(String id) {
            return Optional.ofNullable(map.get(id));
        }

        @Override
        public void save(String id, Integer state) {
            map.put(id, state);
        }
    }

    sealed interface TestEvent {
        String eventId();
    }

    record Registered(String eventId) implements TestEvent {
    }
}
