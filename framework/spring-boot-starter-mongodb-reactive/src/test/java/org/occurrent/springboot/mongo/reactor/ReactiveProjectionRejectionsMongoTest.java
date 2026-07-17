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

package org.occurrent.springboot.mongo.reactor;

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
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the reactive {@code @Projection} registrar's deliberate v1 rejections fail fast at startup with a clear message:
 * a duplicate id, a {@code DcbProjection} in SYNCHRONOUS mode (unsupported on the reactive stack), and a projection with
 * no resolvable store (there is no zero-config reactive Mongo default).
 */
@DisplayName("Reactive Projection annotation rejections")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class ReactiveProjectionRejectionsMongoTest {

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
    void a_duplicate_projection_id_fails_fast() {
        assertThatThrownBy(() -> SpringApplication.run(DuplicateIdApplication.class, bootArgs("reactive-projection-dupid")))
                .hasStackTraceContaining("Duplicate subscription/projection id 'dup'");
    }

    @Test
    void a_dcb_projection_in_synchronous_mode_fails_fast() {
        assertThatThrownBy(() -> SpringApplication.run(DcbSynchronousApplication.class, bootArgs("reactive-projection-dcbsync")))
                .hasStackTraceContaining("mode = SYNCHRONOUS, which the reactive stack does not support");
    }

    @Test
    void a_projection_with_no_store_fails_fast() {
        assertThatThrownBy(() -> SpringApplication.run(NoStoreApplication.class, bootArgs("reactive-projection-nostore")))
                .hasStackTraceContaining("has no read-model store");
    }

    @Test
    void several_beans_of_the_store_type_without_a_storeName_fails_fast() {
        assertThatThrownBy(() -> SpringApplication.run(AmbiguousStoreApplication.class, bootArgs("reactive-projection-ambiguous")))
                .hasStackTraceContaining("Disambiguate with storeName");
    }

    private static CloudEventConverter<TestEvent> newConverter(CloudEventTypeMapper<TestEvent> typeMapper, String source) {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create(source))
                .typeMapper(typeMapper)
                .idMapper(TestEvent::eventId)
                .build();
    }

    private static ViewStateRepository<Integer, String> inMemoryStore() {
        ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
        return ViewStateRepository.create(map::get, map::put);
    }

    private static DcbProjection<Integer, TestEvent, String> dcbProjection() {
        Projection<Integer, TestEvent, String> projection = Projection.<Integer, TestEvent, String>builder(0)
                .id(event -> "k")
                .on(Registered.class, (state, event) -> state + 1)
                .build();
        return new DcbProjection<>(projection, DcbCriteria.all());
    }

    @SpringBootApplication
    @EnableOccurrentReactive
    static class DuplicateIdApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> converter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper, "urn:occurrent:reactive-projection-dupid");
        }

        @Bean
        ViewStateRepository<Integer, String> store() {
            return inMemoryStore();
        }

        @Bean
        DuplicateProvider duplicateProvider() {
            return new DuplicateProvider();
        }
    }

    @Component
    static class DuplicateProvider {
        @org.occurrent.annotation.Projection(id = "dup")
        DcbProjection<Integer, TestEvent, String> one() {
            return dcbProjection();
        }

        @org.occurrent.annotation.Projection(id = "dup")
        DcbProjection<Integer, TestEvent, String> two() {
            return dcbProjection();
        }
    }

    @SpringBootApplication
    @EnableOccurrentReactive
    static class DcbSynchronousApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> converter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper, "urn:occurrent:reactive-projection-dcbsync");
        }

        @Bean
        ViewStateRepository<Integer, String> store() {
            return inMemoryStore();
        }

        @Bean
        DcbSynchronousProvider dcbSynchronousProvider() {
            return new DcbSynchronousProvider();
        }
    }

    @Component
    static class DcbSynchronousProvider {
        @org.occurrent.annotation.Projection(id = "dcb-sync", mode = org.occurrent.annotation.Mode.SYNCHRONOUS)
        DcbProjection<Integer, TestEvent, String> projection() {
            return dcbProjection();
        }
    }

    @SpringBootApplication
    @EnableOccurrentReactive
    static class NoStoreApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> converter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper, "urn:occurrent:reactive-projection-nostore");
        }

        @Bean
        NoStoreProvider noStoreProvider() {
            return new NoStoreProvider();
        }
    }

    @Component
    static class NoStoreProvider {
        @org.occurrent.annotation.Projection(id = "no-store")
        Projection<Integer, TestEvent, String> projection() {
            return Projection.<Integer, TestEvent, String>builder(0)
                    .id(event -> "k")
                    .on(Registered.class, (state, event) -> state + 1)
                    .build();
        }
    }

    @SpringBootApplication
    @EnableOccurrentReactive
    static class AmbiguousStoreApplication {
        @Bean
        CloudEventTypeMapper<TestEvent> typeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> converter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper, "urn:occurrent:reactive-projection-ambiguous");
        }

        @Bean
        ViewStateRepository<Integer, String> storeA() {
            return inMemoryStore();
        }

        @Bean
        ViewStateRepository<Integer, String> storeB() {
            return inMemoryStore();
        }

        @Bean
        AmbiguousStoreProvider ambiguousStoreProvider() {
            return new AmbiguousStoreProvider();
        }
    }

    @Component
    static class AmbiguousStoreProvider {
        @org.occurrent.annotation.Projection(id = "ambiguous", store = ViewStateRepository.class)
        DcbProjection<Integer, TestEvent, String> projection() {
            return dcbProjection();
        }
    }

    sealed interface TestEvent {
        String eventId();
    }

    record Registered(String eventId) implements TestEvent {
    }
}
