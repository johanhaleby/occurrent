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

package org.occurrent.springboot.mongo.reactor;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.projection.AppliedPositionStorage;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link ReactiveMongoAppliedPositionStorage} against a real MongoDB replica set, since {@code $max} upsert
 * behaviour, one document per projection id, and cross-instance visibility of {@link AppliedPositionStorage#advance}
 * are not something an in-memory fake would catch.
 */
@DisplayName("ReactiveMongoAppliedPositionStorage")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(60)
class ReactiveMongoAppliedPositionStorageTest {

    private static ConfigurableApplicationContext context;
    private static ReactiveMongoOperations mongoOperations;

    @BeforeAll
    static void startContext() {
        context = SpringApplication.run(new Class<?>[]{Application.class}, new String[]{"--spring.main.web-application-type=none"});
        mongoOperations = context.getBean(ReactiveMongoOperations.class);
    }

    @AfterAll
    static void stopContext() {
        if (context != null) {
            context.close();
        }
    }

    private AppliedPositionStorage storage(String collection) {
        return new ReactiveMongoAppliedPositionStorage(mongoOperations, collection);
    }

    @Test
    void appliedPosition_is_empty_for_a_projection_that_has_never_advanced() {
        AppliedPositionStorage storage = storage(freshCollection());

        assertThat(storage.appliedPosition("orders")).isEmpty();
    }

    @Test
    void advance_records_the_position_and_appliedPosition_reads_it_back() {
        AppliedPositionStorage storage = storage(freshCollection());

        storage.advance("orders", 42);

        assertThat(storage.appliedPosition("orders")).hasValue(42L);
    }

    @Test
    void advance_never_moves_the_recorded_position_backwards() {
        AppliedPositionStorage storage = storage(freshCollection());

        storage.advance("orders", 50);
        storage.advance("orders", 10);

        assertThat(storage.appliedPosition("orders")).hasValue(50L);
    }

    @Test
    void advance_rejects_a_non_positive_position() {
        AppliedPositionStorage storage = storage(freshCollection());

        assertThat(org.assertj.core.api.Assertions.catchThrowable(() -> storage.advance("orders", 0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void each_projection_id_gets_its_own_document_in_the_same_collection() {
        String collection = freshCollection();
        AppliedPositionStorage storage = storage(collection);

        storage.advance("orders", 10);
        storage.advance("shipments", 20);

        assertThat(storage.appliedPosition("orders")).hasValue(10L);
        assertThat(storage.appliedPosition("shipments")).hasValue(20L);
        assertThat(mongoOperations.findAll(org.bson.Document.class, collection).collectList().block()).hasSize(2);
    }

    @Test
    void waitUntilApplied_observes_a_position_advanced_by_a_different_storage_instance_reading_the_same_collection() {
        String collection = freshCollection();
        AppliedPositionStorage writer = storage(collection);
        AppliedPositionStorage reader = storage(collection);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            scheduler.schedule(() -> writer.advance("orders", 42), 100, TimeUnit.MILLISECONDS);

            boolean caughtUp = reader.waitUntilApplied("orders", 42, Duration.ofSeconds(10), Duration.ofMillis(20));

            assertThat(caughtUp).isTrue();
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static String freshCollection() {
        return "appliedPositions_" + UUID.randomUUID();
    }

    @SpringBootApplication
    static class Application {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);
        }
    }
}
