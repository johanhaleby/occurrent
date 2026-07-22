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

import org.occurrent.dsl.snapshot.mongodb.spring.blocking.SpringMongoSnapshotStore;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.snapshot.Snapshot;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoOperations;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Proves the MongoDB {@code SnapshotStore} round-trips both scalar and POJO state, and that an unreadable stored snapshot
 * (for example after a state-shape change) degrades to an empty result instead of throwing, so a snapshot never fails a
 * command that loads it.
 */
@DisplayName("SpringMongoSnapshotStore")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class SpringMongoSnapshotStoreTest {

    private static ConfigurableApplicationContext context;
    private static MongoOperations mongoOperations;

    @BeforeAll
    static void startContext() {
        context = SpringApplication.run(new Class<?>[]{Application.class}, new String[]{"--spring.main.web-application-type=none"});
        mongoOperations = context.getBean(MongoOperations.class);
    }

    @AfterAll
    static void stopContext() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void round_trips_scalar_state() {
        SpringMongoSnapshotStore<Long> store = new SpringMongoSnapshotStore<>(mongoOperations, Long.class, "snapshot-scalar");
        store.save("account-1", new Snapshot<>(4711L, 7, 2));

        Optional<Snapshot<Long>> found = store.findLatest("account-1");

        assertThat(found).isPresent();
        assertThat(found.get().state()).isEqualTo(4711L);
        assertThat(found.get().version()).isEqualTo(7L);
        assertThat(found.get().schemaVersion()).isEqualTo(2);
    }

    @Test
    void round_trips_pojo_state() {
        SpringMongoSnapshotStore<Balance> store = new SpringMongoSnapshotStore<>(mongoOperations, Balance.class, "snapshot-pojo");
        store.save("account-2", new Snapshot<>(new Balance(250, true), 12, 1));

        Optional<Snapshot<Balance>> found = store.findLatest("account-2");

        assertThat(found).isPresent();
        assertThat(found.get().state()).isEqualTo(new Balance(250, true));
        assertThat(found.get().version()).isEqualTo(12L);
    }

    @Test
    void degrades_to_empty_when_the_stored_state_cannot_be_read() {
        String collection = "snapshot-unreadable";
        // A state value that cannot be converted into the declared state type, standing in for a snapshot written under an
        // incompatible state shape. findLatest must return empty (falling back to a full replay), not throw.
        mongoOperations.save(new Document("_id", "account-3").append("version", 3L).append("schemaVersion", 1).append("state", "not-a-balance"), collection);
        SpringMongoSnapshotStore<Balance> store = new SpringMongoSnapshotStore<>(mongoOperations, Balance.class, collection);

        assertThatCode(() -> assertThat(store.findLatest("account-3")).isEmpty()).doesNotThrowAnyException();
    }

    @SpringBootApplication
    static class Application {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return new MongoDBContainer("mongo:" + System.getProperty("test.mongo.version")).withReplicaSet();
        }
    }

    record Balance(int amount, boolean closed) {
    }
}
