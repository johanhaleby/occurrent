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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.dsl.projection.AppliedAppendStore;
import org.occurrent.eventstore.api.AppendId;
import org.occurrent.retry.Backoff;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link MongoAppliedAppendStore} against a real MongoDB replica set, since the compound unique index's
 * upsert idempotency, one document per (projection id, append id) pair, cross-instance visibility of
 * {@link AppliedAppendStore#recordApplied}, and the TTL index's in-place alteration on a changed retention are not
 * something an in-memory fake would catch.
 */
@DisplayName("MongoAppliedAppendStore")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(60)
class MongoAppliedAppendStoreTest {

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

    private AppliedAppendStore store(String collection) {
        return new MongoAppliedAppendStore(mongoOperations, collection, Duration.ofDays(7));
    }

    @Test
    void hasApplied_is_false_for_an_append_that_was_never_recorded() {
        AppliedAppendStore store = store(freshCollection());

        assertThat(store.hasApplied("orders", AppendId.mint())).isFalse();
    }

    @Test
    void recordApplied_makes_hasApplied_true() {
        AppliedAppendStore store = store(freshCollection());
        AppendId appendId = AppendId.mint();

        store.recordApplied("orders", appendId);

        assertThat(store.hasApplied("orders", appendId)).isTrue();
    }

    @Test
    void recording_the_same_append_twice_upserts_the_same_document_rather_than_creating_a_duplicate() {
        String collection = freshCollection();
        AppliedAppendStore store = store(collection);
        AppendId appendId = AppendId.mint();

        store.recordApplied("orders", appendId);
        store.recordApplied("orders", appendId);

        assertThat(store.hasApplied("orders", appendId)).isTrue();
        assertThat(mongoOperations.getCollection(collection).countDocuments()).isEqualTo(1);
    }

    @Test
    void each_projection_id_and_append_id_pair_gets_its_own_document_in_the_same_collection() {
        String collection = freshCollection();
        AppliedAppendStore store = store(collection);
        AppendId first = AppendId.mint();
        AppendId second = AppendId.mint();

        store.recordApplied("orders", first);
        store.recordApplied("shipments", second);

        assertThat(store.hasApplied("orders", first)).isTrue();
        assertThat(store.hasApplied("shipments", second)).isTrue();
        assertThat(store.hasApplied("orders", second)).isFalse();
        assertThat(mongoOperations.getCollection(collection).countDocuments()).isEqualTo(2);
    }

    @Test
    void clear_removes_every_document_recorded_for_a_projection_and_leaves_other_projections_alone() {
        String collection = freshCollection();
        AppliedAppendStore store = store(collection);
        AppendId orders = AppendId.mint();
        AppendId shipments = AppendId.mint();
        store.recordApplied("orders", orders);
        store.recordApplied("shipments", shipments);

        store.clear("orders");

        assertThat(store.hasApplied("orders", orders)).isFalse();
        assertThat(store.hasApplied("shipments", shipments)).isTrue();
    }

    @Test
    void waitUntilApplied_observes_an_append_recorded_by_a_different_store_instance_reading_the_same_collection() {
        String collection = freshCollection();
        AppliedAppendStore writer = store(collection);
        AppliedAppendStore reader = store(collection);
        AppendId appendId = AppendId.mint();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            scheduler.schedule(() -> writer.recordApplied("orders", appendId), 100, TimeUnit.MILLISECONDS);

            boolean applied = reader.waitUntilApplied("orders", appendId, Duration.ofSeconds(10), Backoff.fixed(20));

            assertThat(applied).isTrue();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void reconstructing_the_store_with_a_different_retention_alters_the_existing_ttl_index_instead_of_failing_startup() {
        String collection = freshCollection();
        // Indexes are created lazily, on first use, so the first store must actually be used once to create the
        // TTL index at the default 7-day retention before the second construction can hit its conflict.
        store(collection).hasApplied("orders", AppendId.mint());

        MongoAppliedAppendStore secondStore = new MongoAppliedAppendStore(mongoOperations, collection, Duration.ofDays(1));
        secondStore.hasApplied("orders", AppendId.mint());

        List<IndexInfo> indexes = mongoOperations.indexOps(collection).getIndexInfo();
        IndexInfo ttlIndex = indexes.stream().filter(index -> "recordedAt_ttl".equals(index.getName())).findFirst().orElseThrow();
        assertThat(ttlIndex.getExpireAfter()).hasValue(Duration.ofDays(1));
    }

    private static String freshCollection() {
        return "appliedAppends_" + UUID.randomUUID();
    }

    @SpringBootApplication
    static class Application {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);
        }
    }
}
