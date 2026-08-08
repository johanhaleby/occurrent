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
import org.occurrent.dsl.view.ViewStateRepository;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.Index;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.springframework.data.domain.Sort.Direction.ASC;

/**
 * Proves the {@code ViewStateRepository} that {@link MongoProjectionStoreProvider#createDefaultProjectionStore}
 * builds behaves identically through its {@code findAllById}/{@code saveAll} overrides as it does through the
 * looping {@code findById}/{@code save} defaults, just batched into fewer round trips.
 */
@DisplayName("MongoProjectionStoreProvider bulk operations")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(60)
class MongoProjectionStoreProviderBulkOperationsTest {

    private static ConfigurableApplicationContext context;
    private static MongoOperations mongoOperations;
    private static MongoProjectionStoreProvider provider;

    @BeforeAll
    static void startContext() {
        context = SpringApplication.run(new Class<?>[]{Application.class}, new String[]{"--spring.main.web-application-type=none"});
        mongoOperations = context.getBean(MongoOperations.class);
        provider = new MongoProjectionStoreProvider(context);
    }

    @AfterAll
    static void stopContext() {
        if (context != null) {
            context.close();
        }
    }

    private ViewStateRepository<PlainState, String> plainRepository() {
        return provider.createDefaultProjectionStore("plain", PlainState.class);
    }

    private ViewStateRepository<VersionedState, String> versionedRepository() {
        return provider.createDefaultProjectionStore("versioned", VersionedState.class);
    }

    // --- findAllById --------------------------------------------------------------------------------------------

    @Test
    void findAllById_returns_exactly_what_the_same_number_of_single_findById_calls_would_return_including_missing_ids() {
        ViewStateRepository<PlainState, String> repository = plainRepository();
        String prefix = "read-" + UUID.randomUUID() + "-";
        List<PlainState> present = List.of(
                new PlainState(prefix + "1", "value-1"),
                new PlainState(prefix + "2", "value-2"),
                new PlainState(prefix + "3", "value-3"));
        present.forEach(state -> repository.save(state.id(), state));
        List<String> missing = List.of(prefix + "missing-1", prefix + "missing-2");
        // Not insertion order, so a result keyed by DB/natural order rather than the requested ids' own order would
        // be caught here.
        List<String> ids = List.of(prefix + "3", prefix + "missing-1", prefix + "1", prefix + "missing-2", prefix + "2");

        Map<String, PlainState> bulkResult = repository.findAllById(ids);

        Map<String, PlainState> loopedResult = new LinkedHashMap<>();
        for (String id : ids) {
            repository.findById(id).ifPresent(state -> loopedResult.put(id, state));
        }
        assertThat(bulkResult).isEqualTo(loopedResult);
        assertThat(bulkResult.keySet()).doesNotContainAnyElementsOf(missing);
        assertThat(List.copyOf(bulkResult.keySet())).isEqualTo(List.of(prefix + "3", prefix + "1", prefix + "2"));
    }

    @Test
    void findAllById_on_an_empty_id_collection_returns_an_empty_map() {
        assertThat(plainRepository().findAllById(List.of())).isEmpty();
    }

    // --- saveAll: same persisted state as N single saves --------------------------------------------------------

    @Test
    void saveAll_persists_exactly_what_the_same_number_of_single_save_calls_would_persist() {
        ViewStateRepository<PlainState, String> repository = plainRepository();
        String prefix = "write-" + UUID.randomUUID() + "-";
        PlainState a = new PlainState(prefix + "a", "value-a");
        PlainState b = new PlainState(prefix + "b", "value-b");

        // save(id, state) writes under the state's own @Id, per the class javadoc; saveAll must do the same, so the
        // map keys below match the states' own ids, which the document-id guard now requires of every entry.
        repository.saveAll(Map.of(a.id(), a, b.id(), b));

        assertThat(mongoOperations.findById(a.id(), PlainState.class)).isEqualTo(a);
        assertThat(mongoOperations.findById(b.id(), PlainState.class)).isEqualTo(b);
    }

    @Test
    void saveAll_upserts_existing_entries_the_same_way_save_does() {
        ViewStateRepository<PlainState, String> repository = plainRepository();
        String id = "update-" + UUID.randomUUID();
        repository.save(id, new PlainState(id, "original"));

        repository.saveAll(Map.of(id, new PlainState(id, "updated")));

        assertThat(mongoOperations.findById(id, PlainState.class)).isEqualTo(new PlainState(id, "updated"));
    }

    @Test
    void saveAll_on_an_empty_map_is_a_no_op() {
        assertThatCode(() -> plainRepository().saveAll(Map.of())).doesNotThrowAnyException();
    }

    // --- saveAll: @Version bookkeeping matches save --------------------------------------------------------------

    @Test
    void saveAll_initializes_a_fresh_versioned_entrys_version_the_same_way_a_single_save_does() {
        ViewStateRepository<VersionedState, String> repository = versionedRepository();
        String loopedId = "fresh-looped-" + UUID.randomUUID();
        String bulkId = "fresh-bulk-" + UUID.randomUUID();

        repository.save(loopedId, new VersionedState(loopedId, "v1", null));
        repository.saveAll(Map.of(bulkId, new VersionedState(bulkId, "v1", null)));

        VersionedState loopedPersisted = mongoOperations.findById(loopedId, VersionedState.class);
        VersionedState bulkPersisted = mongoOperations.findById(bulkId, VersionedState.class);
        assertThat(bulkPersisted.version()).isEqualTo(loopedPersisted.version());
        assertThat(bulkPersisted.version()).isEqualTo(0L);
    }

    @Test
    void saveAll_increments_an_existing_versioned_entrys_version_the_same_way_a_single_save_does() {
        ViewStateRepository<VersionedState, String> repository = versionedRepository();
        String id = "increment-" + UUID.randomUUID();
        repository.save(id, new VersionedState(id, "v1", null));
        VersionedState afterFirstSave = mongoOperations.findById(id, VersionedState.class);

        repository.saveAll(Map.of(id, new VersionedState(id, "v2", afterFirstSave.version())));

        VersionedState afterBulkSave = mongoOperations.findById(id, VersionedState.class);
        assertThat(afterBulkSave.value()).isEqualTo("v2");
        assertThat(afterBulkSave.version()).isEqualTo(afterFirstSave.version() + 1);
    }

    // --- exception translation: OptimisticLockingFailureException -------------------------------------------------

    @Test
    void saveAll_surfaces_OptimisticLockingFailureException_when_an_entrys_version_has_moved_on_same_as_save() {
        ViewStateRepository<VersionedState, String> repository = versionedRepository();
        String id = "stale-" + UUID.randomUUID();
        repository.save(id, new VersionedState(id, "v1", null));
        VersionedState staleRead = mongoOperations.findById(id, VersionedState.class);
        // A concurrent writer moves the version on before this saveAll executes.
        repository.save(id, new VersionedState(id, "v2-from-elsewhere", staleRead.version()));

        Throwable thrown = catchThrowable(() -> repository.saveAll(Map.of(id, new VersionedState(id, "v2-stale", staleRead.version()))));

        assertThat(thrown).isInstanceOf(OptimisticLockingFailureException.class);
        assertThat(mongoOperations.findById(id, VersionedState.class).value()).isEqualTo("v2-from-elsewhere");
    }

    // --- exception translation: DuplicateKeyException ---------------------------------------------------------

    @Test
    void saveAll_surfaces_DuplicateKeyException_not_the_raw_bulk_write_wrapper_for_a_unique_index_violation() {
        mongoOperations.indexOps(UniqueFieldState.class).ensureIndex(new Index().on("uniqueValue", ASC).unique());
        String uniqueValue = "dup-" + UUID.randomUUID();
        UniqueFieldState first = new UniqueFieldState("id-" + UUID.randomUUID(), uniqueValue);
        UniqueFieldState second = new UniqueFieldState("id-" + UUID.randomUUID(), uniqueValue);
        Map<String, UniqueFieldState> states = Map.of(first.id(), first, second.id(), second);
        ViewStateRepository<UniqueFieldState, String> repository = provider.createDefaultProjectionStore("unique", UniqueFieldState.class);

        Throwable thrown = catchThrowable(() -> repository.saveAll(states));

        assertThat(thrown).isInstanceOf(DuplicateKeyException.class);
    }

    @Test
    void single_save_also_throws_DuplicateKeyException_for_the_same_unique_index_violation_proving_the_bulk_path_matches_it() {
        mongoOperations.indexOps(UniqueFieldState.class).ensureIndex(new Index().on("uniqueValue", ASC).unique());
        String uniqueValue = "dup-single-" + UUID.randomUUID();
        mongoOperations.insert(new UniqueFieldState("id-" + UUID.randomUUID(), uniqueValue));

        Throwable thrown = catchThrowable(() -> mongoOperations.insert(new UniqueFieldState("id-" + UUID.randomUUID(), uniqueValue)));

        assertThat(thrown).isInstanceOf(DuplicateKeyException.class);
    }

    @SpringBootApplication
    static class Application {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);
        }
    }

    record PlainState(@Id String id, String value) {
    }

    record VersionedState(@Id String id, String value, @Version Long version) {
    }

    record UniqueFieldState(@Id String id, String uniqueValue) {
    }
}
