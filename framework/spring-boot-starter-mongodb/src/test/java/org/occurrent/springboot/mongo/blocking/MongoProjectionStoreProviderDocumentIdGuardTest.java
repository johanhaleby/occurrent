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
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * {@code save} looks up the document by the state's own {@code @Id}, not by the id it is handed to write under.
 * Proves the store {@link MongoProjectionStoreProvider} builds fails loud with {@link IllegalStateException} on both
 * the single {@code save} and the bulk {@code saveAll} path when a state's {@code @Id} does not agree with the
 * resolved key, instead of silently writing under the state's own id and orphaning the read model, and that
 * matched-id writes are unaffected.
 */
@DisplayName("MongoProjectionStoreProvider document id guard")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(60)
class MongoProjectionStoreProviderDocumentIdGuardTest {

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

    private ViewStateRepository<State, String> repository() {
        return provider.createDefaultProjectionStore("guard", State.class);
    }

    // --- save: matched id is unaffected -------------------------------------------------------------------------

    @Test
    void save_persists_a_state_whose_Id_matches_the_resolved_key() {
        ViewStateRepository<State, String> repository = repository();
        String id = "matched-" + UUID.randomUUID();

        repository.save(id, new State(id, "value"));

        assertThat(mongoOperations.findById(id, State.class)).isEqualTo(new State(id, "value"));
    }

    // --- save: mismatched id fails loud --------------------------------------------------------------------------

    @Test
    void save_throws_IllegalStateException_when_the_states_Id_does_not_match_the_resolved_key() {
        ViewStateRepository<State, String> repository = repository();
        String resolvedKey = "resolved-" + UUID.randomUUID();
        String stateId = "different-" + UUID.randomUUID();

        Throwable thrown = catchThrowable(() -> repository.save(resolvedKey, new State(stateId, "value")));

        assertThat(thrown).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(stateId)
                .hasMessageContaining(resolvedKey);
    }

    @Test
    void save_does_not_write_any_document_when_the_states_Id_does_not_match_the_resolved_key() {
        ViewStateRepository<State, String> repository = repository();
        String resolvedKey = "resolved-" + UUID.randomUUID();
        String stateId = "different-" + UUID.randomUUID();

        catchThrowable(() -> repository.save(resolvedKey, new State(stateId, "value")));

        assertThat(mongoOperations.findById(resolvedKey, State.class)).isNull();
        assertThat(mongoOperations.findById(stateId, State.class)).isNull();
    }

    // --- saveAll: matched ids are unaffected ---------------------------------------------------------------------

    @Test
    void saveAll_persists_states_whose_Id_matches_the_resolved_key() {
        ViewStateRepository<State, String> repository = repository();
        String idA = "matched-a-" + UUID.randomUUID();
        String idB = "matched-b-" + UUID.randomUUID();

        repository.saveAll(Map.of(idA, new State(idA, "value-a"), idB, new State(idB, "value-b")));

        assertThat(mongoOperations.findById(idA, State.class)).isEqualTo(new State(idA, "value-a"));
        assertThat(mongoOperations.findById(idB, State.class)).isEqualTo(new State(idB, "value-b"));
    }

    // --- saveAll: mismatched id fails loud -------------------------------------------------------------------------

    @Test
    void saveAll_throws_IllegalStateException_when_one_entrys_Id_does_not_match_its_resolved_key() {
        ViewStateRepository<State, String> repository = repository();
        String matchedId = "matched-" + UUID.randomUUID();
        String resolvedKey = "resolved-" + UUID.randomUUID();
        String mismatchedStateId = "different-" + UUID.randomUUID();

        Throwable thrown = catchThrowable(() -> repository.saveAll(Map.of(
                matchedId, new State(matchedId, "value-ok"),
                resolvedKey, new State(mismatchedStateId, "value-bad"))));

        assertThat(thrown).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(mismatchedStateId)
                .hasMessageContaining(resolvedKey);
    }

    @Test
    void saveAll_writes_no_document_from_the_batch_when_one_entrys_Id_does_not_match_its_resolved_key() {
        ViewStateRepository<State, String> repository = repository();
        String matchedId = "matched-" + UUID.randomUUID();
        String resolvedKey = "resolved-" + UUID.randomUUID();
        String mismatchedStateId = "different-" + UUID.randomUUID();

        catchThrowable(() -> repository.saveAll(Map.of(
                matchedId, new State(matchedId, "value-ok"),
                resolvedKey, new State(mismatchedStateId, "value-bad"))));

        assertThat(mongoOperations.findById(matchedId, State.class)).isNull();
        assertThat(mongoOperations.findById(resolvedKey, State.class)).isNull();
        assertThat(mongoOperations.findById(mismatchedStateId, State.class)).isNull();
    }

    // --- a document with no id at all is left to the underlying write, not this guard ------------------------------

    @Test
    void save_does_not_reject_a_state_with_no_Id_set_leaving_id_generation_to_Mongo() {
        ViewStateRepository<GeneratedIdState, String> repository = provider.createDefaultProjectionStore("guard-generated", GeneratedIdState.class);

        assertThatCode(() -> repository.save("irrelevant-key-" + UUID.randomUUID(), new GeneratedIdState(null, "value")))
                .doesNotThrowAnyException();
    }

    @SpringBootApplication
    static class Application {
        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);
        }
    }

    record State(@Id String id, String value) {
    }

    record GeneratedIdState(@Id String id, String value) {
    }
}
