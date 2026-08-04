/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.testsupport.mongodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer.ownedBy;
import static org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer.scopedDatabaseName;

/**
 * Covers the naming rules on their own, without a Docker daemon. What the scope buys, and why a dot or an
 * over-long name has to fail loudly rather than quietly, is documented on the container itself.
 */
@DisplayName("Replica set ready MongoDB container")
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReplicaSetReadyMongoDBContainerTest {

    @Test
    void prefixes_the_database_with_the_scope_of_the_container_that_owns_it() {
        assertThat(scopedDatabaseName("oc4711_1", "test")).isEqualTo("oc4711_1_test");
    }

    @Test
    void rejects_a_dot_in_the_database_name() {
        // A connection string splits its path on the first dot, so 'test.events' would name the database
        // 'test' and quietly send the writes to a collection nobody asked for.
        assertThatThrownBy(() -> scopedDatabaseName("oc4711_1", "test.events"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot contain a dot")
                .hasMessageContaining("test.events");
    }

    @Test
    void rejects_a_name_that_would_exceed_the_limit_mongodb_puts_on_a_database() {
        String tooLong = "a".repeat(64);

        assertThatThrownBy(() -> scopedDatabaseName("oc4711_1", tooLong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds MongoDB's limit of 63");
    }

    @Test
    void accepts_a_name_that_lands_exactly_on_the_limit() {
        String scope = "oc4711_1";
        String fillsTheRest = "a".repeat(63 - scope.length() - 1);

        assertThat(scopedDatabaseName(scope, fillsTheRest)).hasSize(63);
    }

    @Test
    void a_scope_owns_only_the_databases_below_it() {
        // The cleanup sweep drops whatever this says it owns, so a prefix that reached one character too far
        // would delete a live run's data, which is the failure this scoping exists to remove. Process id 4711
        // must not claim 47110's databases, and container 1 must not claim container 18's.
        assertThat(ownedBy("oc4711").test("oc4711_1_test")).isTrue();
        assertThat(ownedBy("oc4711").test("oc47110_1_test")).isFalse();
        assertThat(ownedBy("oc4711_1").test("oc4711_1_test")).isTrue();
        assertThat(ownedBy("oc4711_1").test("oc4711_18_test")).isFalse();
    }
}
