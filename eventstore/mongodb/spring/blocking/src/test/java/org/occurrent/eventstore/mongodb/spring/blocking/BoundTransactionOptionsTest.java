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

package org.occurrent.eventstore.mongodb.spring.blocking;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcernLevel;
import com.mongodb.TransactionOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Deployment;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Scope;
import org.occurrent.testsupport.mongodb.TwoShardMongoCluster;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins what the append check relies on in Spring Data 5.1: that {@code MongoTransactionManager.doGetTransaction} reaches
 * the session bound to the template's factory, and that the manager reads a transaction's labels from a
 * {@code TransactionAttribute} (dec-0340 condition 2). Runs through a mongos, where the options decide the scope.
 */
@DisplayName("BoundTransactionOptions")
@DisplayNameGeneration(ReplaceUnderscores.class)
class BoundTransactionOptionsTest {

    private static TwoShardMongoCluster cluster;
    private static MongoClient client;
    private static SimpleMongoClientDatabaseFactory databaseFactory;
    private static MongoTemplate template;
    private static MongoTransactionManager transactionManager;
    private static BoundTransactionOptions boundOptions;

    @BeforeAll
    static void start_the_cluster_and_wire_one_template_and_one_transaction_manager_on_one_factory() {
        cluster = TwoShardMongoCluster.start();
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(cluster.getConnectionString())).build());
        databaseFactory = new SimpleMongoClientDatabaseFactory(client, cluster.createDatabase(TwoShardMongoCluster.SECOND_SHARD));
        template = new MongoTemplate(databaseFactory);
        transactionManager = new MongoTransactionManager(databaseFactory);
        boundOptions = new BoundTransactionOptions(template.getMongoDatabaseFactory());
    }

    @AfterAll
    static void stop_the_cluster() {
        if (client != null) {
            client.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void the_options_are_null_outside_a_transaction() {
        assertThat(boundOptions.current()).isNull();
    }

    @Test
    void the_options_of_a_plain_transaction_template_have_neither_read_concern_snapshot_nor_write_concern_majority() {
        TransactionOptions options = new TransactionTemplate(transactionManager).execute(status -> boundOptions.current());

        assertThat(options).as("the session is bound and its transaction started without a write").isNotNull();
        assertThat(options.getReadConcern() == null || options.getReadConcern().getLevel() == null).as("read concern %s", options.getReadConcern()).isTrue();
        assertThat(options.getWriteConcern() == null || !"majority".equals(options.getWriteConcern().getWObject())).as("write concern %s", options.getWriteConcern()).isTrue();
        assertThat(Scope.of(Deployment.MONGOS, options)).isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void the_options_of_a_labelled_transaction_template_are_read_concern_snapshot_and_write_concern_majority() {
        TransactionTemplate labelled = new LabelledTransactionTemplate(new TransactionTemplate(transactionManager), DcbAppendConditionCheck.SNAPSHOT_THROUGH_MONGOS_LABELS);

        TransactionOptions options = labelled.execute(status -> boundOptions.current());

        assertThat(options).isNotNull();
        assertThat(options.getReadConcern().getLevel()).isEqualTo(ReadConcernLevel.SNAPSHOT);
        assertThat(options.getWriteConcern().getWObject()).isEqualTo("majority");
        assertThat(Scope.of(Deployment.MONGOS, options)).isEqualTo(Scope.SNAPSHOT_THROUGH_MONGOS);
    }

    @Test
    void the_options_of_a_labelled_template_that_joins_a_callers_plain_transaction_are_the_callers() {
        TransactionTemplate labelled = new LabelledTransactionTemplate(new TransactionTemplate(transactionManager), DcbAppendConditionCheck.SNAPSHOT_THROUGH_MONGOS_LABELS);

        TransactionOptions options = new TransactionTemplate(transactionManager).execute(outer -> labelled.execute(inner -> boundOptions.current()));

        assertThat(options).isNotNull();
        assertThat(Scope.of(Deployment.MONGOS, options))
                .as("a joined transaction keeps the options it started with, so the labels of the append don't apply")
                .isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void the_options_are_null_when_read_through_the_factory_the_transaction_is_not_bound_to() {
        BoundTransactionOptions other = new BoundTransactionOptions(new SimpleMongoClientDatabaseFactory(client, databaseFactory.getMongoDatabase().getName()));
        TransactionTemplate labelled = new LabelledTransactionTemplate(new TransactionTemplate(transactionManager), DcbAppendConditionCheck.SNAPSHOT_THROUGH_MONGOS_LABELS);

        TransactionOptions options = labelled.execute(status -> other.current());

        assertThat(options).isNull();
    }
}
