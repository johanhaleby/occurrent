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
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Deployment;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Scope;
import org.occurrent.testsupport.mongodb.TwoShardMongoCluster;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.transaction.support.TransactionTemplate;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins what the append check relies on in Spring Data 5.1: that {@code MongoTransactionManager.doGetTransaction} reaches
 * the session bound to the template's factory, and that the store can abort the transaction its own template just began
 * on that session and start it again at read concern snapshot and write concern majority (ADR 175). Runs through a
 * mongos, where the options decide the scope.
 */
@DisplayName("BoundTransactionOptions")
@DisplayNameGeneration(ReplaceUnderscores.class)
class BoundTransactionOptionsTest {

    private static final int WTIMEOUT_MS = 12_345;
    private static final TransactionOptions MAJORITY_READ_AND_WRITE_WITH_WTIMEOUT_AND_JOURNAL = TransactionOptions.builder()
            .readConcern(ReadConcern.MAJORITY).writeConcern(WriteConcern.MAJORITY.withWTimeout(WTIMEOUT_MS, MILLISECONDS).withJournal(true)).build();
    private static final TransactionOptions SNAPSHOT_MAJORITY = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY).build();

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
    void restarts_a_transaction_its_own_template_just_began_at_read_concern_snapshot_and_write_concern_majority() {
        TransactionOptions options = new TransactionTemplate(transactionManager).execute(status -> {
            boundOptions.restartAtOnePointInTime(status);
            return boundOptions.current();
        });

        assertThat(options).isNotNull();
        assertThat(options.getReadConcern().getLevel()).isEqualTo(ReadConcernLevel.SNAPSHOT);
        assertThat(options.getWriteConcern().getWObject()).isEqualTo("majority");
        assertThat(Scope.of(Deployment.MONGOS, options)).isEqualTo(Scope.SNAPSHOT_THROUGH_MONGOS);
    }

    @Test
    void keeps_the_wtimeout_and_the_journal_flag_of_the_transaction_it_restarts() {
        TransactionTemplate template = new TransactionTemplate(new MongoTransactionManager(databaseFactory, MAJORITY_READ_AND_WRITE_WITH_WTIMEOUT_AND_JOURNAL));

        TransactionOptions options = template.execute(status -> {
            boundOptions.restartAtOnePointInTime(status);
            return boundOptions.current();
        });

        assertThat(options).isNotNull();
        assertThat(options.getReadConcern().getLevel()).isEqualTo(ReadConcernLevel.SNAPSHOT);
        assertThat(options.getWriteConcern().getWObject()).isEqualTo("majority");
        assertThat(options.getWriteConcern().getWTimeout(MILLISECONDS)).isEqualTo(WTIMEOUT_MS);
        assertThat(options.getWriteConcern().getJournal()).isTrue();
    }

    @Test
    void leaves_a_transaction_that_already_runs_at_read_concern_snapshot_and_write_concern_majority_as_it_is() {
        TransactionTemplate template = new TransactionTemplate(new MongoTransactionManager(databaseFactory, SNAPSHOT_MAJORITY));

        Observed[] observed = new Observed[2];
        template.executeWithoutResult(status -> {
            observed[0] = observe();
            boundOptions.restartAtOnePointInTime(status);
            observed[1] = observe();
        });

        assertThat(observed[1].transactionNumber()).as("the same transaction").isEqualTo(observed[0].transactionNumber());
        assertThat(observed[1].options().getReadConcern()).isEqualTo(ReadConcern.SNAPSHOT);
        assertThat(observed[1].options().getWriteConcern()).isEqualTo(WriteConcern.MAJORITY);
    }

    @Test
    void doesnt_touch_a_callers_transaction_that_the_template_joins() {
        TransactionTemplate template = new TransactionTemplate(transactionManager);

        Observed[] observed = new Observed[2];
        template.executeWithoutResult(outer -> {
            observed[0] = observe();
            template.executeWithoutResult(joined -> boundOptions.restartAtOnePointInTime(joined));
            observed[1] = observe();
        });

        assertThat(observed[1].transactionNumber()).as("the callers transaction").isEqualTo(observed[0].transactionNumber());
        assertThat(observed[1].options().getReadConcern()).isEqualTo(observed[0].options().getReadConcern());
        assertThat(observed[1].options().getWriteConcern()).isEqualTo(observed[0].options().getWriteConcern());
        assertThat(Scope.of(Deployment.MONGOS, observed[1].options())).isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void doesnt_touch_the_bound_session_when_the_new_transaction_belongs_to_another_database_factory() {
        TransactionTemplate outerTemplate = new TransactionTemplate(transactionManager);
        SimpleMongoClientDatabaseFactory otherFactory = new SimpleMongoClientDatabaseFactory(client, databaseFactory.getMongoDatabase().getName());
        TransactionTemplate otherTemplate = new TransactionTemplate(new MongoTransactionManager(otherFactory));

        Observed[] observed = new Observed[2];
        outerTemplate.executeWithoutResult(outer -> {
            observed[0] = observe();
            otherTemplate.executeWithoutResult(other -> {
                assertThat(other.isNewTransaction()).as("the other manager began its own transaction").isTrue();
                boundOptions.restartAtOnePointInTime(other);
            });
            observed[1] = observe();
        });

        assertThat(observed[1].transactionNumber()).as("the outer transaction on the bound session").isEqualTo(observed[0].transactionNumber());
        assertThat(observed[1].options().getReadConcern()).isEqualTo(observed[0].options().getReadConcern());
        assertThat(observed[1].options().getWriteConcern()).isEqualTo(observed[0].options().getWriteConcern());
        assertThat(Scope.of(Deployment.MONGOS, observed[1].options())).isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void the_options_are_null_when_read_through_the_factory_the_transaction_is_not_bound_to() {
        BoundTransactionOptions other = new BoundTransactionOptions(new SimpleMongoClientDatabaseFactory(client, databaseFactory.getMongoDatabase().getName()));

        TransactionOptions options = new TransactionTemplate(transactionManager).execute(status -> other.current());

        assertThat(options).isNull();
    }

    private static Observed observe() {
        ClientSession session = new SessionOfTheBoundTransaction(databaseFactory).session();
        return new Observed(session.getTransactionOptions(), session.getServerSession().getTransactionNumber());
    }

    private static final class SessionOfTheBoundTransaction extends MongoTransactionManager {

        private SessionOfTheBoundTransaction(SimpleMongoClientDatabaseFactory databaseFactory) {
            super(databaseFactory);
        }

        private ClientSession session() {
            return ((MongoTransactionObject) doGetTransaction()).getSession();
        }
    }

    private record Observed(TransactionOptions options, long transactionNumber) {
    }
}
