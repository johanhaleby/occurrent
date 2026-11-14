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

package org.occurrent.eventstore.mongodb.spring.reactor;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Deployment;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbAppendConditionCheck.Scope;
import org.occurrent.testsupport.mongodb.TwoShardMongoCluster;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins what the append check relies on in Spring Data 5.1: that {@code ReactiveMongoTransactionManager.doGetTransaction}
 * reaches the session bound to the template's factory in the subscriber context, and that the store can abort the
 * transaction its own operator just began on that session and start it again at read concern snapshot and write concern
 * majority (ADR 175). Runs through a mongos, where the options decide the scope.
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
    private static SimpleReactiveMongoDatabaseFactory databaseFactory;
    private static ReactiveMongoTemplate template;
    private static ReactiveMongoTransactionManager transactionManager;
    private static BoundTransactionOptions boundOptions;

    @BeforeAll
    static void start_the_cluster_and_wire_one_template_and_one_transaction_manager_on_one_factory() {
        cluster = TwoShardMongoCluster.start();
        client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(cluster.getConnectionString())).build());
        databaseFactory = new SimpleReactiveMongoDatabaseFactory(client, cluster.createDatabase(TwoShardMongoCluster.SECOND_SHARD));
        template = new ReactiveMongoTemplate(databaseFactory);
        transactionManager = new ReactiveMongoTransactionManager(databaseFactory);
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
    void the_options_are_empty_outside_a_transaction_and_no_error_is_signalled() {
        StepVerifier.create(boundOptions.current())
                .expectNext(Optional.empty())
                .verifyComplete();
    }

    @Test
    void the_options_of_a_plain_transactional_operator_have_neither_read_concern_snapshot_nor_write_concern_majority() {
        Optional<TransactionOptions> current = TransactionalOperator.create(transactionManager).transactional(boundOptions.current()).block();

        assertThat(current).as("the session is bound and its transaction started without a write").isPresent();
        TransactionOptions options = current.get();
        assertThat(options.getReadConcern() == null || options.getReadConcern().getLevel() == null).as("read concern %s", options.getReadConcern()).isTrue();
        assertThat(options.getWriteConcern() == null || !"majority".equals(options.getWriteConcern().getWObject())).as("write concern %s", options.getWriteConcern()).isTrue();
        assertThat(Scope.of(Deployment.MONGOS, options)).isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void restarts_a_transaction_its_own_operator_just_began_at_read_concern_snapshot_and_write_concern_majority() {
        Optional<TransactionOptions> current = TransactionalOperator.create(transactionManager)
                .execute(status -> boundOptions.restartAtOnePointInTime(status).then(boundOptions.current()))
                .single().block();

        assertThat(current).isPresent();
        assertThat(current.get().getReadConcern().getLevel()).isEqualTo(ReadConcernLevel.SNAPSHOT);
        assertThat(current.get().getWriteConcern().getWObject()).isEqualTo("majority");
        assertThat(Scope.of(Deployment.MONGOS, current.get())).isEqualTo(Scope.SNAPSHOT_THROUGH_MONGOS);
    }

    @Test
    void keeps_the_wtimeout_and_the_journal_flag_of_the_transaction_it_restarts() {
        TransactionalOperator operator = TransactionalOperator.create(new ReactiveMongoTransactionManager(databaseFactory, MAJORITY_READ_AND_WRITE_WITH_WTIMEOUT_AND_JOURNAL));

        Optional<TransactionOptions> current = operator.execute(status -> boundOptions.restartAtOnePointInTime(status).then(boundOptions.current())).single().block();

        assertThat(current).isPresent();
        TransactionOptions options = current.get();
        assertThat(options.getReadConcern().getLevel()).isEqualTo(ReadConcernLevel.SNAPSHOT);
        assertThat(options.getWriteConcern().getWObject()).isEqualTo("majority");
        assertThat(options.getWriteConcern().getWTimeout(MILLISECONDS)).isEqualTo(WTIMEOUT_MS);
        assertThat(options.getWriteConcern().getJournal()).isTrue();
    }

    @Test
    void leaves_a_transaction_that_already_runs_at_read_concern_snapshot_and_write_concern_majority_as_it_is() {
        TransactionalOperator operator = TransactionalOperator.create(new ReactiveMongoTransactionManager(databaseFactory, SNAPSHOT_MAJORITY));

        List<Observed> observed = operator.execute(status -> observe().flatMap(before -> boundOptions.restartAtOnePointInTime(status).then(observe()).map(after -> List.of(before, after)))).single().block();

        assertThat(observed.get(1).transactionNumber()).as("the same transaction").isEqualTo(observed.get(0).transactionNumber());
        assertThat(observed.get(1).options().getReadConcern()).isEqualTo(ReadConcern.SNAPSHOT);
        assertThat(observed.get(1).options().getWriteConcern()).isEqualTo(WriteConcern.MAJORITY);
    }

    @Test
    void doesnt_touch_a_callers_transaction_that_the_operator_joins() {
        TransactionalOperator operator = TransactionalOperator.create(transactionManager);

        List<Observed> observed = operator.execute(outer -> observe().flatMap(before ->
                operator.execute(joined -> boundOptions.restartAtOnePointInTime(joined)).then(observe()).map(after -> List.of(before, after)))).single().block();

        assertThat(observed.get(1).transactionNumber()).as("the callers transaction").isEqualTo(observed.get(0).transactionNumber());
        assertThat(observed.get(1).options().getReadConcern()).isEqualTo(observed.get(0).options().getReadConcern());
        assertThat(observed.get(1).options().getWriteConcern()).isEqualTo(observed.get(0).options().getWriteConcern());
        assertThat(Scope.of(Deployment.MONGOS, observed.get(1).options())).isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void doesnt_touch_the_bound_session_when_the_new_transaction_belongs_to_another_database_factory() {
        TransactionalOperator outerOperator = TransactionalOperator.create(transactionManager);
        SimpleReactiveMongoDatabaseFactory otherFactory = new SimpleReactiveMongoDatabaseFactory(client, databaseFactory.getMongoDatabase().block().getName());
        TransactionalOperator otherOperator = TransactionalOperator.create(new ReactiveMongoTransactionManager(otherFactory));

        List<Observed> observed = outerOperator.execute(outer -> observe().flatMap(before ->
                otherOperator.execute(other -> {
                    assertThat(other.isNewTransaction()).as("the other manager began its own transaction").isTrue();
                    return boundOptions.restartAtOnePointInTime(other);
                }).then(observe()).map(after -> List.of(before, after)))).single().block();

        assertThat(observed.get(1).transactionNumber()).as("the outer transaction on the bound session").isEqualTo(observed.get(0).transactionNumber());
        assertThat(observed.get(1).options().getReadConcern()).isEqualTo(observed.get(0).options().getReadConcern());
        assertThat(observed.get(1).options().getWriteConcern()).isEqualTo(observed.get(0).options().getWriteConcern());
        assertThat(Scope.of(Deployment.MONGOS, observed.get(1).options())).isEqualTo(Scope.MARKERS_ONLY);
    }

    @Test
    void the_options_are_empty_when_read_through_the_factory_the_transaction_is_not_bound_to() {
        BoundTransactionOptions other = new BoundTransactionOptions(new SimpleReactiveMongoDatabaseFactory(client, databaseFactory.getMongoDatabase().block().getName()));

        Optional<TransactionOptions> current = TransactionalOperator.create(transactionManager).transactional(other.current()).block();

        assertThat(current).isEmpty();
    }

    private static Mono<Observed> observe() {
        return TransactionSynchronizationManager.forCurrentTransaction().map(synchronization -> {
            ClientSession session = new SessionOfTheBoundTransaction(databaseFactory).session(synchronization);
            return new Observed(session.getTransactionOptions(), session.getServerSession().getTransactionNumber());
        });
    }

    private static final class SessionOfTheBoundTransaction extends ReactiveMongoTransactionManager {

        private SessionOfTheBoundTransaction(SimpleReactiveMongoDatabaseFactory databaseFactory) {
            super(databaseFactory);
        }

        private ClientSession session(TransactionSynchronizationManager synchronization) {
            return ((ReactiveMongoTransactionObject) doGetTransaction(synchronization)).getSession();
        }
    }

    private record Observed(TransactionOptions options, long transactionNumber) {
    }
}
