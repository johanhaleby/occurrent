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
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.internal.CatchupReadScope;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.eventstore.mongodb.internal.OperationTimeLiveStart;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.RecordingCommandListener;
import org.occurrent.testsupport.mongodb.RecordingCommandListener.Command;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.occurrent.tck.ConformanceEvents.event;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactorMongoEventStoreCatchupReadScopeTest {

    private static final String COLLECTION = "events";
    private static final String POSITIONS = DcbMarkerModel.positionCollectionName(COLLECTION);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final RecordingCommandListener commands = new RecordingCommandListener();
    private MongoClient mongoClient;
    private ReactiveMongoTransactionManager transactionManager;
    private ReactorMongoEventStore eventStore;

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void the_live_start_is_one_increment_after_the_operation_time_the_server_reported_for_the_read_of_the_position_counter() {
        openStore(query -> query);
        eventStore.write("stream", event("first", "Happened")).block(TIMEOUT);
        commands.clear();

        CatchupReadScope scope = eventStore.openCatchupReadScope(null).block(TIMEOUT);
        try {
            BsonDocument counterRead = onlyReadOf(POSITIONS).reply();

            BsonTimestamp serverOperationTime = counterRead.getTimestamp("operationTime");
            assertThat(scope.liveStart()).isEqualTo(OperationTimeLiveStart.formatOneIncrementAfter(serverOperationTime));
            assertThat(OperationTimeLiveStart.parse(scope.liveStart()).getInc()).isEqualTo(serverOperationTime.getInc() + 1);
        } finally {
            scope.close().block(TIMEOUT);
        }
    }

    @Test
    void every_read_in_a_scope_is_a_majority_read_in_the_one_causally_consistent_session_of_the_scope() {
        openStore(query -> query);
        eventStore.write("stream", Flux.just(event("first", "Happened"), event("second", "Happened"))).block(TIMEOUT);
        commands.clear();

        CatchupReadScope scope = eventStore.openCatchupReadScope(null).block(TIMEOUT);
        try {
            List<String> replayed = idsInPositionOrder(scope);

            assertThat(replayed).containsExactly("first", "second");
            List<Command> reads = scopeReads();
            assertThat(reads).extracting(Command::collection).contains(POSITIONS, COLLECTION);
            assertThat(reads).as("read concern of %s", reads).allSatisfy(read -> assertThat(read.readConcernLevel()).isEqualTo("majority"));
            assertThat(reads.stream().map(Command::sessionId).distinct()).as("sessions used by %s", reads).hasSize(1).doesNotContainNull();
            assertThat(reads.stream().skip(1)).as("reads after the first, which wait for the cluster time of the one before: %s", reads)
                    .allSatisfy(read -> assertThat(read.waitsForClusterTime()).isTrue());
        } finally {
            scope.close().block(TIMEOUT);
        }
    }

    @Test
    void read_concern_stays_majority_when_the_query_options_set_read_concern_local() {
        openStore(query -> query.withReadConcern(ReadConcern.LOCAL));
        eventStore.write("stream", event("first", "Happened")).block(TIMEOUT);
        commands.clear();
        eventStore.query(Filter.all()).collectList().block(TIMEOUT);
        assertThat(commands.commands("find", COLLECTION)).as("reads the store makes itself use the configured read concern")
                .extracting(Command::readConcernLevel).containsOnly("local");
        commands.clear();

        CatchupReadScope scope = eventStore.openCatchupReadScope(null).block(TIMEOUT);
        try {
            idsInPositionOrder(scope);

            assertThat(scopeReads()).extracting(Command::readConcernLevel).containsOnly("majority");
        } finally {
            scope.close().block(TIMEOUT);
        }
    }

    @Test
    void a_callers_open_transaction_is_not_used_by_the_scope() {
        openStore(query -> query);
        TransactionalOperator transaction = TransactionalOperator.create(transactionManager);

        transaction.execute(status -> {
            status.setRollbackOnly();
            return eventStore.write("stream", event("uncommitted", "Happened"))
                    .then(eventStore.read("stream").flatMapMany(stream -> stream.events()).map(CloudEvent::getId).collectList())
                    .doOnNext(ids -> assertThat(ids).as("the transaction sees its own write").containsExactly("uncommitted"))
                    .doOnNext(__ -> commands.clear())
                    .then(Mono.usingWhen(eventStore.openCatchupReadScope(null),
                            scope -> scope.currentPosition()
                                    .flatMapMany(position -> scope.readInPositionOrder(Filter.all(), PositionRange.upToPosition(position)))
                                    .map(CloudEvent::getId).collectList(),
                            CatchupReadScope::close))
                    .doOnNext(replayed -> {
                        assertThat(replayed).as("events the scope replays while the caller's transaction is open").isEmpty();
                        assertThat(scopeReads()).isNotEmpty().noneSatisfy(read -> assertThat(read.inTransaction()).as("%s", read).isTrue());
                    });
        }).blockLast(TIMEOUT);
    }

    @Test
    void a_scope_resumed_from_a_live_start_nothing_has_been_written_since_reads_every_event_before_it() {
        openStore(query -> query);
        eventStore.write("stream", Flux.just(event("first", "Happened"), event("second", "Happened"))).block(TIMEOUT);
        CatchupReadScope first = eventStore.openCatchupReadScope(null).block(TIMEOUT);
        String liveStart;
        try {
            liveStart = first.liveStart();
        } finally {
            first.close().block(TIMEOUT);
        }

        List<String> replayed = new ArrayList<>();
        assertThatCode(() -> {
            CatchupReadScope resumed = eventStore.openCatchupReadScope(liveStart).block(TIMEOUT);
            try {
                replayed.addAll(idsInPositionOrder(resumed));
            } finally {
                resumed.close().block(TIMEOUT);
            }
        }).doesNotThrowAnyException();
        assertThat(replayed).containsExactly("first", "second");
    }

    private void openStore(UnaryOperator<Query> queryOptions) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".scope");
        mongoClient = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(commands).build());
        String database = connectionString.getDatabase();
        ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(mongoClient, database);
        transactionManager = new ReactiveMongoTransactionManager(new SimpleReactiveMongoDatabaseFactory(mongoClient, database));
        EventStoreConfig config = new EventStoreConfig.Builder()
                .eventStoreCollectionName(COLLECTION)
                .transactionConfig(transactionManager)
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .queryOptions(queryOptions)
                .build();
        eventStore = new ReactorMongoEventStore(mongoTemplate, config);
    }

    private List<String> idsInPositionOrder(CatchupReadScope scope) {
        Long position = scope.currentPosition().block(TIMEOUT);
        return scope.readInPositionOrder(Filter.all(), PositionRange.upToPosition(position)).map(CloudEvent::getId).collectList().block(TIMEOUT);
    }

    private List<Command> scopeReads() {
        return commands.commands().stream()
                .filter(command -> command.name().equals("find"))
                .filter(command -> POSITIONS.equals(command.collection()) || COLLECTION.equals(command.collection()))
                .toList();
    }

    private Command onlyReadOf(String collection) {
        List<Command> reads = commands.commands("find", collection);
        assertThat(reads).as("finds of %s", collection).hasSize(1);
        return reads.getFirst();
    }
}
