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

package org.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.cloudevents.CloudEvent;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.blocking.internal.CatchupReadScope;
import org.occurrent.eventstore.mongodb.internal.OperationTimeLiveStart;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.filter.Filter;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testing.mongodb.OccurrentMongoFlush;
import org.occurrent.testsupport.mongodb.MongoTestDatabase;
import org.occurrent.testsupport.mongodb.RecordingCommandListener;
import org.occurrent.testsupport.mongodb.RecordingCommandListener.Command;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.eventstore.api.EventStoreCapability.STREAM;
import static org.occurrent.tck.ConformanceEvents.event;
import static org.occurrent.tck.ConformanceEvents.idsOf;

@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
class MongoEventStoreCatchupReadScopeTest {

    private static final String COLLECTION = "events";
    private static final String POSITIONS = DcbMarkerModel.positionCollectionName(COLLECTION);

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    @RegisterExtension
    OccurrentMongoFlush flushMongoDBExtension = OccurrentMongoFlush.everyCollectionIn(MongoTestDatabase.of(mongoDBContainer));

    private final RecordingCommandListener commands = new RecordingCommandListener();
    private MongoClient mongoClient;
    private MongoEventStore eventStore;

    @AfterEach
    void close_mongo_client() {
        mongoClient.close();
    }

    @Test
    void the_live_start_is_one_increment_after_the_operation_time_the_server_reported_for_the_read_of_the_position_counter() {
        openStore(null);
        eventStore.write("stream", List.of(event("first", "Happened")));
        commands.clear();

        try (CatchupReadScope scope = eventStore.openCatchupReadScope(null)) {
            BsonDocument counterRead = onlyReadOf(POSITIONS).reply();

            BsonTimestamp serverOperationTime = counterRead.getTimestamp("operationTime");
            assertThat(scope.liveStart()).isEqualTo(OperationTimeLiveStart.formatOneIncrementAfter(serverOperationTime));
            assertThat(OperationTimeLiveStart.parse(scope.liveStart()).getInc()).isEqualTo(serverOperationTime.getInc() + 1);
        }
    }

    @Test
    void every_read_in_a_scope_is_a_majority_read_in_the_one_causally_consistent_session_of_the_scope() {
        openStore(null);
        eventStore.write("stream", List.of(event("first", "Happened"), event("second", "Happened")));
        commands.clear();

        try (CatchupReadScope scope = eventStore.openCatchupReadScope(null)) {
            List<String> replayed = idsInPositionOrder(scope);

            assertThat(replayed).containsExactly("first", "second");
            List<Command> reads = scopeReads();
            assertThat(reads).extracting(Command::collection).contains(POSITIONS, COLLECTION);
            assertThat(reads).as("read concern of %s", reads).allSatisfy(read -> assertThat(read.readConcernLevel()).isEqualTo("majority"));
            assertThat(reads.stream().map(Command::sessionId).distinct()).as("sessions used by %s", reads).hasSize(1).doesNotContainNull();
            assertThat(reads.stream().skip(1)).as("reads after the first, which wait for the cluster time of the one before: %s", reads)
                    .allSatisfy(read -> assertThat(read.waitsForClusterTime()).isTrue());
        }
    }

    @Test
    void read_concern_stays_majority_when_the_client_is_configured_with_read_concern_local() {
        openStore(ReadConcern.LOCAL);
        eventStore.write("stream", List.of(event("first", "Happened")));
        commands.clear();
        eventStore.read("stream").events().toList();
        assertThat(commands.commands("find", COLLECTION)).as("reads the store makes itself use the configured read concern")
                .extracting(Command::readConcernLevel).containsOnly("local");
        commands.clear();

        try (CatchupReadScope scope = eventStore.openCatchupReadScope(null)) {
            idsInPositionOrder(scope);

            assertThat(scopeReads()).extracting(Command::readConcernLevel).containsOnly("majority");
        }
    }

    @Test
    void a_callers_open_transaction_is_not_used_by_the_scope() {
        openStore(null);
        try (ClientSession callersSession = mongoClient.startSession()) {
            callersSession.startTransaction();
            ClientSessionHolder.set(callersSession);
            try {
                eventStore.write("stream", List.of(event("uncommitted", "Happened")));
                assertThat(idsOf(eventStore.read("stream").events())).as("the transaction sees its own write").containsExactly("uncommitted");
                commands.clear();

                try (CatchupReadScope scope = eventStore.openCatchupReadScope(null)) {
                    List<String> replayed = idsInPositionOrder(scope);

                    assertThat(replayed).as("events the scope replays while the caller's transaction is open").isEmpty();
                    List<Command> reads = scopeReads();
                    assertThat(reads).isNotEmpty().noneSatisfy(read -> assertThat(read.inTransaction()).as("%s", read).isTrue());
                    assertThat(reads.stream().map(Command::sessionId)).as("sessions used by %s", reads)
                            .doesNotContain(callersSession.getServerSession().getIdentifier());
                }
            } finally {
                ClientSessionHolder.remove();
                if (callersSession.hasActiveTransaction()) {
                    callersSession.abortTransaction();
                }
            }
        }
    }

    private void openStore(ReadConcern clientReadConcern) {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl() + ".scope");
        MongoClientSettings.Builder settings = MongoClientSettings.builder().applyConnectionString(connectionString).addCommandListener(commands);
        if (clientReadConcern != null) {
            settings.readConcern(clientReadConcern);
        }
        mongoClient = MongoClients.create(settings.build());
        EventStoreConfig config = new EventStoreConfig.Builder()
                .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                .eventStoreCapabilities(STREAM)
                .build();
        eventStore = new MongoEventStore(mongoClient, connectionString.getDatabase(), COLLECTION, config);
    }

    private List<String> idsInPositionOrder(CatchupReadScope scope) {
        try (Stream<CloudEvent> events = scope.readInPositionOrder(Filter.all(), PositionRange.upToPosition(scope.currentPosition()))) {
            return idsOf(events);
        }
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
