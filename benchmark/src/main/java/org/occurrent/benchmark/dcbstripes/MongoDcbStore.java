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

package org.occurrent.benchmark.dcbstripes;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.occurrent.eventstore.api.dcb.DcbAppendConditionNotFulfilledException;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbReadOptions;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.nativedriver.EventStoreConfig;
import org.occurrent.eventstore.mongodb.nativedriver.MongoEventStore;
import org.occurrent.mongodb.timerepresentation.TimeRepresentation;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.occurrent.eventstore.api.EventStoreCapability.DCB;
import static org.occurrent.eventstore.api.dcb.DcbAppendCondition.failIfEventsMatch;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.types;

/**
 * A native {@link MongoEventStore} on a single-node replica set of its own, and the appends the DCB stripe benchmarks
 * time. Uses only API that exists before the type marker stripes, so {@link DcbTypeMarkerCostBenchmark} compiles on
 * that layer unchanged.
 */
final class MongoDcbStore implements AutoCloseable {

    static final String TYPE = "TicketSold";

    private static final URI SOURCE = URI.create("urn:occurrent:benchmark");
    // A token needs a read, and a read of every event of the type would grow with the run, so read the newest one only.
    // The token holds the criteria's markers whatever the limit.
    private static final DcbReadOptions TOKEN_ONLY = DcbReadOptions.fromBeginning().backwards().limit(1);

    // The sync driver fires command events on the thread that runs the operation, so a conflict is counted against the
    // benchmark thread whose append hit it. close() prints CONFLICTS_SEEN and CONFLICTS_TAKEN, so a run shows whether they agree.
    private static final ThreadLocal<long[]> CONFLICTS = ThreadLocal.withInitial(() -> new long[1]);
    private static final AtomicLong CONFLICTS_SEEN = new AtomicLong();
    private static final AtomicLong CONFLICTS_TAKEN = new AtomicLong();

    private final ReplicaSetReadyMongoDBContainer mongo;
    private final MongoClient mongoClient;
    private final MongoEventStore eventStore;

    private MongoDcbStore(ReplicaSetReadyMongoDBContainer mongo, MongoClient mongoClient, MongoEventStore eventStore) {
        this.mongo = mongo;
        this.mongoClient = mongoClient;
        this.eventStore = eventStore;
    }

    static MongoDcbStore start(UnaryOperator<EventStoreConfig.Builder> settings) {
        ReplicaSetReadyMongoDBContainer mongo = ReplicaSetReadyMongoDBContainer.withDefaultVersion();
        mongo.start();
        ConnectionString connectionString = new ConnectionString(mongo.getReplicaSetUrl("dcb"));
        MongoClient mongoClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .addCommandListener(new ConflictCounter())
                .build());
        EventStoreConfig config = settings.apply(new EventStoreConfig.Builder()
                        .timeRepresentation(TimeRepresentation.RFC_3339_STRING)
                        .eventStoreCapabilities(DCB))
                .build();
        MongoEventStore eventStore = new MongoEventStore(mongoClient, requireNonNull(connectionString.getDatabase()), "events", config);
        return new MongoDcbStore(mongo, mongoClient, eventStore);
    }

    /**
     * Appends an event of {@code type} with a tag nothing else has, on the condition that no event has the tag.
     */
    void taggedConditionalAppend(String type) {
        Tag tag = newTag();
        eventStore.append(List.of(event(type, tag)), failIfEventsMatch(tags(tag)));
    }

    /**
     * Reads a token for {@code types(type)} and appends an event of {@code type} on the condition that no event of the
     * type was appended after that read.
     *
     * @return whether the condition held
     */
    boolean typeOnlyConditionalAppend(String type) {
        DcbConsistencyToken token = eventStore.read(types(type), TOKEN_ONLY).consistencyToken();
        try {
            eventStore.append(List.of(event(type, newTag())), failIfEventsMatch(token));
            return true;
        } catch (DcbAppendConditionNotFulfilledException e) {
            return false;
        }
    }

    void unconditionalAppend(String type) {
        eventStore.append(List.of(event(type, newTag())));
    }

    List<CloudEvent> readType(String type) {
        return eventStore.read(types(type)).events();
    }

    /**
     * @return the transient transaction errors this thread's commands failed with since the last call. Each one is an
     * attempt the driver or the store ran again, in practice a write conflict on a marker document.
     */
    static long takeConflicts() {
        long[] conflicts = CONFLICTS.get();
        long taken = conflicts[0];
        conflicts[0] = 0;
        CONFLICTS_TAKEN.addAndGet(taken);
        return taken;
    }

    @Override
    public void close() {
        System.out.println("\nTransient transaction errors seen: " + CONFLICTS_SEEN.get() + ", counted against a benchmark thread: " + CONFLICTS_TAKEN.get());
        mongoClient.close();
        mongo.stop();
    }

    private static Tag newTag() {
        return Tag.of("order", UUID.randomUUID().toString());
    }

    private static CloudEvent event(String type, Tag tag) {
        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(SOURCE)
                .withType(type)
                .withTime(OffsetDateTime.now())
                .withData("{}".getBytes(UTF_8))
                .build();
        return DcbCloudEvents.withTags(event, List.of(tag));
    }

    private static final class ConflictCounter implements CommandListener {
        @Override
        public void commandFailed(CommandFailedEvent event) {
            // 112 is WriteConflict, in case the label isn't on the exception yet when the event fires
            if (event.getThrowable() instanceof MongoException e && (e.getCode() == 112 || e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL))) {
                CONFLICTS.get()[0]++;
                CONFLICTS_SEEN.incrementAndGet();
            }
        }
    }
}
