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

package org.occurrent.eventstore.mongodb.migration.positionbackfill;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.eventstore.mongodb.dcb.internal.PositionDocumentMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static java.util.Objects.requireNonNull;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.POSITION;

/**
 * Retrofits the global {@code position} extension (see {@link org.occurrent.cloudevents.OccurrentCloudEventExtension#POSITION})
 * onto events written before {@code position} existed for a MongoDB event store collection, so an existing
 * deployment can turn stream position on safely. See {@code doc/runbooks/position-backfill.md} for the full
 * upgrade sequence this tool is one step of.
 * <p>
 * Reuses {@link DcbMarkerModel} for the counter document contract and {@link PositionDocumentMapper} for writing the
 * position field, so the backfilled schema is identical to what a live store writes and cannot drift from it.
 * <p>
 * A single {@link #run()} call processes events in {@code _id} order (the collection's primary key, always
 * indexed, so no additional index or sort on a time field is needed) in batches, is safe to interrupt and resume
 * (a checkpoint document records the last processed {@code _id}), is idempotent (only events missing
 * {@code position} are touched, so a completed or partially completed run can always be re-run safely), and can be
 * throttled between batches via {@link PositionBackfillOptions#throttleMillis()} to avoid competing with production
 * traffic.
 * <p>
 * Typical usage for a one-off migration run:
 * <pre>{@code
 * MongoDatabase database = mongoClient.getDatabase("my-database");
 * PositionBackfill backfill = new PositionBackfill(database, "events", PositionBackfillOptions.defaults());
 * PositionBackfillResult result = backfill.run();
 * }</pre>
 */
@NullMarked
public final class PositionBackfill {

    private static final Logger log = LoggerFactory.getLogger(PositionBackfill.class);

    private static final String ID = "_id";

    private final MongoDatabase database;
    private final String eventStoreCollectionName;
    private final PositionBackfillOptions options;

    private final MongoCollection<Document> eventCollection;
    private final MongoCollection<Document> positionCollection;
    private final MongoCollection<Document> checkpointCollection;

    public PositionBackfill(MongoDatabase database, String eventStoreCollectionName, PositionBackfillOptions options) {
        this.database = requireNonNull(database, "database cannot be null");
        this.eventStoreCollectionName = requireNonNull(eventStoreCollectionName, "eventStoreCollectionName cannot be null");
        this.options = requireNonNull(options, "options cannot be null");
        this.eventCollection = database.getCollection(eventStoreCollectionName);
        this.positionCollection = database.getCollection(DcbMarkerModel.positionCollectionName(eventStoreCollectionName));
        this.checkpointCollection = database.getCollection(checkpointCollectionName(eventStoreCollectionName));
    }

    /**
     * Runs the backfill to completion: seeds the counter (step 1, a no-op if already seeded above the current
     * historical max), then backfills every un-positioned event in {@code _id} order (steps 2-4), resuming from any
     * prior checkpoint. Blocks the calling thread until every un-positioned event has been processed.
     *
     * @return a summary of the work done by this call.
     */
    public PositionBackfillResult run() {
        long seededTo = seedCounter();
        long positioned = 0;
        while (true) {
            long batchPositioned = backfillBatch();
            positioned += batchPositioned;
            if (batchPositioned == 0) {
                break;
            }
            if (options.throttleMillis() > 0) {
                sleep(options.throttleMillis());
            }
        }
        markCompleted();
        log.info("Position backfill for collection '{}' finished: {} events positioned this run, counter seeded to {}.",
                eventStoreCollectionName, positioned, seededTo);
        return new PositionBackfillResult(positioned, seededTo, true);
    }

    /**
     * Seeds the position counter document above the current historical event count, using an accurate
     * {@link MongoCollection#countDocuments()} (not the estimated, metadata-based count, which can under-count and
     * would let live writes after deploy collide with positions this backfill has not assigned yet) plus
     * {@link PositionBackfillOptions#counterSeedSlack()}. A no-op if the counter is already seeded at or above that
     * value, so this step is itself idempotent and safe to call before every {@link #run()}.
     *
     * @return the counter value after this call (either newly seeded or the pre-existing, already-sufficient value).
     */
    public long seedCounter() {
        long historicalCount = eventCollection.countDocuments();
        long seedTarget = historicalCount + options.counterSeedSlack();

        Document current = positionCollection.find(eq(ID, DcbMarkerModel.POSITION_DOCUMENT_ID)).first();
        long currentValue = current == null ? 0 : ((Number) current.get(DcbMarkerModel.COUNTER_POSITION)).longValue();
        if (currentValue >= seedTarget) {
            log.info("Position counter for collection '{}' already at {}, at or above the seed target {}; leaving it untouched.",
                    eventStoreCollectionName, currentValue, seedTarget);
            return currentValue;
        }

        Document updated = positionCollection.findOneAndUpdate(
                eq(ID, DcbMarkerModel.POSITION_DOCUMENT_ID),
                Updates.max(DcbMarkerModel.COUNTER_POSITION, seedTarget),
                new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)
        );
        long seededTo = ((Number) requireNonNull(updated).get(DcbMarkerModel.COUNTER_POSITION)).longValue();
        log.info("Seeded position counter for collection '{}' to {} (historical count {} + slack {}).",
                eventStoreCollectionName, seededTo, historicalCount, options.counterSeedSlack());
        return seededTo;
    }

    /**
     * Backfills a single batch of up to {@link PositionBackfillOptions#batchSize()} un-positioned events, ordered by
     * {@code _id} ascending, resuming after the last checkpointed {@code _id} if one exists. Reserves one contiguous
     * block of positions per batch (via the same counter document {@link #seedCounter()} seeds) so the assigned
     * positions are strictly increasing across batches and across resumed runs.
     *
     * @return the number of events positioned by this batch; {@code 0} means there was nothing left to do.
     */
    public long backfillBatch() {
        Bson filter = and(exists(POSITION, false), afterCheckpointFilter());
        List<Document> batch = eventCollection.find(filter)
                .sort(Sorts.ascending(ID))
                .limit(options.batchSize())
                .into(new ArrayList<>());

        if (batch.isEmpty()) {
            return 0;
        }

        long firstPosition = reservePositions(batch.size());
        long position = firstPosition;
        Object lastId = null;
        for (Document event : batch) {
            Document positionHolder = new Document();
            PositionDocumentMapper.addPosition(positionHolder, position);
            eventCollection.updateOne(eq(ID, event.get(ID)), Updates.set(POSITION, positionHolder.get(POSITION)));
            lastId = event.get(ID);
            position++;
        }
        checkpoint(lastId, batch.size());
        log.info("Positioned {} events in collection '{}' (positions {}-{}).", batch.size(), eventStoreCollectionName, firstPosition, position - 1);
        return batch.size();
    }

    private Bson afterCheckpointFilter() {
        Document checkpoint = checkpointCollection.find(eq(ID, PositionBackfillCheckpoint.CHECKPOINT_DOCUMENT_ID)).first();
        if (checkpoint == null) {
            return new Document();
        }
        Object lastProcessedId = checkpoint.get(PositionBackfillCheckpoint.FIELD_LAST_PROCESSED_ID);
        if (lastProcessedId == null) {
            return new Document();
        }
        return gt(ID, lastProcessedId);
    }

    private void checkpoint(Object lastProcessedId, int batchSize) {
        checkpointCollection.findOneAndUpdate(
                eq(ID, PositionBackfillCheckpoint.CHECKPOINT_DOCUMENT_ID),
                Updates.combine(
                        Updates.set(PositionBackfillCheckpoint.FIELD_LAST_PROCESSED_ID, lastProcessedId),
                        Updates.inc(PositionBackfillCheckpoint.FIELD_PROCESSED_COUNT, batchSize),
                        Updates.set(PositionBackfillCheckpoint.FIELD_COMPLETED, false)
                ),
                new FindOneAndUpdateOptions().upsert(true)
        );
    }

    private void markCompleted() {
        checkpointCollection.findOneAndUpdate(
                eq(ID, PositionBackfillCheckpoint.CHECKPOINT_DOCUMENT_ID),
                Updates.set(PositionBackfillCheckpoint.FIELD_COMPLETED, true),
                new FindOneAndUpdateOptions().upsert(true)
        );
    }

    /**
     * Reserves a contiguous block of {@code eventCount} positions from the shared counter document and returns the
     * first position in that block, exactly mirroring the reservation contract the live stores use (see
     * {@code SpringMongoEventStore.reservePositions}) so backfilled and live-written positions share one sequence
     * with no overlap.
     */
    private long reservePositions(int eventCount) {
        Document updated = positionCollection.findOneAndUpdate(
                eq(ID, DcbMarkerModel.POSITION_DOCUMENT_ID),
                Updates.inc(DcbMarkerModel.COUNTER_POSITION, eventCount),
                new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)
        );
        long lastPosition = ((Number) requireNonNull(updated, "Position document cannot be null").get(DcbMarkerModel.COUNTER_POSITION)).longValue();
        return lastPosition - eventCount + 1;
    }

    private static String checkpointCollectionName(String eventStoreCollectionName) {
        return eventStoreCollectionName + "_position_backfill_checkpoint";
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Position backfill was interrupted while throttling between batches", e);
        }
    }

    /**
     * Simple command-line entry point: {@code java -jar position-backfill.jar <mongoUri> <database> <collection>}.
     * Uses {@link PositionBackfillOptions#defaults()}; use the {@link PositionBackfill} constructor directly for
     * programmatic control over batching and throttling.
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: java -jar position-backfill.jar <mongoUri> <database> <collection>");
            System.exit(1);
            return;
        }
        String mongoUri = args[0];
        String databaseName = args[1];
        String collectionName = args[2];

        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            PositionBackfill backfill = new PositionBackfill(database, collectionName, PositionBackfillOptions.defaults());
            PositionBackfillResult result = backfill.run();
            log.info("Backfill result: {}", result);
        }
    }
}
