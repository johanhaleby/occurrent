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


package org.occurrent.eventstore.mongodb.migration.updateeventrepair;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbDocumentMapper;
import org.occurrent.eventstore.mongodb.dcb.internal.PositionDocumentMapper;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.type;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.POSITION;
import static org.occurrent.retry.internal.RetryExecution.executeWithRetry;

/**
 * Repairs events that Occurrent's own {@code updateEvent} damaged before 0.34.0, so they become visible to DCB reads
 * and to position-ordered reads again.
 *
 * <h2>What went wrong</h2>
 * Up to and including 0.33.0, {@code updateEvent} rebuilt the stored document through the stream-only mapper, which
 * routes {@code position} through the general CloudEvent extension writer. That writer has no {@code Long} overload,
 * so {@code position} came back as a BSON string instead of an int64, and the indexed {@code dcbTags} array was
 * never written back at all. Both fields drop the event out of a query: MongoDB brackets comparison operators by
 * type, so a string {@code position} matches neither bound of a numeric range, and a DCB read additionally requires
 * {@code dcbTags} to exist. An event damaged this way is missing from DCB reads, from {@code exists} and
 * {@code count}, from position-ordered stream reads and from position-based catch-up, and it is missing from the
 * conflict query behind a conditional append, so an append that should have conflicted can be accepted. Nothing
 * reports any of it.
 *
 * <h2>What this tool restores, and what it cannot</h2>
 * It rebuilds {@code position} from the string the document still holds, and rebuilds the {@code dcbTags} array from
 * the {@code dcbtags} CloudEvent extension, which is a genuine string and so survived the coercion. It reuses the
 * store's own {@link PositionDocumentMapper} and {@link DcbCloudEvents#decodeTags(String)}, so a repaired event is
 * the same as one a running store writes.
 * <p>
 * It is not a general recovery. Where the old write-back destroyed the only copy of a value, that value is gone.
 * Each {@link UnrecoverableEvent.Reason} says which case it is.
 * <p>
 * A position it does restore is the value the document holds, not one it can check. The old write-back kept whatever
 * position the update function returned, so a function that forged one left that number behind as a string like any
 * other. Two of those the tool still catches: a value another event already holds is refused by the unique index, and
 * zero or a negative value is a position no store assigns. A positive value that happens to be free, in a gap in the
 * sequence for instance, is indistinguishable from the event's own. The tool converts it to an int64 and counts a
 * repair, because nothing in the store records what the position was.
 * <p>
 * Two kinds of damage cannot be seen at all, both from an update function that returned a replacement event built
 * from scratch. One drops the {@code dcbtags} extension, leaving a document that no longer looks like a DCB event
 * and that nothing distinguishes from an ordinary stream event. The other drops the {@code position} of a plain
 * stream event, which never had {@code dcbtags} to begin with, leaving a document that nothing distinguishes from
 * history written before position existed. Neither matches this tool's filter, so neither is counted or repaired,
 * and the second one reaches a store that writes position as an ordinary un-backfilled event. Backfilling it would
 * give it a position it never had, which is why every position-backfill startup message points here.
 *
 * <h2>Running it</h2>
 * {@link #report()} counts the damage and writes nothing. {@link #run()} repairs, walking the collection in
 * {@code _id} order in batches. A run is idempotent because it only touches events that still look damaged, and it
 * is safe to kill because each event is repaired on its own and a checkpoint document records how far it got.
 *
 * <pre>{@code
 * MongoDatabase database = mongoClient.getDatabase("my-database");
 * UpdateEventRepair repair = new UpdateEventRepair(database, "events", UpdateEventRepairOptions.defaults());
 * UpdateEventRepairReport report = repair.report();
 * if (report.eventsNeedingRepair() > 0) {
 *     UpdateEventRepairResult result = repair.run();
 * }
 * }</pre>
 */
@NullMarked
public final class UpdateEventRepair {

    private static final Logger log = LoggerFactory.getLogger(UpdateEventRepair.class);

    private static final String ID = "_id";

    private final String eventStoreCollectionName;
    private final UpdateEventRepairOptions options;
    private final RetryStrategy retryStrategy;

    private final MongoCollection<Document> eventCollection;
    private final MongoCollection<Document> checkpointCollection;

    /**
     * Retries every MongoDB operation with exponential backoff from 100 ms up to 2 seconds, so a transient outage
     * does not abandon a repair that may have hours of collection left to walk.
     */
    public UpdateEventRepair(MongoDatabase database, String eventStoreCollectionName, UpdateEventRepairOptions options) {
        this(database, eventStoreCollectionName, options, defaultRetryStrategy());
    }

    /**
     * @param retryStrategy How to retry a MongoDB operation that fails. A repair walks a whole collection, so a
     *                      strategy that gives up immediately turns a momentary outage into a run an operator has
     *                      to notice and restart.
     */
    public UpdateEventRepair(MongoDatabase database, String eventStoreCollectionName, UpdateEventRepairOptions options, RetryStrategy retryStrategy) {
        requireNonNull(database, "database cannot be null");
        this.retryStrategy = requireNonNull(retryStrategy, "retryStrategy cannot be null");
        this.eventStoreCollectionName = requireNonNull(eventStoreCollectionName, "eventStoreCollectionName cannot be null");
        this.options = requireNonNull(options, "options cannot be null");
        this.eventCollection = database.getCollection(eventStoreCollectionName);
        this.checkpointCollection = database.getCollection(checkpointCollectionName(eventStoreCollectionName));
    }

    /**
     * Counts the damage in the collection without changing anything, so the size of a repair is known before one is
     * started. It writes nothing, so it is safe to run against a live store, but it is not cheap. Finding an event
     * whose tag array is missing cannot use an index, so this reads the whole collection. On a large store run it
     * during a quiet period, the way the runbook's equivalent shell query says to.
     *
     * @return how many events the repair would touch, and how many of those have a position it cannot restore.
     */
    public UpdateEventRepairReport report() {
        long needingRepair = withRetry(() -> eventCollection.countDocuments(damagedEventFilter()));
        long lostPosition = withRetry(() -> eventCollection.countDocuments(and(exists(DcbCloudEvents.TAGS), exists(POSITION, false))));
        log.info("Repair report for collection '{}': {} events need repair. Separately, {} events have a position that cannot be restored.",
                eventStoreCollectionName, needingRepair, lostPosition);
        return new UpdateEventRepairReport(needingRepair, lostPosition);
    }

    /**
     * Repairs every damaged event in the collection, in {@code _id} order, resuming from any prior checkpoint. Blocks
     * until done.
     *
     * @return what was repaired, and what could not be.
     */
    public UpdateEventRepairResult run() {
        Document checkpoint = loadCheckpoint();
        Object lastProcessedId = checkpoint == null ? null : checkpoint.get(UpdateEventRepairCheckpoint.FIELD_LAST_PROCESSED_ID);
        long unrecoverableCount = checkpoint == null ? 0 : numberOrZero(checkpoint.get(UpdateEventRepairCheckpoint.FIELD_UNRECOVERABLE_COUNT));
        if (lastProcessedId != null) {
            log.info("Resuming the repair of collection '{}' after _id {}, from an earlier run that did not finish. Drop the '{}' collection to start from the beginning instead.",
                    eventStoreCollectionName, lastProcessedId, checkpointCollectionName(eventStoreCollectionName));
        }
        long repaired = 0;
        List<UnrecoverableEvent> unrecoverable = new ArrayList<>();

        while (true) {
            Object resumeAfter = lastProcessedId;
            List<Document> batch = withRetry(() -> eventCollection.find(and(damagedEventFilter(), afterFilter(resumeAfter)))
                    .sort(Sorts.ascending(ID))
                    .limit(options.batchSize())
                    .into(new ArrayList<>()));
            if (batch.isEmpty()) {
                break;
            }

            long repairedInBatch = 0;
            for (Document event : batch) {
                List<UnrecoverableEvent> found = new ArrayList<>(1);
                if (repairEvent(event, found)) {
                    repaired++;
                    repairedInBatch++;
                }
                if (!found.isEmpty()) {
                    // One document can produce more than one finding. A dcbtags value that is not a string and a
                    // position that cannot be read are independent damage, and an event carrying both reports both.
                    // The count is of events, because that is what the CLI's exit message and the runbook promise:
                    // how many events a person has to look at, not how many things are wrong with them.
                    unrecoverableCount++;
                }
                for (UnrecoverableEvent unrecoverableEvent : found) {
                    log.warn("Cannot fully repair event {} in collection '{}': {} ({}).",
                            unrecoverableEvent.eventId(), eventStoreCollectionName, unrecoverableEvent.reason(), unrecoverableEvent.detail());
                    if (unrecoverable.size() < options.maxReportedUnrecoverable()) {
                        unrecoverable.add(unrecoverableEvent);
                    }
                }
            }

            // Advance past the whole batch, including events nothing could be done about. They keep matching the
            // damaged-event filter, so without this the next batch would return them again and the run would not end.
            lastProcessedId = batch.getLast().get(ID);
            checkpoint(lastProcessedId, batch.size(), unrecoverableCount);
            log.info("Repaired {} of {} events in this batch of collection '{}', {} repaired so far.",
                    repairedInBatch, batch.size(), eventStoreCollectionName, repaired);

            if (options.throttleMillis() > 0) {
                sleep(options.throttleMillis());
            }
        }

        deleteCheckpoint();
        log.info("Repair of collection '{}' finished: {} events repaired, {} events hold damage that cannot be undone.",
                eventStoreCollectionName, repaired, unrecoverableCount);
        return new UpdateEventRepairResult(repaired, unrecoverableCount, unrecoverable);
    }

    /**
     * An event is damaged when its {@code position} is a string, which is what the old write-back's coercion left
     * behind, or when it carries the {@code dcbtags} extension without the indexed array derived from it. The two are
     * separate because one update can produce either alone. An event with no DCB tags only ever loses its position,
     * and a second update of an already repaired event would restore neither on its own.
     */
    private static Bson damagedEventFilter() {
        return or(
                type(POSITION, BsonType.STRING),
                and(exists(DcbCloudEvents.TAGS), exists(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, false))
        );
    }

    /**
     * Repairs one event in a single update, so the fields it can restore are written together or not at all.
     * <p>
     * That is atomicity across the recoverable fields, not a promise that both always come back. When one field is
     * beyond saving and the other is not, the recoverable one is still restored and the other is reported. An
     * unreadable position leaves the tag array repairable, and an unreadable tag encoding leaves the position
     * repairable. Only a rejected write keeps both exactly as they were found.
     *
     * @return whether this call's update reached the event. A write the server applied and then failed to acknowledge
     * counts, since the retry that follows it repairs nothing only because the first attempt already did.
     */
    private boolean repairEvent(Document event, List<UnrecoverableEvent> unrecoverable) {
        Object eventId = event.get(ID);
        Object storedPosition = event.get(POSITION);
        Object rawTags = event.get(DcbCloudEvents.TAGS);
        String encodedTags;
        if (rawTags == null || rawTags instanceof String) {
            encodedTags = (String) rawTags;
        } else {
            // The position does not depend on the tags, so carry on and repair it. Only the tag array is beyond
            // saving here, the same way an unreadable position below still leaves the tag array repairable.
            unrecoverable.add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.UNREADABLE,
                    "dcbtags is a " + rawTags.getClass().getSimpleName() + " rather than a string"));
            encodedTags = null;
        }
        List<Bson> updates = new ArrayList<>(2);

        if (storedPosition instanceof String positionAsString) {
            Long position;
            try {
                long parsedPosition = Long.parseLong(positionAsString);
                if (parsedPosition > 0) {
                    position = parsedPosition;
                } else {
                    // A store's positions start above zero, and getPosition returns zero for an event that has none,
                    // so zero and anything below it are values no store ever assigned. Writing one back as an int64
                    // would count as a repair and leave the event exactly as invisible, because every position query
                    // reads position greater than zero. Only a forged position gets here, so report it.
                    unrecoverable.add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.POSITION_NOT_POSITIVE, positionAsString));
                    position = null;
                }
            } catch (NumberFormatException e) {
                // The tag array does not depend on the position, so rebuild it anyway, the way a dropped position
                // does below. Only the position itself is beyond saving here.
                unrecoverable.add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.POSITION_NOT_A_NUMBER, positionAsString));
                position = null;
            }
            if (position != null) {
                Document positionHolder = new Document();
                PositionDocumentMapper.addPosition(positionHolder, position);
                updates.add(Updates.set(POSITION, positionHolder.get(POSITION)));
            }
        } else if (storedPosition == null && encodedTags != null) {
            // A DCB append always writes a position, so a DCB event without one lost it. The tag array below is still
            // worth rebuilding, and the position is reported rather than invented.
            unrecoverable.add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.POSITION_LOST, "no position field"));
        }

        if (encodedTags != null && !event.containsKey(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD)) {
            try {
                List<String> canonicalTags = DcbCloudEvents.decodeTags(encodedTags).stream()
                        .map(Tag::canonical)
                        .collect(toCollection(ArrayList::new));
                updates.add(Updates.set(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, canonicalTags));
            } catch (RuntimeException e) {
                unrecoverable.add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.UNREADABLE, String.valueOf(e.getMessage())));
            }
        }

        if (updates.isEmpty()) {
            return false;
        }

        // The duplicate key is caught inside the retried block, so a deterministic rejection returns rather than
        // throwing, and the retry only ever sees a transient failure. Re-running the same $set is harmless.
        //
        // Matched rather than modified, because a retry after an ambiguous failure has to count as the repair it is.
        // Every field in this update is one the event does not have yet: position is set only when it is a string, so
        // writing it changes its type, and the tag array only when the field is absent. A first attempt that reaches
        // the server therefore always modifies the document, and modified zero can only mean the lost acknowledgement
        // of a write that did land. Counting that as unrepaired would understate the run against the event's own log
        // line, which is written whatever the count says.
        return withRetry(() -> {
            try {
                return eventCollection.updateOne(eq(ID, eventId), Updates.combine(updates)).getMatchedCount() > 0;
            } catch (MongoWriteException e) {
                if (ErrorCategory.fromErrorCode(e.getError().getCode()) != ErrorCategory.DUPLICATE_KEY) {
                    throw e;
                }
                // Another event already holds this position as a number, and the unique position index refuses a
                // second claim on it. The update was rejected whole, so the event is exactly as it was found.
                unrecoverable.add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.POSITION_ALREADY_TAKEN, String.valueOf(storedPosition)));
                return false;
            }
        });
    }

    private @Nullable Document loadCheckpoint() {
        return withRetry(() -> checkpointCollection.find(eq(ID, UpdateEventRepairCheckpoint.CHECKPOINT_DOCUMENT_ID)).first());
    }

    private static long numberOrZero(@Nullable Object value) {
        return value instanceof Number number ? number.longValue() : 0;
    }

    private static Bson afterFilter(@Nullable Object lastProcessedId) {
        return lastProcessedId == null ? new Document() : gt(ID, lastProcessedId);
    }

    private void checkpoint(Object lastProcessedId, int batchSize, long unrecoverableCount) {
        withRetry(() -> checkpointCollection.findOneAndUpdate(
                eq(ID, UpdateEventRepairCheckpoint.CHECKPOINT_DOCUMENT_ID),
                Updates.combine(
                        Updates.set(UpdateEventRepairCheckpoint.FIELD_LAST_PROCESSED_ID, lastProcessedId),
                        Updates.set(UpdateEventRepairCheckpoint.FIELD_UNRECOVERABLE_COUNT, unrecoverableCount),
                        Updates.inc(UpdateEventRepairCheckpoint.FIELD_PROCESSED_COUNT, batchSize)
                ),
                new FindOneAndUpdateOptions().upsert(true)
        ));
    }

    // Remove the checkpoint once the whole collection has been walked, so a finished repair leaves no state behind
    // and a later run starts from the beginning and finds nothing to do.
    private void deleteCheckpoint() {
        withRetry(() -> checkpointCollection.deleteOne(eq(ID, UpdateEventRepairCheckpoint.CHECKPOINT_DOCUMENT_ID)));
    }

    private <T> T withRetry(Supplier<T> mongoOperation) {
        return executeWithRetry(mongoOperation, __ -> true, retryStrategy).get();
    }

    private static RetryStrategy defaultRetryStrategy() {
        return RetryStrategy.exponentialBackoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0f);
    }

    private static String checkpointCollectionName(String eventStoreCollectionName) {
        return eventStoreCollectionName + "_update_event_repair_checkpoint";
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Update event repair was interrupted while throttling between batches", e);
        }
    }

    /**
     * Command-line entry point taking {@code <mongoUri> <database> <collection> [report|repair]} and running with
     * {@link UpdateEventRepairOptions#defaults()}. Defaults to {@code report}, which changes nothing.
     * <p>
     * A {@code repair} that leaves any event unrepaired exits with status {@code 2}, so a job scheduler does not
     * record it as a clean run when a person still has to look at something.
     */
    public static void main(String[] args) {
        if (args.length < 3 || args.length > 4) {
            System.err.println("Usage: UpdateEventRepair <mongoUri> <database> <collection> [report|repair]");
            System.exit(1);
            return;
        }
        String mongoUri = args[0];
        String databaseName = args[1];
        String collectionName = args[2];
        String command = args.length == 4 ? args[3] : "report";
        if (!command.equals("report") && !command.equals("repair")) {
            System.err.println("Unknown command '" + command + "'. Use 'report' or 'repair'.");
            System.exit(1);
            return;
        }

        try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            UpdateEventRepair repair = new UpdateEventRepair(database, collectionName, UpdateEventRepairOptions.defaults());
            if (command.equals("report")) {
                log.info("Report: {}", repair.report());
                return;
            }
            UpdateEventRepairResult result = repair.run();
            log.info("Repair result: {}", result);
            if (result.unrecoverableEventCount() > 0) {
                // Exit non-zero so a job scheduler does not record a run that left events unrepaired as a success.
                // The events are named in the log above and each one needs a person to decide what to do about it.
                System.err.println(result.unrecoverableEventCount() + " event(s) hold damage that cannot be undone automatically."
                        + " See the WARN lines above and doc/runbooks/update-event-repair.md.");
                System.exit(2);
            }
        }
    }
}
