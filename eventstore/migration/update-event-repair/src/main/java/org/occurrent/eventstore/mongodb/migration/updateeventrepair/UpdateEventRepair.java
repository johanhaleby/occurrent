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
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbDocumentMapper;
import org.occurrent.eventstore.mongodb.dcb.internal.DcbMarkerModel;
import org.occurrent.eventstore.mongodb.dcb.internal.PositionDocumentMapper;
import org.occurrent.eventstore.mongodb.dcb.internal.UpdateEventDamage;
import org.occurrent.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.occurrent.cloudevents.OccurrentCloudEventExtension.POSITION;

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
 * other. Three of those the tool still catches. A value another event already holds is refused by the unique index,
 * zero or a negative value is a position no store assigns, and a value above the store's position counter is one it
 * never handed out. What is left is a positive value inside the assigned range that happens to be free, in a gap in
 * the sequence for instance, and nothing distinguishes it from the event's own. The tool converts it to an int64 and
 * counts a repair, because nothing in the store records what the position was.
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
 * {@link #report()} sizes the damage and writes nothing. {@link #run()} repairs, walking the collection in
 * {@code _id} order in batches. A run is idempotent because it only touches events that still look damaged, and it
 * is safe to kill because each event is repaired on its own and a checkpoint document records how far it got. The
 * checkpoint is written twice per batch, once before any event in it is touched, with its range widened to every
 * position the batch could modify, and again after, narrowed back to exactly what the batch confirmed repairing.
 * A kill between the two still has the wider one on disk for a later run to load, so {@link #run()}'s own returned
 * range can be wider than what it actually repaired once a run has resumed past an interruption. See
 * {@link UpdateEventRepairResult}.
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
    private final MongoCollection<Document> positionCounterCollection;

    /**
     * Retries a MongoDB operation that failed with an error a later attempt can succeed on, with exponential backoff
     * from 100 ms up to 2 seconds and no limit on attempts, so a transient outage does not abandon a repair that may
     * have hours of collection left to walk. See {@link #UpdateEventRepair(MongoDatabase, String, UpdateEventRepairOptions, RetryStrategy)}
     * for which errors those are.
     */
    public UpdateEventRepair(MongoDatabase database, String eventStoreCollectionName, UpdateEventRepairOptions options) {
        this(database, eventStoreCollectionName, options, defaultRetryStrategy());
    }

    /**
     * Only an error a later attempt can succeed on without anyone changing anything is retried, and each retry is
     * logged at WARN with the error that caused it. That is the set MongoDB's retryable reads and retryable writes
     * specifications retry on, a network error, a cleared connection pool, an error labelled
     * {@code RetryableWriteError}, and a command or write concern error with one of the codes those specifications
     * list, such as a primary stepping down or a server shutting down. A failure to authenticate that one of those
     * caused is retried too, as the MongoDB driver retries it. Any other error, a missing privilege or no
     * server to select before the driver's server selection timeout for instance, fails the run at once, and running
     * it again resumes from the checkpoint.
     *
     * @param retryStrategy How to retry such an error. A repair walks a whole collection, so a strategy that gives up
     *                      immediately turns a momentary outage into a run an operator has to notice and restart. A
     *                      strategy built by {@link RetryStrategy#retry()} keeps its own {@code retryIf} as well, so it
     *                      can narrow the set above but not widen it. One you implement yourself decides on its own
     *                      which errors to retry.
     */
    public UpdateEventRepair(MongoDatabase database, String eventStoreCollectionName, UpdateEventRepairOptions options, RetryStrategy retryStrategy) {
        requireNonNull(database, "database cannot be null");
        this.retryStrategy = requireNonNull(retryStrategy, "retryStrategy cannot be null");
        this.eventStoreCollectionName = requireNonNull(eventStoreCollectionName, "eventStoreCollectionName cannot be null");
        this.options = requireNonNull(options, "options cannot be null");
        this.eventCollection = database.getCollection(eventStoreCollectionName);
        this.checkpointCollection = database.getCollection(checkpointCollectionName(eventStoreCollectionName));
        this.positionCounterCollection = database.getCollection(DcbMarkerModel.positionCollectionName(eventStoreCollectionName));
    }

    /**
     * Counts the damage in the collection without changing anything, so the size of a repair is known before one is
     * started. It writes nothing, so it is safe to run against a live store, but it is not cheap. Finding an event
     * whose tag array does not hold its tags cannot use an index, so this reads the whole collection. On a large
     * store run it during a quiet period, the way the runbook's equivalent shell query says to.
     *
     * <p>
     * It sizes a repair rather than predicting its outcome. The two counts it returns are independent of each other.
     * The first counts every event a run visits, and that includes an event it will only report, because a position
     * another event already holds, one that is not a positive integer, one above the counter and a tag encoding that
     * cannot be read are told apart from damage the repair can undo only during {@link #run()}, where each surfaces
     * as an {@link UnrecoverableEvent}.
     *
     * @return how many events the repair would touch, and separately how many have DCB tags and no position at all.
     */
    public UpdateEventRepairReport report() {
        long needingRepair = withRetry(() -> eventCollection.countDocuments(UpdateEventDamage.damagedEvent()));
        long lostPosition = withRetry(() -> eventCollection.countDocuments(UpdateEventDamage.positionLost()));
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
        // Bounds of every position this call and, once resumed, every earlier segment of this same run actually
        // confirmed repaired, carried across a resume the same way unrecoverableCount is and for the same reason.
        // Without that, a run killed after repairing positions in an earlier segment would return a range naming
        // only the segment the resumed call walked itself, hiding the earlier one from step 7. Only the per-event
        // loop below ever widens this pair. The pre-batch checkpoint widens a separate, wider value, so a run that
        // resumes past an interruption can start from that wider value here too, which is why a range carried
        // across a resume can include a position no segment of the run ever actually repaired, see
        // UpdateEventRepairResult.
        @Nullable Long minRepairedPosition = checkpoint == null ? null : numberOrNull(checkpoint.get(UpdateEventRepairCheckpoint.FIELD_MIN_REPAIRED_POSITION));
        @Nullable Long maxRepairedPosition = checkpoint == null ? null : numberOrNull(checkpoint.get(UpdateEventRepairCheckpoint.FIELD_MAX_REPAIRED_POSITION));
        // Read once up front. A damaged event predates this run, since no version from 0.34.0 on can create one, so
        // its position cannot exceed the counter as it stands now.
        long positionCeiling = positionCeiling();

        while (true) {
            Object resumeAfter = lastProcessedId;
            List<Document> batch = withRetry(() -> eventCollection.find(and(UpdateEventDamage.damagedEvent(), afterFilter(resumeAfter)))
                    .sort(Sorts.ascending(ID))
                    .limit(options.batchSize())
                    // Only the four fields a repair decision is made from. A stored event carries its data payload,
                    // which a repair never looks at and which reaches MongoDB's 16 MB document limit, so a batch of
                    // whole documents would hold far more in memory than the batch size suggests.
                    .projection(Projections.include(ID, POSITION, DcbCloudEvents.TAGS, DcbDocumentMapper.DCB_TAGS_INDEX_FIELD))
                    .into(new ArrayList<>()));
            if (batch.isEmpty()) {
                break;
            }

            // Planned once per event, before anything in the batch is touched, so the plan the widen below checks
            // is the same plan repairEvent writes from. Planning twice risked the two calls seeing different
            // answers for the same event, since a live store's position counter can advance between them, so a
            // candidate above the counter at the first call could validate at a second one, writing a position the
            // widen never saw and so never checkpointed.
            List<PlannedRepair> planned = planBatch(batch, positionCeiling);

            // Checkpoint a range and an unrecoverable count wide enough to cover whatever this batch is about to
            // modify, before touching any of it, so a checkpoint covering the batch already exists even for a kill
            // that skips the post-batch write below. This widens local copies, never minRepairedPosition,
            // maxRepairedPosition or unrecoverableCount themselves. The range only widens for a plan with a
            // readable position, a parse or validation failure or an unrebuildable tag array leave nothing to widen
            // it with. Neither the range nor the count asks whether another event currently owns a candidate
            // position, since that can change before repairEvent's write actually resolves it against the same
            // index, and a snapshot taken here would only be a stale guess of what that live check will find. That
            // omission is what closes the race an ownership check here would otherwise reopen. The same plan
            // decides what gets widened and what repairEvent writes from, so the widen and the write can never
            // disagree about a candidate, since only the live index can reject one, and only at write time. The
            // count widens for a plan with a finding already on it, since that finding is fixed once the plan is,
            // and it has to survive an event whose write fixes the one thing that made it match the damaged-event
            // filter, an unrebuildable tag array for instance, while a finding unrelated to that fix, a position
            // above the counter for instance, still needs reporting after a scan can no longer find the event
            // to report it from. The post-batch write below narrows the checkpoint back to exactly what got
            // confirmed, so these local values only outlive the batch when a kill catches it before that narrowing
            // runs.
            Long widenedMin = minRepairedPosition;
            Long widenedMax = maxRepairedPosition;
            long widenedUnrecoverableCount = unrecoverableCount;
            for (PlannedRepair plannedRepair : planned) {
                RepairPlan plan = plannedRepair.plan();
                if (!plan.updates().isEmpty() && plan.readablePosition() != null) {
                    long candidate = plan.readablePosition();
                    widenedMin = widenedMin == null ? candidate : Math.min(widenedMin, candidate);
                    widenedMax = widenedMax == null ? candidate : Math.max(widenedMax, candidate);
                }
                if (!plannedRepair.findings().isEmpty()) {
                    widenedUnrecoverableCount++;
                }
            }
            checkpointCrashRecord(widenedMin, widenedMax, widenedUnrecoverableCount);

            // Logged here, before any event in the batch is written, so a kill right after one of those writes
            // does not keep the finding out of the log even though the loop below is the ordinary place it gets
            // logged from. Only a plan's own findings are logged here, since those are fixed once the plan is. The
            // loop below logs only what is new since this pass, a write-time POSITION_ALREADY_TAKEN for instance,
            // so nothing gets logged twice.
            for (PlannedRepair plannedRepair : planned) {
                for (UnrecoverableEvent unrecoverableEvent : plannedRepair.findings()) {
                    log.warn("Cannot fully repair event {} in collection '{}': {} ({}).",
                            unrecoverableEvent.eventId(), eventStoreCollectionName, unrecoverableEvent.reason(), unrecoverableEvent.detail());
                }
            }

            long repairedInBatch = 0;
            for (PlannedRepair plannedRepair : planned) {
                int plannedFindingCount = plannedRepair.findings().size();
                List<Long> repairedPosition = new ArrayList<>(1);
                if (repairEvent(plannedRepair, repairedPosition)) {
                    repaired++;
                    repairedInBatch++;
                }
                for (long position : repairedPosition) {
                    minRepairedPosition = minRepairedPosition == null ? position : Math.min(minRepairedPosition, position);
                    maxRepairedPosition = maxRepairedPosition == null ? position : Math.max(maxRepairedPosition, position);
                }
                List<UnrecoverableEvent> found = plannedRepair.findings();
                if (!found.isEmpty()) {
                    // One document can produce more than one finding. A dcbtags value that is not a string and a
                    // position that cannot be read are independent damage, and an event carrying both reports both.
                    // The count is of events, because that is what the CLI's exit message and the runbook promise:
                    // how many events a person has to look at, not how many things are wrong with them.
                    unrecoverableCount++;
                }
                for (int i = 0; i < found.size(); i++) {
                    UnrecoverableEvent unrecoverableEvent = found.get(i);
                    // Findings up to plannedFindingCount were already logged above, before this event was written.
                    // Only a finding repairEvent's own write added, POSITION_ALREADY_TAKEN, is new here.
                    if (i >= plannedFindingCount) {
                        log.warn("Cannot fully repair event {} in collection '{}': {} ({}).",
                                unrecoverableEvent.eventId(), eventStoreCollectionName, unrecoverableEvent.reason(), unrecoverableEvent.detail());
                    }
                    if (unrecoverable.size() < options.maxReportedUnrecoverable()) {
                        unrecoverable.add(unrecoverableEvent);
                    }
                }
            }

            // Advance past the whole batch, including events nothing could be done about. They keep matching the
            // damaged-event filter, so without this the next batch would return them again and the run would not end.
            lastProcessedId = batch.getLast().get(ID);
            checkpoint(lastProcessedId, batch.size(), unrecoverableCount, minRepairedPosition, maxRepairedPosition);
            log.info("Repaired {} of {} events in this batch of collection '{}', {} repaired so far.",
                    repairedInBatch, batch.size(), eventStoreCollectionName, repaired);

            if (options.throttleMillis() > 0) {
                sleep(options.throttleMillis());
            }
        }

        // Asked of the collection rather than accumulated over the walk, because the walk cannot see all of it. A
        // POSITION_LOST event gets its tag array rebuilt, which stops it matching the damaged-event filter, so a run
        // killed between that write and the batch checkpoint leaves an event no resumed run rediscovers. Counting
        // what is still there means a finished run cannot report a clean collection while a position is still gone.
        long lostPosition = withRetry(() -> eventCollection.countDocuments(UpdateEventDamage.positionLost()));

        String repairedRange = minRepairedPosition == null
                ? "No position was repaired"
                : "Repaired positions ranged from " + minRepairedPosition + " to " + maxRepairedPosition;
        // checkpoint is unchanged since the top of this call, so it still says whether this run resumed one that
        // did not finish. Only such a run needs this, since the range and the count above are then upper bounds
        // rather than exact, per UpdateEventRepairResult.
        String precisionNote = checkpoint == null
                ? ""
                : " This run resumed one that did not finish, so the range and the count above can be upper bounds rather than exact.";
        log.info("Repair of collection '{}' finished: {} events repaired, {} events hold damage that cannot be undone, {} are left without a position. {}.{}",
                eventStoreCollectionName, repaired, unrecoverableCount, lostPosition, repairedRange, precisionNote);
        // Logged above before the checkpoint is removed, not after, so a kill between the two still leaves this
        // finished run's own result in the log. Deleting first would have made this the only durable copy of a
        // result nothing failed to compute, only failed to get out of the process, exactly the loss this class
        // otherwise checkpoints against.
        deleteCheckpoint();
        return new UpdateEventRepairResult(repaired, unrecoverableCount, lostPosition, unrecoverable, minRepairedPosition, maxRepairedPosition);
    }

    /**
     * Repairs one event from its plan in a single update, so the fields it can restore are written together or not
     * at all.
     * <p>
     * That is atomicity across the recoverable fields, not a promise that both always come back. When one field is
     * beyond saving and the other is not, the recoverable one is still restored and the other is reported. The tag
     * array is rebuilt next to an unreadable position, except a position stored as an array, since MongoDB refuses
     * to index two arrays in one document, and the position is restored next to an unreadable tag encoding. Only a
     * rejected write keeps both exactly as they were found.
     *
     * @param repairedPosition filled with this event's numeric {@code position}, but only once the update is
     *                         confirmed to have reached the event. That position can be one this call restored, or
     *                         one that was already correct, for instance a {@code POSITION_ALREADY_TAKEN} event
     *                         whose position an operator set by hand before this run only had its tag array left to
     *                         fix. Left empty when the update was rejected, or when the position stays unreadable.
     * @return whether this call's update reached the event. A write the server applied and then failed to acknowledge
     * counts, since the retry that follows it repairs nothing only because the first attempt already did.
     */
    private boolean repairEvent(PlannedRepair plannedRepair, List<Long> repairedPosition) {
        Object eventId = plannedRepair.event().get(ID);
        RepairPlan plan = plannedRepair.plan();

        if (plan.updates().isEmpty()) {
            return false;
        }

        // The duplicate key is caught inside the retried block, so a deterministic rejection returns rather than
        // throwing, and the retry only ever sees a transient failure. Re-running the same $set is harmless.
        //
        // Matched rather than modified, because a retry after an ambiguous failure has to count as the repair it is.
        // Every field in this update holds a value the event does not have yet. Position is set only when it is a
        // string, so writing it changes its type, and dcbtags and the tag array only when they list a different set
        // of tags than the one written. A first attempt that reaches the server therefore always modifies the
        // document, and modified zero can only mean the lost acknowledgement of a write that reached the server.
        // Counting that as unrepaired would understate the run against the event's own log line, which is written
        // whatever the count says.
        boolean wrote = withRetry(() -> {
            try {
                return eventCollection.updateOne(eq(ID, eventId), Updates.combine(plan.updates())).getMatchedCount() > 0;
            } catch (MongoWriteException e) {
                if (ErrorCategory.fromErrorCode(e.getError().getCode()) != ErrorCategory.DUPLICATE_KEY) {
                    throw e;
                }
                // Another event already holds this position as a number, and the unique position index refuses a
                // second claim on it. The update was rejected whole, so the event is exactly as it was found.
                plannedRepair.findings().add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.POSITION_ALREADY_TAKEN,
                        String.valueOf(plannedRepair.event().get(POSITION))));
                return false;
            }
        });
        if (wrote && plan.readablePosition() != null) {
            repairedPosition.add(plan.readablePosition());
        }
        return wrote;
    }

    // Plans every event in a batch exactly once, before anything in it is touched, so the plan the pre-batch widen
    // checks and the plan repairEvent writes from are the same object rather than two separate calls that could
    // answer differently. A live store's position counter can move between two calls, so a second, independent
    // planRepair could validate a candidate the first one had rejected, writing a position the widen never saw and
    // so never checkpointed.
    private List<PlannedRepair> planBatch(List<Document> batch, long positionCeiling) {
        List<PlannedRepair> planned = new ArrayList<>(batch.size());
        for (Document event : batch) {
            List<UnrecoverableEvent> findings = new ArrayList<>(1);
            planned.add(new PlannedRepair(event, planRepair(event, positionCeiling, findings), findings));
        }
        return planned;
    }

    // An event alongside its plan and the findings planning it produced, findings a real repair attempt reports
    // once repairEvent has also had its own chance to add a write-time one, POSITION_ALREADY_TAKEN, to the same list.
    private record PlannedRepair(Document event, RepairPlan plan, List<UnrecoverableEvent> findings) {
    }

    // What repairEvent would write for this event, and the position it would record if that write reaches the
    // server, computed without touching the event so planBatch's single call also tells the pre-batch widen what
    // this event would change.
    private RepairPlan planRepair(Document event, long positionCeiling, List<UnrecoverableEvent> unrecoverable) {
        Object eventId = event.get(ID);
        Object storedPosition = event.get(POSITION);
        Object rawTags = event.get(DcbCloudEvents.TAGS);
        String encodedTags;
        if (rawTags instanceof String tags) {
            encodedTags = tags;
        } else if (rawTags == null && !event.containsKey(DcbCloudEvents.TAGS)) {
            // No dcbtags field at all, which is an ordinary stream event rather than damage, unless it has a tag
            // index. Nothing says whether that index is stray or dcbtags was lost, so neither is written. A document
            // holding an explicit null falls through to the branch below, since the damaged-event filter matches it
            // and a run that neither updated it nor said anything about it would finish clean while report() still
            // counted it.
            if (event.containsKey(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD)) {
                unrecoverable.add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.UNREADABLE,
                        "no dcbtags field, while dcbTags holds " + event.get(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD)));
            }
            encodedTags = null;
        } else {
            // The position does not depend on the tags, so carry on and repair it. Only the tag array is beyond
            // saving here, the same way an unreadable position below still leaves the tag array repairable.
            unrecoverable.add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.UNREADABLE,
                    rawTags == null ? "dcbtags is null rather than a string" : "dcbtags is a " + rawTags.getClass().getSimpleName() + " rather than a string"));
            encodedTags = null;
        }
        List<Bson> updates = new ArrayList<>(2);
        @Nullable Long readablePosition = null;

        if (storedPosition instanceof String positionAsString) {
            Long position;
            try {
                long parsedPosition = Long.parseLong(positionAsString);
                position = validatedPosition(parsedPosition, positionCeiling, eventId, unrecoverable);
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
                readablePosition = position;
            }
        } else if (storedPosition == null && encodedTags != null) {
            // A DCB append always writes a position, so a DCB event without one lost it. The tag array below is still
            // worth rebuilding, and the position is reported rather than invented.
            String detail = event.containsKey(POSITION) ? "position is null" : "no position field";
            unrecoverable.add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.POSITION_LOST, detail));
        } else if (storedPosition != null) {
            // Never damaged by the old write-back, which only ever turned a position into a string. A repair that
            // follows a hand-set POSITION_ALREADY_TAKEN fix (the runbook's step 5) reaches this branch with a
            // position an operator typed by hand, so it is held to UpdateEventDamage's rule, the one
            // requireRepairedEvents checks, and a value that fails it is reported rather than read as the whole
            // number next to it. Only a whole number that fits in a long goes on to validatedPosition, as with a
            // string above.
            @Nullable Long position = UpdateEventDamage.wholeNumber(storedPosition);
            if (position == null) {
                String waiting = encodedTags != null && storedPosition instanceof List<?>
                        ? ", and the tag array is rebuilt only once position is no longer an array"
                        : "";
                unrecoverable.add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.POSITION_NOT_A_NUMBER,
                        storedPosition + " (" + storedPosition.getClass().getSimpleName() + ")" + waiting));
            } else {
                readablePosition = validatedPosition(position, positionCeiling, eventId, unrecoverable);
            }
        }

        // MongoDB refuses a document that holds two arrays under one compound index, and the dcbTags and position
        // index is one, so writing a tag array next to an array position fails the same way on every retry
        if (encodedTags != null && !(storedPosition instanceof List<?>)) {
            try {
                Set<Tag> tags = DcbCloudEvents.decodeTags(encodedTags);
                List<String> canonicalTags = tags.stream()
                        .map(Tag::canonical)
                        .collect(toCollection(ArrayList::new));
                Set<String> tagSet = new HashSet<>(canonicalTags);
                // requireRepairedEvents compares the tag array with the lines of dcbtags as stored, so a dcbtags
                // an operator wrote back by hand with stray whitespace is rewritten the way an append writes it
                if (!UpdateEventDamage.listedTags(encodedTags).equals(tagSet)) {
                    updates.add(Updates.set(DcbCloudEvents.TAGS, DcbCloudEvents.encodeTags(tags)));
                }
                if (!(event.get(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD) instanceof List<?> index && new HashSet<>(index).equals(tagSet))) {
                    updates.add(Updates.set(DcbDocumentMapper.DCB_TAGS_INDEX_FIELD, canonicalTags));
                }
            } catch (RuntimeException e) {
                unrecoverable.add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.UNREADABLE, String.valueOf(e.getMessage())));
            }
        }

        return new RepairPlan(updates, readablePosition);
    }

    private record RepairPlan(List<Bson> updates, @Nullable Long readablePosition) {
    }

    /**
     * A position is assignable when it is positive and at or below the store's position counter, whether it came
     * from parsing a damaged string or was read as a number from a document whose position was never damaged. Above
     * the counter is as unassignable as at or below zero, and DCB reads and reads in position order skip it just the
     * same, because they stop at this same counter. The counter is re-read here rather than trusted from the start of the run, so a
     * store that wrote while the repair walked cannot have an event wrongly called forged. A counter of zero means
     * there is no ceiling to compare against, which is what {@link #positionCeiling()} returns for a missing counter
     * document or one no writer would store.
     *
     * @return the position, or {@code null} if it was reported as unrecoverable instead.
     */
    private @Nullable Long validatedPosition(long candidate, long positionCeiling, Object eventId, List<UnrecoverableEvent> unrecoverable) {
        if (candidate > positionCeiling && positionCeiling > 0) {
            long ceilingNow = positionCeiling();
            if (ceilingNow > 0 && candidate > ceilingNow) {
                unrecoverable.add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.POSITION_ABOVE_COUNTER,
                        candidate + ", and the store's position counter is " + ceilingNow));
                return null;
            }
            return candidate;
        } else if (candidate > 0) {
            return candidate;
        } else {
            // A store's positions start above zero, and getPosition returns zero for an event that has none, so
            // zero and anything below it are values no store ever assigned. Writing one back as an int64 would
            // count as a repair and leave the event exactly as invisible, because every position query reads
            // position greater than zero. Only a forged or mistyped position gets here, so report it.
            unrecoverable.add(new UnrecoverableEvent(eventId, UnrecoverableEvent.Reason.POSITION_NOT_POSITIVE, String.valueOf(candidate)));
            return null;
        }
    }

    /**
     * The highest position the store has ever handed out, which is the ceiling on any position it assigned. DCB reads
     * and reads in position order stop at this same counter, so they skip an event above it as they skip one at or
     * below zero.
     *
     * @return the counter, or {@code 0} when there is no counter document or its value is one
     * {@link UpdateEventDamage#counterValue(Object)} rejects, which this treats as "no ceiling known" rather than as a
     * ceiling of zero. requireRepairedEvents refuses both until the counter is restored.
     */
    private long positionCeiling() {
        Document counter = withRetry(() -> positionCounterCollection.find(eq(ID, DcbMarkerModel.POSITION_DOCUMENT_ID)).first());
        Long value = counter == null ? null : UpdateEventDamage.counterValue(counter.get(DcbMarkerModel.COUNTER_POSITION));
        return value == null ? 0 : value;
    }

    private @Nullable Document loadCheckpoint() {
        return withRetry(() -> checkpointCollection.find(eq(ID, UpdateEventRepairCheckpoint.CHECKPOINT_DOCUMENT_ID)).first());
    }

    private static long numberOrZero(@Nullable Object value) {
        return value instanceof Number number ? number.longValue() : 0;
    }

    private static @Nullable Long numberOrNull(@Nullable Object value) {
        return value instanceof Number number ? number.longValue() : null;
    }

    private static Bson afterFilter(@Nullable Object lastProcessedId) {
        return lastProcessedId == null ? new Document() : gt(ID, lastProcessedId);
    }

    // Upserts the repaired-range and unrecoverable-count fields, leaving lastProcessedId and processedCount alone
    // since neither has changed yet for this batch. Creates the checkpoint document on a first-batch kill, the
    // same way the post-batch checkpoint below would have.
    //
    // The range widens safely under a replay because Math.min/Math.max of the same candidate twice is the
    // candidate. The count does not have that property. If a kill lands here and the batch is replayed because
    // lastProcessedId never advanced past it, an event whose plan can never produce an update, an unreadable tag
    // encoding on a position that itself can never be assigned for instance, is planned and counted again on the
    // replay, since nothing here can tell that the count it loaded already includes this same not-yet-confirmed
    // batch's contribution rather than only batches that finished. Telling those apart needs the checkpoint to
    // store the pending batch's own count apart from the confirmed one, which this does not do. Left this way
    // because undercounting, the gap this widen closes, hides real damage from an operator, and the count this
    // trades it for only overstates the damage instead.
    private void checkpointCrashRecord(@Nullable Long minRepairedPosition, @Nullable Long maxRepairedPosition, long unrecoverableCount) {
        if (minRepairedPosition == null && unrecoverableCount == 0) {
            return;
        }
        withRetry(() -> checkpointCollection.findOneAndUpdate(
                eq(ID, UpdateEventRepairCheckpoint.CHECKPOINT_DOCUMENT_ID),
                Updates.combine(
                        Updates.set(UpdateEventRepairCheckpoint.FIELD_MIN_REPAIRED_POSITION, minRepairedPosition),
                        Updates.set(UpdateEventRepairCheckpoint.FIELD_MAX_REPAIRED_POSITION, maxRepairedPosition),
                        Updates.set(UpdateEventRepairCheckpoint.FIELD_UNRECOVERABLE_COUNT, unrecoverableCount)
                ),
                new FindOneAndUpdateOptions().upsert(true)
        ));
    }

    private void checkpoint(Object lastProcessedId, int batchSize, long unrecoverableCount, @Nullable Long minRepairedPosition, @Nullable Long maxRepairedPosition) {
        withRetry(() -> checkpointCollection.findOneAndUpdate(
                eq(ID, UpdateEventRepairCheckpoint.CHECKPOINT_DOCUMENT_ID),
                Updates.combine(
                        Updates.set(UpdateEventRepairCheckpoint.FIELD_LAST_PROCESSED_ID, lastProcessedId),
                        Updates.set(UpdateEventRepairCheckpoint.FIELD_UNRECOVERABLE_COUNT, unrecoverableCount),
                        Updates.set(UpdateEventRepairCheckpoint.FIELD_MIN_REPAIRED_POSITION, minRepairedPosition),
                        Updates.set(UpdateEventRepairCheckpoint.FIELD_MAX_REPAIRED_POSITION, maxRepairedPosition),
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

    // Logged as the retry starts rather than when the error is caught, so an error the strategy has no attempts left
    // for is not logged as retried
    private <T> T withRetry(Supplier<T> mongoOperation) {
        AtomicReference<RuntimeException> previousError = new AtomicReference<>();
        return retryStrategy.execute(retryInfo -> {
            RuntimeException previous = previousError.get();
            if (previous != null) {
                log.warn("Retrying a MongoDB operation in the repair of collection '{}', attempt {}, after the error below.",
                        eventStoreCollectionName, retryInfo.getAttemptNumber(), previous);
            }
            try {
                return mongoOperation.get();
            } catch (RuntimeException e) {
                previousError.set(e);
                throw e;
            }
        }, UpdateEventRepair::retryable);
    }

    // The codes the MongoDB specifications retry a read or a write on, "Retryable Error" in
    // source/retryable-reads/retryable-reads.md and "Determining Retryable Write Errors" in
    // source/retryable-writes/retryable-writes.md at github.com/mongodb/specifications, and the list
    // CommandOperationHelper holds in driver 5.8.0. The two specifications list the same codes but for 134, which only
    // a read gets. A command error and a write concern error can both hold one, since MongoWriteConcernException takes
    // its code from the write concern error. A server selection timeout is not on either list, so an outage that
    // outlasts the driver's server selection timeout ends the run, and running it again resumes from the checkpoint.
    private static final Set<Integer> RETRYABLE_ERROR_CODES = Set.of(
            6, // HostUnreachable
            7, // HostNotFound
            89, // NetworkTimeout
            91, // ShutdownInProgress
            134, // ReadConcernMajorityNotAvailableYet
            189, // PrimarySteppedDown
            262, // ExceededTimeLimit
            9001, // SocketException
            10107, // NotWritablePrimary
            11600, // InterruptedAtShutdown
            11602, // InterruptedDueToReplStateChange
            13435, // NotPrimaryNoSecondaryOk
            13436 // NotPrimaryOrSecondary
    );

    // What the driver's CommandOperationHelper retries a read or a write on, an error isRetryableException accepts or
    // a MongoSecurityException whose cause it accepts, which is how a connection lost while authenticating arrives,
    // and besides that the RetryableWriteError label a 4.4 or later server puts on a write it may retry
    static boolean retryable(Throwable error) {
        return retryableByDriver(error)
                || error instanceof MongoSecurityException && retryableByDriver(error.getCause())
                || error instanceof MongoException mongoException && mongoException.hasErrorLabel("RetryableWriteError");
    }

    // CommandOperationHelper.isRetryableException, a network error, a cleared connection pool, which the retryable
    // reads specification also lists, a not primary or node is recovering error, which the driver also raises for an
    // older server that sends only a message, or one of the codes above
    private static boolean retryableByDriver(@Nullable Throwable error) {
        return error instanceof MongoException mongoException
                && (mongoException instanceof MongoSocketException
                || mongoException instanceof MongoConnectionPoolClearedException
                || mongoException instanceof MongoNotPrimaryException
                || mongoException instanceof MongoNodeIsRecoveringException
                || RETRYABLE_ERROR_CODES.contains(mongoException.getCode()));
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
            if (result.unrecoverableEventCount() > 0 || result.eventsWithLostPosition() > 0) {
                // Exit non-zero so a job scheduler does not record a run that left events unrepaired as a success.
                // The events are named in the log above and each one needs a person to decide what to do about it.
                System.err.println(result.unrecoverableEventCount() + " event(s) hold damage that cannot be undone automatically and "
                        + result.eventsWithLostPosition() + " are left without a position."
                        + " See the WARN lines above and doc/runbooks/update-event-repair.md.");
                System.exit(2);
            }
        }
    }
}
