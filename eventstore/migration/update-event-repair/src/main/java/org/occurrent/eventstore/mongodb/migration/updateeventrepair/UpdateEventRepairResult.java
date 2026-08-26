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

import org.jspecify.annotations.NullMarked;

import java.util.List;

/**
 * The outcome of an {@link UpdateEventRepair#run()} call.
 * <p>
 * An event can be counted in {@code eventsRepaired} and still appear in {@code unrecoverableEvents}. An event whose
 * position was dropped entirely, for instance, gets its {@code dcbTags} array rebuilt, which is counted as a repair,
 * while its position stays gone and is reported. A run where {@code unrecoverableEventCount} is {@code 0} restored
 * everything it could detect, which is not the same as everything the old write-back damaged. An update that dropped
 * the {@code dcbtags} extension outright leaves a document nothing can pick out from an ordinary stream event, so it
 * is neither counted nor repaired. See {@link UpdateEventRepair} for that case.
 *
 * @param eventsRepaired          How many stored events this call modified. A re-run after a completed run reports
 *                                {@code 0}, since the repair only touches events that still look damaged.
 * @param unrecoverableEventCount How many events hold damage this tool cannot undo. Counts every one, whether or not
 *                                it fitted in {@code unrecoverableEvents}. This counts events rather than findings,
 *                                so an event with two things wrong with it counts once, which is what the number is
 *                                for, how many events a person has to look at.
 * @param eventsWithLostPosition  How many events in the collection have DCB tags and no {@code position} at all once
 *                                this run finished. Asked of the collection rather than accumulated over the run, so
 *                                it covers events an earlier run repaired the tag array of and events this one was
 *                                killed before checkpointing. Repairing that array stops an event matching the
 *                                damaged-event filter, so no later run would rediscover it, and without this number a
 *                                finished run could report a clean collection while a position was still gone. Matches
 *                                {@link UpdateEventRepairReport#eventsWithLostPosition()}, and it is not a subset of
 *                                {@code unrecoverableEventCount}.
 * @param unrecoverableEvents     Up to {@link UpdateEventRepairOptions#maxReportedUnrecoverable()} findings from
 *                                THIS call. One event can appear more than once, since a {@code dcbtags} value that
 *                                is not a string and a position that cannot be read are independent damage, so this
 *                                list can be longer than {@code unrecoverableEventCount} even without a resume. A run
 *                                that resumed an interrupted one lists only what it saw itself, while
 *                                {@code unrecoverableEventCount} covers the earlier part too, since that count is
 *                                carried in the checkpoint. Every finding is logged at WARN when it is found, so
 *                                neither a truncated list nor a resume means a lost report.
 */
@NullMarked
public record UpdateEventRepairResult(long eventsRepaired, long unrecoverableEventCount, long eventsWithLostPosition,
                                      List<UnrecoverableEvent> unrecoverableEvents) {

    public UpdateEventRepairResult {
        unrecoverableEvents = List.copyOf(unrecoverableEvents);
    }
}
