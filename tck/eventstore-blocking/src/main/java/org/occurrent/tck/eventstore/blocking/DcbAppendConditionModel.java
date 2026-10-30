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

package org.occurrent.tck.eventstore.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.eventstore.api.dcb.DcbAppendCondition;
import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
import org.occurrent.eventstore.api.dcb.DcbCriteria;

/**
 * How a store decides whether a token-qualified {@link DcbAppendCondition} was violated.
 * <p>
 * Every model is sound. None lets an append succeed after a conflicting event. They differ in what else they do,
 * and the difference is observable, so a fixture declares which one its store implements and
 * {@code DcbEventStoreConformance} asserts the outcome that model owes rather than skipping the question.
 * <p>
 * The suite asks two questions of a model. Does an event of a type the criteria excludes conflict when it has a
 * tag the criteria requires? And does {@link DcbAppendCondition#wholeStoreLock(DcbConsistencyToken)} see a tag-scoped
 * append committed after its read? Each question has its own switch over this enum in {@code DcbEventStoreConformance},
 * because the answers don't always go together. A store that compares only markers can be coarse in both directions at
 * once, and a store that compares events can be exact in both. A store that compares markers and then queries the
 * events is exact for an excluded type and coarse for the whole-store lock, because the query needs a marker key to
 * select events by and {@link DcbCriteria#all()} has none.
 *
 * @see <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0021-dcb-write-path-query-scoped-concurrency.md">ADR 21, the tag-marker write path</a>
 * @see <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0030-keep-matchall-dcb-append-condition-with-documented-limit.md">ADR 30, the whole-store lock's documented limit</a>
 * @see <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0149-the-mongodb-dcb-append-check-compares-each-marker-and-then-the-events-above-it.md">ADR 149, the marker check followed by a query of the events</a>
 */
@NullMarked
public enum DcbAppendConditionModel {

    /**
     * The store answers a token-qualified condition by comparing the events committed after the token against the
     * condition's criteria, exactly as a read would. Occurrent's in-memory store works this way.
     * <p>
     * The two consequences the suite asserts:
     * <ul>
     *     <li>An event whose type the criteria excludes does <strong>not</strong> conflict, even when it has a tag
     *     the criteria requires, because the exclusion is applied to the event itself.</li>
     *     <li>{@link DcbAppendCondition#wholeStoreLock(DcbConsistencyToken)} <strong>does</strong> detect a
     *     tag-scoped append committed after the read, because {@link DcbCriteria#all()} matches that event like any
     *     other.</li>
     * </ul>
     */
    EXACT_CRITERIA,

    /**
     * The store compares the version markers kept per consistency boundary, and when the markers can't settle a
     * criteria item it queries the events committed above the versions the read saw. Occurrent's three MongoDB stores
     * work this way on a replica set (ADR 149).
     * <p>
     * The two consequences the suite asserts, which differ from both other models:
     * <ul>
     *     <li>An event whose type the criteria excludes does <strong>not</strong> conflict, even when it has a tag
     *     the criteria requires. The marker of the tag moved, so the store queries the events, and the query applies
     *     the exclusion to the event itself.</li>
     *     <li>{@link DcbAppendCondition#wholeStoreLock(DcbConsistencyToken)} does <strong>not</strong> detect a
     *     tag-scoped append committed after the read. {@link DcbCriteria#all()} has no marker key to select events by,
     *     so the check relies on a marker that only another whole-store append moves. This is the limitation
     *     {@link DcbAppendCondition#wholeStoreLock()} already documents, and the reason it is correct only for a single
     *     writer or a bootstrap append that races only with other bootstrap appends (ADR 30).</li>
     * </ul>
     * On a deployment where the store can't query, such as a sharded cluster, a MongoDB store refuses where the
     * query would have run. That is the {@link #TAG_MARKER} answer to the first question, and the suite doesn't assert
     * it for this model.
     */
    MARKERS_THEN_EVENTS,

    /**
     * The store answers a token-qualified condition by comparing version markers kept per consistency boundary and
     * never reads the events themselves. {@link DcbAppendCondition} allows a store to work this way. No store in this
     * repository declares it.
     * <p>
     * The two consequences the suite asserts, which are the exact inverse of {@link #EXACT_CRITERIA}:
     * <ul>
     *     <li>An event whose type the criteria excludes <strong>does</strong> conflict when it carries a tag the
     *     criteria requires, because the marker the token was derived from is keyed on the tag and knows nothing about
     *     types. It is a false conflict, and it self-heals, because the application service re-reads the
     *     still-excluded boundary and retries.</li>
     *     <li>{@link DcbAppendCondition#wholeStoreLock(DcbConsistencyToken)} does <strong>not</strong> detect a
     *     tag-scoped append committed after the read, because a whole-store lock is keyed on a marker that only
     *     another whole-store append touches. This is the limitation
     *     {@link DcbAppendCondition#wholeStoreLock()} already documents in as many words, and the reason it is
     *     correct only for a single writer or a bootstrap append that races only with other bootstrap appends (ADR 30).</li>
     * </ul>
     */
    TAG_MARKER
}
