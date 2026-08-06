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
 * Both models are sound in the sense that matters. Neither lets a real conflict through as a successful append. They
 * differ in what else they do, and the difference is observable, so a fixture declares which one its store implements
 * and {@code DcbEventStoreConformance} asserts the outcome that model owes rather than skipping the question.
 * <p>
 * This is one declaration rather than one per symptom because the two symptoms are one fact seen from either side. A
 * model that compares the actual events against the actual criteria is exact in both directions. A model that compares
 * per-boundary version markers is coarser in both directions, and coarser means over-approximating for a criteria
 * narrower than its boundary and under-approximating for a boundary wider than the markers a scoped append touches.
 * The two enum constants therefore give opposite answers to both questions below, which is why one answer settles
 * them both.
 *
 * @see <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0021-dcb-write-path-query-scoped-concurrency.md">ADR 21, the tag-marker write path</a>
 * @see <a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0030-keep-matchall-dcb-append-condition-with-documented-limit.md">ADR 30, the whole-store lock's documented limit</a>
 */
@NullMarked
public enum DcbAppendConditionModel {

    /**
     * The store answers a token-qualified condition by comparing the events committed after the token against the
     * condition's criteria, exactly as a read would. Occurrent's in-memory store works this way.
     * <p>
     * The two consequences the suite asserts:
     * <ul>
     *     <li>An event whose type the criteria excludes does <strong>not</strong> conflict, even when it carries a tag
     *     the criteria requires, because the exclusion is applied to the event itself.</li>
     *     <li>{@link DcbAppendCondition#wholeStoreLock(DcbConsistencyToken)} <strong>does</strong> detect a
     *     tag-scoped append committed after the read, because {@link DcbCriteria#all()} matches that event like any
     *     other.</li>
     * </ul>
     */
    EXACT_CRITERIA,

    /**
     * The store answers a token-qualified condition by comparing version markers kept per consistency boundary, so it
     * never reads the events themselves. Occurrent's three MongoDB stores work this way, so that the check is a single
     * conditional write rather than a scan (ADR 21).
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
     *     correct only for a single writer or an empty-store guard (ADR 30).</li>
     * </ul>
     */
    TAG_MARKER
}
