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

package org.occurrent.subscription;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;

/**
 * What a subscription model tells a recording projection about its own catch-up
 * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0132-an-append-has-an-identity-and-read-your-writes-becomes-a-membership-question.md">ADR 132</a>,
 * decision 6).
 * <p>
 * Told rather than asked. A recorder that samples a model has to work out what happened between two of its own
 * readings, and a catch-up that started and finished in between looks like no catch-up at all. Both calls here are
 * made by the model that owns the catch-up, at the moment it acts, so there is nothing to work out.
 * <p>
 * Both calls must return promptly and must not throw. They are made from the thread that registers or runs the
 * catch-up, and an implementation that blocked one would hold up the subscription itself.
 */
@NullMarked
public interface CatchupListener {

    /**
     * A catch-up has begun and has delivered nothing yet. The projection clears what it recorded before, and records
     * nothing until {@link #historyRead(Object)} for the same {@code episode}.
     * <p>
     * Sent before whatever produces the deliveries exists, so it always precedes the first one.
     *
     * @param episode Identifies this catch-up. Any object whose identity is unique to it, compared by identity and
     *                never interpreted, so it carries no ordering and means nothing beyond which catch-up it is.
     */
    void catchupStarted(Object episode);

    /**
     * The history this catch-up set out to read has been read, and what follows was written since it started. The
     * projection records from here on, because for some of those events this catch-up is the only delivery they get.
     * <p>
     * Ignored for any episode other than the one currently held, which is what stops a catch-up that has lost its
     * subscription from moving its replacement past a history the replacement has not read. Not sent at all for a
     * history a stop truncated, since a history that stopped part way through is not a history that was read.
     *
     * @param episode The catch-up whose history has been read, as given to {@link #catchupStarted(Object)}.
     */
    void historyRead(Object episode);

    /**
     * A live copy arrived of an event this catch-up's history read already delivered, so the model did not deliver it
     * a second time. The projection has applied the event and is meant to apply it exactly once, so this is not a
     * delivery and nothing here should apply it again. What it is, is the only chance the projection gets to write
     * down the append that event came from, since the history read wrote nothing down
     * (<a href="https://github.com/johanhaleby/occurrent/blob/main/doc/architecture/decisions/0137-a-live-payload-the-replay-already-delivered-still-reaches-its-source.md">ADR 137</a>).
     * <p>
     * Sent only after {@link #historyRead(Object)}, and only for an event the history read itself delivered. An event
     * an earlier live delivery already handled is not sent here, because that delivery wrote down what it owed. Sent
     * again for every further copy the source offers, so an implementation records rather than counts.
     * <p>
     * Must return promptly and must not throw, the same as the two calls above. The default does nothing, which is
     * what a listener that records nothing per event wants.
     *
     * @param event The event that was not delivered a second time.
     */
    default void alreadyDeliveredByReplay(CloudEvent event) {
    }
}
