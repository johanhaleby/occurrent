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

package org.occurrent.subscription.mongodb.internal;

import com.mongodb.WriteConcern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static org.occurrent.subscription.mongodb.internal.MongoCommons.CATCHUP_MARKER_FORGET_WRITE_CONCERN;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.CATCHUP_MARKER_FORGET_WRITE_CONCERN_WITHOUT_JOURNAL;
import static org.occurrent.subscription.mongodb.internal.MongoCommons.isJournalNotSupported;

/**
 * Sends a write of a catch-up marker forget with {@link MongoCommons#CATCHUP_MARKER_FORGET_WRITE_CONCERN}, and with
 * {@link MongoCommons#CATCHUP_MARKER_FORGET_WRITE_CONCERN_WITHOUT_JOURNAL} from the first time the server refuses
 * {@code j: true} because it runs without journaling. One instance per checkpoint storage, so the storage asks for
 * {@code j: true} only until that first refusal.
 * <p>
 * A server without journaling cannot journal a forget, so from that refusal on an acknowledged forget is not in any
 * on-disk journal and is lost if the server crashes. The guarantee that an acknowledged forget never rolls back holds
 * only on a server with journaling. The first refusal is logged as a warning.
 */
public final class CatchupMarkerForgetWrites {
    private static final Logger log = LoggerFactory.getLogger(CatchupMarkerForgetWrites.class);

    private volatile boolean journalNotSupported = false;

    /**
     * @param write sends the write with the write concern it is given
     */
    public void write(Consumer<WriteConcern> write) {
        if (!journalNotSupported) {
            try {
                write.accept(CATCHUP_MARKER_FORGET_WRITE_CONCERN);
                return;
            } catch (RuntimeException e) {
                if (!isJournalNotSupported(e)) {
                    throw e;
                }
                if (!journalNotSupported) {
                    journalNotSupported = true;
                    log.warn("The MongoDB server refused j: true because it runs without journaling. Catch-up marker forgets are sent without j from now on, so an acknowledged forget is lost if the server crashes.");
                }
            }
        }
        write.accept(CATCHUP_MARKER_FORGET_WRITE_CONCERN_WITHOUT_JOURNAL);
    }
}
