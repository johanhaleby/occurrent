/*
 *
 *  Copyright 2023 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import org.bson.BsonNumber;
import org.jspecify.annotations.NullMarked;

import java.util.Objects;

@NullMarked
class ListenerLock {
    private final long version;
    private final long sentAt;

    public ListenerLock(BsonNumber version, long sentAt) {
        Objects.requireNonNull(version, "fencingToken");
        this.version = version.longValue();
        this.sentAt = sentAt;
    }

    /**
     * The lock's version, the fencing token referred to elsewhere in this class. It increments on a
     * genuine takeover and stays put on a refresh. {@code MongoLeaseCompetingConsumerStrategySupport}
     * reads this to answer {@code CompetingConsumerStrategy.fencingToken} (see ADR 116).
     */
    public long version() {
        return version;
    }

    /**
     * The clock reading taken before the request that set this lease was sent. MongoDB set the lease to expire
     * {@code leaseTime} after it handled that request, which was after this reading, so the lease cannot expire
     * sooner than {@code leaseTime} after it, as long as the clock of the database and the clock of this node agree.
     */
    public long sentAt() {
        return sentAt;
    }

    @Override
    public String toString() {
        return "ListenerLock{" +
                "fencingToken=" + version +
                '}';
    }
}
