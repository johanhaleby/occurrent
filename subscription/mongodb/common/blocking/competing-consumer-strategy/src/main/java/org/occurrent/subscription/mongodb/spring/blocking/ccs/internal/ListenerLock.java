/*
 * MIT License
 *
 * Copyright (c) 2020 Alec Henninger
 */

package org.occurrent.subscription.mongodb.spring.blocking.ccs.internal;

import org.bson.BsonNumber;

import java.util.Objects;

class ListenerLock {
    private final long version;

    public ListenerLock(BsonNumber version) {
        Objects.requireNonNull(version, "fencingToken");
        this.version = version.longValue();
    }

    public long version() {
        return version;
    }

    @Override
    public String toString() {
        return "ListenerLock{" +
                "fencingToken=" + version +
                '}';
    }
}
