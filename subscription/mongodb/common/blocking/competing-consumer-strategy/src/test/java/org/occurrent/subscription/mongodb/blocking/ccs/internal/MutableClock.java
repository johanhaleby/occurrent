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

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * A clock a test moves itself, so a lease can be up without any time passing.
 * <p>
 * One instance stands for every node in a test, since two instances of an application see the same wall clock give or
 * take skew, and a lease means nothing if they do not.
 */
class MutableClock extends Clock {
    private volatile Instant now;

    MutableClock(Instant now) {
        this.now = now;
    }

    void advanceBy(Duration duration) {
        now = now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException("This clock is UTC and the code under test never asks for another zone");
    }

    @Override
    public Instant instant() {
        return now;
    }
}
