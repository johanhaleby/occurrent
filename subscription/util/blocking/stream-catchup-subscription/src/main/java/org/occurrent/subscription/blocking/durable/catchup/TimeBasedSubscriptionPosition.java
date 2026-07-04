/*
 * Copyright 2020 Johan Haleby
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

package org.occurrent.subscription.blocking.durable.catchup;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.SubscriptionPosition;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static java.util.Objects.requireNonNull;
import static org.occurrent.time.internal.RFC3339.RFC_3339_DATE_TIME_FORMATTER;

@NullMarked
public class TimeBasedSubscriptionPosition implements SubscriptionPosition {

    private static final OffsetDateTime BEGINNING_OF_TIME = Instant.EPOCH.atOffset(ZoneOffset.UTC);

    private final OffsetDateTime time;

    public TimeBasedSubscriptionPosition(OffsetDateTime time) {
        requireNonNull(time, OffsetDateTime.class.getSimpleName() + " cannot be null");
        this.time = time;
    }

    public static TimeBasedSubscriptionPosition beginningOfTime() {
        return new TimeBasedSubscriptionPosition(BEGINNING_OF_TIME);
    }

    public static TimeBasedSubscriptionPosition from(OffsetDateTime time) {
        return new TimeBasedSubscriptionPosition(time);
    }

    public boolean isBeginningOfTime() {
        return BEGINNING_OF_TIME.equals(time);
    }

    @Override
    public String asString() {
        return RFC_3339_DATE_TIME_FORMATTER.format(time);
    }
}