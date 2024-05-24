/*
 *
 *  Copyright 2024 Johan Haleby
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

package org.occurrent.subscription.blocking.durable.catchup;

import org.occurrent.subscription.DurationToTimeoutConverter;
import org.occurrent.subscription.DurationToTimeoutConverter.Timeout;
import org.occurrent.subscription.api.blocking.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.concurrent.Future;

record CatchupSubscription(String id, Future<Subscription> delegatedSubscription) implements Subscription {
    private static final Logger log = LoggerFactory.getLogger(CatchupSubscription.class);

    @Override
    public boolean waitUntilStarted(Duration timeout) {
        final long timeStarted = System.currentTimeMillis();
        Timeout safeTimeout = DurationToTimeoutConverter.convertDurationToTimeout(timeout);
        try {
            Subscription subscription = delegatedSubscription.get(safeTimeout.timeout(), safeTimeout.timeUnit());
            long catchupEndTime = System.currentTimeMillis();
            long remainingMillisToWait = catchupEndTime - timeStarted;
            Duration remainingDurationToWait = timeout.minusMillis(remainingMillisToWait);
            subscription.waitUntilStarted(remainingDurationToWait);
            return true;
        } catch (Exception e) {
            logException(e, Level.WARN);
            return false;
        }
    }

    private static void logException(Exception e, Level level) {
        log.atLevel(level).log("Failed to wait until subscription was started because of exception: {} - {}", e.getClass().getName(), e.getMessage(), e);
    }
}
