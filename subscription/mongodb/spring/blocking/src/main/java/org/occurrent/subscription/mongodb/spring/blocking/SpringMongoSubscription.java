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

package org.occurrent.subscription.mongodb.spring.blocking;

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.DurationToTimeoutConverter;
import org.occurrent.subscription.api.blocking.Subscription;

import java.time.Duration;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.function.BooleanSupplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@NullMarked
public class SpringMongoSubscription implements Subscription {

    private final String subscriptionId;
    private final CountDownLatch started;
    private final BooleanSupplier modelIsShutDown;

    SpringMongoSubscription(String subscriptionId, CountDownLatch started, BooleanSupplier modelIsShutDown) {
        this.subscriptionId = subscriptionId;
        this.started = started;
        this.modelIsShutDown = modelIsShutDown;
    }

    @Override
    public String id() {
        return subscriptionId;
    }

    /**
     * Waits until the change stream of the subscription has opened. The wait also ends, with {@code false}, when the
     * subscription model is shut down.
     * <p>
     * An open change stream doesn't mean that MongoDB has confirmed it is healthy. A failure right after this returns
     * is still possible, and the subscription model then restarts the change stream.
     */
    @Override
    public boolean waitUntilStarted(Duration timeout) {
        long timeoutMillis = DurationToTimeoutConverter.convertDurationToTimeout(timeout, MILLISECONDS).timeout();
        long startTime = System.currentTimeMillis();
        try {
            while (!modelIsShutDown.getAsBoolean()) {
                long remaining = timeoutMillis - (System.currentTimeMillis() - startTime);
                if (remaining <= 0) {
                    return started.getCount() == 0;
                }
                if (started.await(Math.min(100, remaining), MILLISECONDS)) {
                    return true;
                }
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", SpringMongoSubscription.class.getSimpleName() + "[", "]")
                .add("subscriptionId='" + subscriptionId + "'")
                .add("started=" + (started.getCount() == 0))
                .toString();
    }
}
