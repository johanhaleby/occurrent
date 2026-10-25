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

package org.occurrent.subscription.blocking.competingconsumers;

import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Behaves like the strategy it wraps until {@link #stall()} is called. From then on it acts like a node whose lease
 * refresh has stopped running, the way a loaded CI runner can stop it. {@link #hasLock(String, String)} answers false,
 * so the competing consumer holds back every event it gets, and a lease the wrapped strategy is granted is given straight
 * back, since a stalled node could not keep it.
 */
final class StallableCompetingConsumerStrategy implements CompetingConsumerStrategy {
    private final CompetingConsumerStrategy delegate;
    private final Map<CompetingConsumerListener, CompetingConsumerListener> listeners = new ConcurrentHashMap<>();
    private volatile boolean stalled;

    StallableCompetingConsumerStrategy(CompetingConsumerStrategy delegate) {
        this.delegate = delegate;
    }

    void stall() {
        stalled = true;
    }

    @Override
    public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        return delegate.registerCompetingConsumer(subscriptionId, subscriberId);
    }

    @Override
    public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        delegate.unregisterCompetingConsumer(subscriptionId, subscriberId);
    }

    @Override
    public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        delegate.releaseCompetingConsumer(subscriptionId, subscriberId);
    }

    @Override
    public boolean hasLock(String subscriptionId, String subscriberId) {
        return !stalled && delegate.hasLock(subscriptionId, subscriberId);
    }

    @Override
    public OptionalLong fencingToken(String subscriptionId) {
        return delegate.fencingToken(subscriptionId);
    }

    @Override
    public void addListener(CompetingConsumerListener listener) {
        CompetingConsumerListener stallable = new CompetingConsumerListener() {
            @Override
            public void onConsumeGranted(String subscriptionId, String subscriberId) {
                if (stalled) {
                    delegate.releaseCompetingConsumer(subscriptionId, subscriberId);
                } else {
                    listener.onConsumeGranted(subscriptionId, subscriberId);
                }
            }

            @Override
            public void onConsumeProhibited(String subscriptionId, String subscriberId) {
                listener.onConsumeProhibited(subscriptionId, subscriberId);
            }
        };
        listeners.put(listener, stallable);
        delegate.addListener(stallable);
    }

    @Override
    public void removeListener(CompetingConsumerListener listener) {
        CompetingConsumerListener stallable = listeners.remove(listener);
        if (stallable != null) {
            delegate.removeListener(stallable);
        }
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }
}
