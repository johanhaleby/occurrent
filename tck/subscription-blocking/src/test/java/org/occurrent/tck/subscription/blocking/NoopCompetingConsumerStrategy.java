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

package org.occurrent.tck.subscription.blocking;

import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;

/**
 * A strategy that honours none of the contract, so that every test in
 * {@link CompetingConsumerStrategyConformance} can be seen failing against it.
 */
enum NoopCompetingConsumerStrategy implements CompetingConsumerStrategy {
    INSTANCE;

    @Override
    public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        throw unsupported("registerCompetingConsumer");
    }

    @Override
    public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
        throw unsupported("unregisterCompetingConsumer");
    }

    @Override
    public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
        throw unsupported("releaseCompetingConsumer");
    }

    @Override
    public boolean hasLock(String subscriptionId, String subscriberId) {
        throw unsupported("hasLock");
    }

    @Override
    public void addListener(CompetingConsumerListener listenerConsumer) {
        throw unsupported("addListener");
    }

    @Override
    public void removeListener(CompetingConsumerListener listenerConsumer) {
        throw unsupported("removeListener");
    }

    private static UnsupportedOperationException unsupported(String method) {
        return new UnsupportedOperationException(NoopCompetingConsumerStrategy.class.getSimpleName()
                + " honours nothing, and " + method + " is no exception");
    }
}
