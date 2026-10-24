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

/**
 * A strategy that grants no lease to anyone, for a test that never needs a subscription to run.
 */
final class NoLeases implements CompetingConsumerStrategy {
    @Override
    public boolean registerCompetingConsumer(String subscriptionId, String subscriberId) {
        return false;
    }

    @Override
    public void unregisterCompetingConsumer(String subscriptionId, String subscriberId) {
    }

    @Override
    public void releaseCompetingConsumer(String subscriptionId, String subscriberId) {
    }

    @Override
    public boolean hasLock(String subscriptionId, String subscriberId) {
        return false;
    }

    @Override
    public void addListener(CompetingConsumerListener listenerConsumer) {
    }

    @Override
    public void removeListener(CompetingConsumerListener listenerConsumer) {
    }
}
