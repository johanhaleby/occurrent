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

package org.occurrent.subscription.push.blocking;

import org.occurrent.tck.subscription.blocking.SubscriptionModelConformance;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;

/**
 * The first conformance wiring this model has had on either stack. Phase 6 left the wrapper models to phase 8 and
 * phase 7 held the reactor twin back for the same reason: the suite had no way for a model to say that it replays a
 * history to every new subscription and takes no start position, so wiring it meant either loosening an assertion for
 * every other model or leaving this one untested.
 */
class CatchupThenPushSubscriptionModelConformanceTest extends SubscriptionModelConformance {

    @Override
    protected SubscriptionModelFixture createFixture() {
        return new CatchupThenPushSubscriptionModelFixture();
    }
}
