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

package org.occurrent.subscription.push.reactor;

import org.occurrent.tck.subscription.blocking.SubscriptionModelConformance;
import org.occurrent.tck.subscription.blocking.SubscriptionModelFixture;

/**
 * The wiring phase 7 held back. It waited for the two declarations this model needs to describe itself: that it accepts
 * only the default start position, and that a subscription id it has not seen before is replayed the whole history.
 * Without them the suite asserted a fresh subscription starts at the present, which this model contradicts on purpose,
 * and the only alternatives were leaving it untested or loosening an assertion for every other model.
 */
class CatchupThenPushSubscriptionModelConformanceTest extends SubscriptionModelConformance {

    @Override
    protected SubscriptionModelFixture createFixture() {
        return new CatchupThenPushSubscriptionModelFixture();
    }
}
