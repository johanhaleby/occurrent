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

import org.jspecify.annotations.NullMarked;
import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy;

import java.time.Duration;

/**
 * What a {@link CompetingConsumerStrategy} implementation hands the conformance suite.
 * <p>
 * A fixture is created fresh for every test method, and the storage behind it <strong>must hold no locks</strong>. How
 * that is achieved is the implementation's business, and the suite never cleans up on an implementation's behalf.
 * <p>
 * Two things the suite cannot get from the interface, and both are here for the same reason. Contention between
 * competing consumers is external. A strategy coordinates with rivals it never holds a reference to, through storage
 * the interface does not mention, on a schedule the interface does not report.
 */
@NullMarked
public interface CompetingConsumerStrategyFixture {

    /**
     * The strategy under test, holding no locks.
     */
    CompetingConsumerStrategy competingConsumerStrategy();

    /**
     * Another strategy contending over the same storage as {@link #competingConsumerStrategy()}, holding no locks of its
     * own.
     * <p>
     * This is a factory rather than a second accessor because a single call is not enough. The suite needs a rival for
     * the strategy under test, and some of what it asserts needs a third instance that outlives a rival it deliberately
     * shuts down. Every call must hand back a <em>new</em> strategy contending over the same storage.
     * <p>
     * Constructing several strategies over one storage is therefore an explicit constraint on an implementor rather
     * than an accident of how Occurrent's own strategies happen to be built. Nothing on {@code CompetingConsumerStrategy}
     * lets one instance reach another, so a contract about who holds a lock cannot be asserted from one reference.
     * <p>
     * The suite shuts down every strategy it obtains here, so the fixture does not have to track them.
     */
    CompetingConsumerStrategy newCompetingConsumerStrategy();

    /**
     * The longest the suite will wait for the strategy's own coordination to reach the right answer about who holds a
     * lock, when nothing told it directly.
     * <p>
     * Three things the suite asserts need this, and they are one property seen from three sides. A rival takes over
     * from a holder that stopped coordinating. A registration that lost the lock wins it later without registering
     * again. A released consumer takes its lock back on its own. In none of them does anybody call into the strategy
     * that has to change its answer, so the change arrives on whatever schedule the implementation coordinates on, and
     * nothing on the interface reports that schedule.
     * <p>
     * <strong>This is a bound, not a delay.</strong> The suite waits for the condition to hold and stops as soon as it
     * does, so a generous bound costs a passing run nothing and is only paid in full by a test that was going to fail
     * anyway. Declare it comfortably above the implementation's worst case rather than tightly. A bound too small turns
     * a loaded machine into a red build, while a bound too large slows down only failures. Occurrent's MongoDB
     * strategies are lease based, so their worst case is one lease plus one refresh period, and their fixtures declare
     * several times that.
     * <p>
     * What this is <em>not</em> is a lease time. The contract has no notion of a lease, and an implementation
     * coordinating some other way (a leader election, a broker, a shared counter) still owes every property above and
     * still has a schedule of its own to declare here.
     */
    Duration timeToConverge();

    /**
     * Releases whatever the fixture opened, and shuts down the strategy under test. Called after every test method,
     * including a failing one, and after the suite has shut down every strategy it obtained from
     * {@link #newCompetingConsumerStrategy()}.
     */
    default void close() {
    }
}
