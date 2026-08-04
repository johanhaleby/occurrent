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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.occurrent.subscription.api.blocking.SubscriptionModel;

import static java.util.Objects.requireNonNull;

/**
 * The fixture lifecycle every subscription-model suite shares: created before each test, closed after each test even
 * when the test failed, and reachable only from inside a test.
 * <p>
 * Package-private, so it adds nothing to what an implementation sees. It extends nothing and is extended only by the
 * three suites in this package, which is the same shape {@code EventStoreConformance} gives the event-store leaf.
 */
@NullMarked
abstract class SubscriptionModelSuite {

    private @Nullable SubscriptionModelFixture fixture;

    /**
     * Creates a fixture whose model has no subscriptions. Called before every test method.
     */
    protected abstract SubscriptionModelFixture createFixture();

    /**
     * Anything a particular suite needs to be true of the fixture before its first assertion. Runs once the model has
     * been checked for null. The default asks nothing further.
     */
    protected void checkFixtureCanAnswerThisSuite(SubscriptionModelFixture fixture) {
    }

    @BeforeEach
    final void createFixtureAndCheckItsDeclaration() {
        SubscriptionModelFixture created = requireNonNull(createFixture(), "createFixture() returned null");
        // Touch the accessor now, so a fixture that has not wired up its model says so before the first assertion
        // rather than halfway through a test that looks like a delivery failure.
        requireNonNull(created.subscriptionModel(),
                created.getClass().getName() + " returned null from subscriptionModel()");
        checkFixtureCanAnswerThisSuite(created);
        this.fixture = created;
    }

    @AfterEach
    final void closeFixture() {
        SubscriptionModelFixture current = this.fixture;
        this.fixture = null;
        if (current != null) {
            current.close();
        }
    }

    protected final SubscriptionModelFixture fixture() {
        SubscriptionModelFixture current = this.fixture;
        if (current == null) {
            throw new IllegalStateException("No fixture. It is created and closed per test method, so it cannot be "
                    + "reached from @BeforeAll or @AfterAll. Anything shared across the class, a container or a "
                    + "client, belongs in one of those rather than in the fixture.");
        }
        return current;
    }

    protected final SubscriptionModel subscriptionModel() {
        return fixture().subscriptionModel();
    }
}
