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
import org.junit.jupiter.api.extension.ExtendWith;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.FailureNamesTheTestClass;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Every subscription-model suite shares this fixture lifecycle. A fixture is created before each test and closed
 * after each test, even when the test failed, and it is reachable only from inside a test.
 * <p>
 * Package-private, so it adds nothing to what an implementation sees. It extends nothing and is extended only by the
 * three suites in this package, which is the same shape {@code EventStoreConformance} gives the event-store leaf.
 */
@NullMarked
@ExtendWith(FailureNamesTheTestClass.class)
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
        checkDeliveryTimeout(created);
        checkFixtureCanAnswerThisSuite(created);
        this.fixture = created;
    }

    private static void checkDeliveryTimeout(SubscriptionModelFixture fixture) {
        Duration declared = requireNonNull(fixture.deliveryTimeout(),
                fixture.getClass().getName() + " returned null from deliveryTimeout()");
        if (declared.isZero() || declared.isNegative()) {
            throw new IllegalArgumentException(fixture.getClass().getName() + " declared a deliveryTimeout() of "
                    + declared + ". Every wait in these suites is bounded by it, so a budget that is not positive "
                    + "makes each of them give up before looking.");
        }
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

    /**
     * The budget every wait in every suite here is given, as {@link SubscriptionModelFixture#deliveryTimeout()}
     * declares it. Checked for null and positive before the first assertion.
     * <p>
     * It lives here rather than as a constant on one suite because five suites wait on it and three of them used to
     * reach across to a constant on a fourth to get at it.
     */
    protected final Duration deliveryTimeout() {
        return fixture().deliveryTimeout();
    }
}
