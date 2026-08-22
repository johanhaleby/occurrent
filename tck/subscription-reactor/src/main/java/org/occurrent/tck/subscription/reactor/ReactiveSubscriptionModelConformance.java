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

package org.occurrent.tck.subscription.reactor;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.extension.ExtendWith;
import org.occurrent.subscription.api.reactor.SubscriptionHandle;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.tck.ConformanceEvents;
import org.occurrent.tck.FailureNamesTheTestClass;
import org.occurrent.tck.subscription.blocking.RecordedEvents;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The part of a reactive subscription model's contract that survives only until something blocks on a result.
 * <p>
 * Everything about what a model delivers, pauses, resumes and reports is asserted once, by the blocking suites running
 * over {@link BlockingSubscriptionOverReactive}, rather than described a second time in terms of {@code Mono}. What
 * that cannot reach is the reactor API's two publishers, the {@code Mono<Void>} an action returns, which the
 * <em>model</em> subscribes to, and {@link SubscriptionHandle#waitUntilStarted()}. A model can get both wrong and still
 * pass every bridged suite, because the bridge's own action has already run by the time it hands back a {@code Mono},
 * whether or not anything subscribes to it, and the bridge blocks either way.
 * <p>
 * Why each of them matters to somebody using the model:
 * <ul>
 *   <li>A model that calls the action function and drops the returned {@code Mono} without subscribing runs none of
 *       the work inside it. Every handler written the idiomatic way, {@code ce -> repository.save(ce)}, silently does
 *       nothing, which is the reactive equivalent of never calling the handler at all.</li>
 *   <li>An action whose {@code Mono} errors must fail through the model's own error path, the same path the blocking
 *       fixture declares as retry-or-propagate. A model that lets it detonate somewhere unrelated, or that stops
 *       itself, turns one failed delivery into an outage.</li>
 *   <li>{@link SubscriptionHandle#waitUntilStarted()} is promised to answer, and callers gate application startup on it. One
 *       that never completes hangs the caller. One that consumes its answer on first use never answers a second
 *       caller.</li>
 *   <li>Disposing a wait is a caller giving up on waiting, not on the subscription. A model that tears anything down
 *       on that disposal punishes an ordinary timeout pattern.</li>
 * </ul>
 * <p>
 * Every wait here has a timeout, and no test installs an action that fails forever. A retrying model would retry it for
 * the rest of the timeout, so failures are always fail-once.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the reactive contract a blocking bridge cannot see")
@ExtendWith(FailureNamesTheTestClass.class)
public abstract class ReactiveSubscriptionModelConformance {

    private static final String SUBSCRIPTION = "reactive-conformance";

    private @Nullable ReactiveSubscriptionModelFixture fixture;

    /**
     * Creates a fixture whose model has no subscriptions. Called before every test method.
     */
    protected abstract ReactiveSubscriptionModelFixture createFixture();

    @BeforeEach
    final void createTheFixture() {
        ReactiveSubscriptionModelFixture created = requireNonNull(createFixture(), "createFixture() returned null");
        Duration declared = requireNonNull(created.deliveryTimeout(),
                created.getClass().getName() + " returned null from deliveryTimeout()");
        if (declared.isZero() || declared.isNegative()) {
            throw new IllegalArgumentException(created.getClass().getName() + " declared a deliveryTimeout() of "
                    + declared + ". Every wait here is bounded by it, so a budget that is not positive makes each of "
                    + "them give up before looking.");
        }
        this.fixture = created;
    }

    /**
     * The budget every wait here is given, as {@link ReactiveSubscriptionModelFixture#deliveryTimeout()} declares it.
     */
    private Duration timeout() {
        return fixture().deliveryTimeout();
    }

    @AfterEach
    final void closeTheFixture() {
        ReactiveSubscriptionModelFixture current = this.fixture;
        this.fixture = null;
        if (current != null) {
            current.close();
        }
    }

    @Nested
    @DisplayName("the action's mono is the delivery, not a description of one")
    class TheActionMono {

        @Test
        void the_work_inside_the_actions_mono_runs_for_a_delivered_event() {
            RecordedEvents recorded = new RecordedEvents();
            SubscriptionHandle subscription = model().subscribe(SUBSCRIPTION,
                    cloudEvent -> Mono.fromRunnable(() -> recorded.accept(cloudEvent)));
            awaitStarted(subscription);

            fixture().publish(List.of(ConformanceEvents.event("1", "NameDefined")));

            assertThat(ConformanceEvents.idsOf(recorded.awaitAtLeast(1, timeout())))
                    .as("the recording runs inside the returned Mono, so it arriving proves the model subscribed to " +
                            "the Mono rather than assembling and dropping it, and a fire-and-forget model fails here")
                    .containsExactly("1");
        }

        @Test
        void subscribing_alone_runs_no_action() {
            AtomicBoolean ran = new AtomicBoolean();
            SubscriptionHandle subscription = model().subscribe(SUBSCRIPTION,
                    cloudEvent -> Mono.fromRunnable(() -> ran.set(true)));
            awaitStarted(subscription);

            // Asserted synchronously after the started-wait rather than after a grace period: registration completing
            // is the moment a registration-time invocation would already have happened, and a wait for "nothing more
            // arrives" proves nothing a timeout can defend.
            assertThat(ran)
                    .as("registering a subscription must not invoke its action, or assembling a pipeline runs it")
                    .isFalse();
        }

        @Test
        void an_action_whose_mono_errors_fails_through_the_model_and_leaves_it_running() {
            RecordedEvents recorded = new RecordedEvents();
            AtomicBoolean failedOnce = new AtomicBoolean();
            SubscriptionHandle subscription = model().subscribe(SUBSCRIPTION, cloudEvent -> {
                if (failedOnce.compareAndSet(false, true)) {
                    return Mono.error(new IllegalStateException("simulated action failure"));
                }
                return Mono.fromRunnable(() -> recorded.accept(cloudEvent));
            });
            awaitStarted(subscription);

            // Whether the failure is retried or reaches the publisher is the blocking fixture's declaration and is
            // asserted by the bridged suites. Both are allowed here, which is why the publish may throw; what is NOT
            // allowed is the model dying of it.
            try {
                fixture().publish(List.of(ConformanceEvents.event("1", "NameDefined")));
            } catch (RuntimeException propagatedActionFailure) {
                // A propagating model surfaces the action failure here, and that is one of the two documented answers.
            }

            assertThat(model().isRunning())
                    .as("one failed delivery must not stop the model")
                    .isTrue();

            fixture().publish(List.of(ConformanceEvents.event("2", "NameWasChanged")));

            // The wait is for the later event itself, not a count: a retrying model records the redelivered "1"
            // first, which would satisfy a count of one while "2" is still in flight.
            assertThat(ConformanceEvents.idsOf(recorded.awaitUntil(events -> ConformanceEvents.idsOf(events).contains("2"), timeout())))
                    .as("a later event must still be delivered after an action failed, or one bad event ends the " +
                            "subscription. A retrying model may also redeliver the failed event first, which is why " +
                            "only the later event's arrival is asserted")
                    .contains("2");
        }
    }

    @Nested
    @DisplayName("waitUntilStarted answers")
    class WaitUntilStarted {

        @Test
        void wait_until_started_answers_within_its_timeout() {
            SubscriptionHandle subscription = model().subscribe(SUBSCRIPTION, cloudEvent -> Mono.empty());

            assertThat(subscription.waitUntilStarted(timeout()).block())
                    .as("waitUntilStarted is promised to answer, and callers gate startup on it")
                    .isTrue();
        }

        @Test
        void asking_twice_answers_twice() {
            SubscriptionHandle subscription = model().subscribe(SUBSCRIPTION, cloudEvent -> Mono.empty());
            awaitStarted(subscription);

            assertThat(subscription.waitUntilStarted(timeout()).block())
                    .as("a second wait must answer too, or the answer is consumed by whoever asked first")
                    .isTrue();
        }

        @Test
        void disposing_a_wait_leaves_the_subscription_working() {
            RecordedEvents recorded = new RecordedEvents();
            SubscriptionHandle subscription = model().subscribe(SUBSCRIPTION,
                    cloudEvent -> Mono.fromRunnable(() -> recorded.accept(cloudEvent)));

            Disposable abandonedWait = subscription.waitUntilStarted().subscribe();
            abandonedWait.dispose();

            assertThat(subscription.waitUntilStarted(timeout()).block())
                    .as("disposing one wait is a caller giving up on waiting, not on the subscription")
                    .isTrue();

            fixture().publish(List.of(ConformanceEvents.event("1", "NameDefined")));

            assertThat(ConformanceEvents.idsOf(recorded.awaitAtLeast(1, timeout())))
                    .as("delivery must be unaffected by a disposed wait")
                    .containsExactly("1");
        }
    }

    private SubscriptionModel model() {
        return fixture().subscriptionModel();
    }

    private ReactiveSubscriptionModelFixture fixture() {
        return requireNonNull(fixture, "fixture is not initialized");
    }

    private void awaitStarted(SubscriptionHandle subscription) {
        Boolean started = subscription.waitUntilStarted(timeout()).block();
        assertThat(started)
                .as("the subscription must report started before the test can mean anything")
                .isTrue();
    }
}
