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

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StreamSubscriptionFilter;
import org.occurrent.subscription.SubscriptionAlreadyRunningException;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.SubscriptionNotRunningException;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.UnsupportedStartAtException;
import org.occurrent.subscription.UnsupportedSubscriptionFilterException;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.tck.ConformanceEvents;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.tck.ConformanceEvents.idsOf;

/**
 * The contract every {@link SubscriptionModel} owes. An event published while a subscription is running reaches its
 * handler, a filter decides which events do, and the life cycle does what it says.
 * <p>
 * An implementation extends this and supplies a {@link SubscriptionModelFixture}:
 * <pre>{@code
 * class PostgresqlSubscriptionModelTest extends SubscriptionModelConformance {
 *     @Override
 *     protected SubscriptionModelFixture createFixture() {
 *         return new PostgresqlSubscriptionModelFixture();
 *     }
 * }
 * }</pre>
 * <p>
 * Not extending this suite is how an implementation declines to be conformance tested. That is a visible, searchable
 * absence rather than a runtime skip, and nothing here calls {@code Assumptions}. Where models legitimately differ, the
 * fixture declares which way it goes and the suite asserts the documented outcome for that answer, so both branches are
 * checked by somebody.
 * <p>
 * <strong>How this suite waits.</strong> Every wait is for something that must arrive within the budget the fixture
 * declares in {@link SubscriptionModelFixture#deliveryTimeout()},
 * through {@link RecordedEvents}. For "this event must not arrive" it publishes a marker afterwards and waits for the marker,
 * then asserts the whole recorded list. That is not a stylistic choice. A wait for a period in which nothing happens
 * passes just as well against a subscription that was never listening, and any constant short enough to keep a test
 * quick is short enough to flake on a loaded CI runner. The marker rests on one property every model has, that a
 * subscription receives events in publish order, so a marker published after the forbidden event cannot arrive first.
 * <p>
 * What this suite deliberately does not assert:
 * <ul>
 *     <li><strong>The wording of a refusal.</strong> The type is the contract and the message is not. Every member of
 *     the {@link org.occurrent.subscription.SubscriptionRefusedException} family builds its message in its own
 *     constructor, so Occurrent's models happen to word each one identically, but an implementation is free to supply
 *     a message of its own and nothing here reads one.</li>
 *     <li><strong>Ordering across two subscriptions.</strong> Two subscriptions are two cursors or two threads, and one
 *     may be several events behind the other at any moment. Only order within one subscription is a promise.</li>
 *     <li><strong>Order by the {@code position} extension.</strong> A position is reserved outside the write
 *     transaction, so a lower position can commit after a higher one. Arrival order is the promise, not position order.</li>
 *     <li><strong>At-least-once delivery, and resuming after a restart.</strong> Neither is a promise of this contract.
 *     Both need durable state that survives the model, which only some models have. {@link RestartConformance} covers
 *     them, and a model that cannot be rebuilt over the state it left behind declines it by not extending it.</li>
 * </ul>
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the subscription model contract")
@Timeout(60)
public abstract class SubscriptionModelConformance extends SubscriptionModelSuite {

    /**
     * Creates a fixture whose model has no subscriptions. Called before every test method.
     */
    @Override
    protected abstract SubscriptionModelFixture createFixture();

    @Override
    protected void checkFixtureCanAnswerThisSuite(SubscriptionModelFixture fixture) {
        Set<StartAtVariant> accepted = requireNonNull(fixture.acceptedStartAtVariants(),
                fixture.getClass().getName() + " returned null from acceptedStartAtVariants()");
        if (accepted.isEmpty()) {
            throw new IllegalStateException(fixture.getClass().getName() + " accepts no StartAt variant at all, so "
                    + "nothing can subscribe to this model. Every model accepts at least "
                    + StartAtVariant.SUBSCRIPTION_MODEL_DEFAULT + ", which is what the no-argument subscribe overloads "
                    + "pass.");
        }
        requireNonNull(fixture.aCheckpointToStartFrom(),
                fixture.getClass().getName() + " returned null from aCheckpointToStartFrom()");
    }

    private static String subscriptionId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Subscribes and waits until the subscription is listening, so an event published next cannot outrun it.
     */
    private RecordedEvents subscribeAndWait(String subscriptionId, @Nullable SubscriptionFilter filter) {
        RecordedEvents recorded = new RecordedEvents();
        Subscription subscription = subscriptionModel().subscribe(subscriptionId, filter, StartAt.subscriptionModelDefault(), recorded);
        assertThat(subscription.waitUntilStarted(deliveryTimeout()))
                .as("the subscription must report started within %s, or nothing published afterwards can be expected "
                        + "to arrive", deliveryTimeout())
                .isTrue();
        return recorded;
    }

    private RecordedEvents subscribeAndWait(String subscriptionId) {
        return subscribeAndWait(subscriptionId, null);
    }

    private void publish(CloudEvent... events) {
        fixture().publish(List.of(events));
    }

    /**
     * Asserts that exactly these events arrived, in this order, by waiting for as many as are expected.
     */
    private void assertReceives(RecordedEvents recorded, CloudEvent... expected) {
        List<CloudEvent> received = recorded.awaitAtLeast(expected.length, deliveryTimeout());
        assertThat(idsOf(received))
                .as("every published event must reach the handler, in publish order")
                .containsExactly(idsOf(expected).toArray(new String[0]));
    }

    /**
     * Asserts that the forbidden events never arrive, by publishing a marker afterwards and waiting for that.
     * <p>
     * The marker is what makes this non-vacuous. Waiting for a quiet period would pass against a subscription that was
     * never listening at all, whereas a marker that arrives proves the subscription is alive and that the forbidden
     * events still did not come through it.
     */
    private void assertReceivesOnlyTheMarker(RecordedEvents recorded) {
        CloudEvent marker = ConformanceEvents.event("marker-" + UUID.randomUUID(), "MarkerEvent");
        publish(marker);

        List<CloudEvent> received = recorded.awaitAtLeast(1, deliveryTimeout());
        assertThat(idsOf(received))
                .as("the marker proves the subscription is listening, so anything else in this list arrived when it "
                        + "should not have")
                .containsExactly(marker.getId());
    }

    @Nested
    @DisplayName("delivering")
    class Delivering {

        @Test
        void delivers_every_published_event_to_a_running_subscription_in_order() {
            RecordedEvents recorded = subscribeAndWait(subscriptionId());
            CloudEvent first = ConformanceEvents.event("1", "NameDefined");
            CloudEvent second = ConformanceEvents.event("2", "NameWasChanged");
            CloudEvent third = ConformanceEvents.event("3", "NameWasChanged");

            publish(first, second, third);

            assertReceives(recorded, first, second, third);
        }

        @Test
        void delivers_only_the_events_a_filter_matches() {
            SubscriptionFilter onlyNameDefined = new StreamSubscriptionFilter(Filter.type("NameDefined"));
            RecordedEvents recorded = subscribeAndWait(subscriptionId(), onlyNameDefined);
            CloudEvent matching = ConformanceEvents.event("1", "NameDefined");
            CloudEvent other = ConformanceEvents.event("2", "NameWasChanged");
            CloudEvent alsoMatching = ConformanceEvents.event("3", "NameDefined");

            publish(matching, other, alsoMatching);

            assertReceives(recorded, matching, alsoMatching);
        }

        @Test
        void the_convenience_overloads_behave_as_the_explicit_call() {
            // subscribe(id, action) documents itself as no filter plus the model's default start position, so a model
            // that reads either differently would deliver something other than everything.
            RecordedEvents recorded = new RecordedEvents();
            Subscription subscription = subscriptionModel().subscribe(subscriptionId(), recorded);
            assertThat(subscription.waitUntilStarted(deliveryTimeout())).isTrue();
            CloudEvent event = ConformanceEvents.event("1", "NameDefined");

            publish(event);

            assertReceives(recorded, event);
        }

        @Test
        void refuses_a_subscription_id_that_is_already_in_use() {
            String id = subscriptionId();
            subscribeAndWait(id);

            assertThatThrownBy(() -> subscriptionModel().subscribe(id, new RecordedEvents()))
                    .as("a subscription id identifies one subscription, so reusing a live one has to be refused rather "
                            + "than silently replacing the handler that is already there")
                    .isInstanceOf(DuplicateSubscriptionIdException.class);
        }

        @Test
        void refuses_a_subscription_filter_it_does_not_understand() {
            SubscriptionFilter unrecognised = new SubscriptionFilter() {
            };

            assertThatThrownBy(() -> subscriptionModel().subscribe(subscriptionId(), unrecognised, StartAt.subscriptionModelDefault(), new RecordedEvents()))
                    .as("a filter a model cannot apply must be refused, since accepting it and ignoring it would "
                            + "deliver events the caller asked not to receive")
                    .isInstanceOf(UnsupportedSubscriptionFilterException.class);
        }
    }

    @Nested
    @DisplayName("the start position")
    class TheStartPosition {

        @Test
        void delivers_from_the_start_positions_it_accepts_and_refuses_the_rest() {
            // Every one of the four is asked about on every model, so a variant left out of the declaration is a claim
            // the model has to live up to rather than a way of not being asked. Sealed at four by StartAt itself.
            for (StartAtVariant variant : StartAtVariant.values()) {
                if (fixture().acceptedStartAtVariants().contains(variant)) {
                    assertDeliversStartingFrom(variant);
                } else {
                    assertRefuses(variant);
                }
            }
        }

        private void assertDeliversStartingFrom(StartAtVariant variant) {
            String id = subscriptionId();
            RecordedEvents recorded = new RecordedEvents();
            // Passed as a supplier, so only CHECKPOINT pays for it and it is read at the moment that variant
            // subscribes. Reading it once for the whole loop instead looks like the obvious saving and is wrong: by the
            // time CHECKPOINT ran, the earlier variants had published events of their own, so a change-stream model
            // started at that older position replayed them and the wait below was satisfied by a replayed event
            // instead of this variant's own.
            Subscription subscription = subscriptionModel()
                    .subscribe(id, null, variant.startAt(() -> fixture().aCheckpointToStartFrom()), recorded);
            assertThat(subscription.waitUntilStarted(deliveryTimeout()))
                    .as("this model declares it accepts %s, so a subscription starting there must report started", variant)
                    .isTrue();
            CloudEvent event = ConformanceEvents.event(UUID.randomUUID().toString(), "NameDefined");

            publish(event);

            // Waits for this event rather than for a count. A start position is allowed to be one a model replays from,
            // so the first thing to arrive is not necessarily the thing this assertion is about, and a count-wait would
            // read the list while it was still filling.
            List<CloudEvent> received = recorded.awaitUntil(
                    events -> idsOf(events).contains(event.getId()), deliveryTimeout());
            assertThat(idsOf(received))
                    .as("this model declares it accepts %s, and an accepted start position owes a working "
                            + "subscription. Accepting one and then delivering nothing is the failure this catches: it "
                            + "leaves a caller holding a subscription that never says anything is wrong", variant)
                    .contains(event.getId());
            // Freed before the next variant, so a model that feeds one subscription at a time gets to answer for all
            // four rather than refusing the second for a reason this test is not about.
            subscriptionModel().cancelSubscription(id);
        }

        private void assertRefuses(StartAtVariant variant) {
            assertThatThrownBy(() -> subscriptionModel()
                    .subscribe(subscriptionId(), null, variant.startAt(() -> fixture().aCheckpointToStartFrom()), new RecordedEvents()))
                    .as("this model declares it does not accept %s, and a start position it cannot honour has to be "
                            + "refused rather than quietly ignored, which would start the subscription somewhere the "
                            + "caller did not ask for", variant)
                    .isInstanceOf(UnsupportedStartAtException.class);
        }

        @Test
        void replays_history_to_a_new_subscription_or_starts_where_it_was_told_as_the_fixture_declares() {
            CloudEvent beforeAnythingSubscribed = ConformanceEvents.event("1", "NameDefined");
            publish(beforeAnythingSubscribed);

            RecordedEvents recorded = subscribeAndWait(subscriptionId());

            if (fixture().replaysHistoryToANewSubscription()) {
                assertThat(idsOf(recorded.awaitAtLeast(1, deliveryTimeout())))
                        .as("this model declares it replays its history to a subscription id it has not seen before, "
                                + "so an event published before that subscription existed still has to arrive")
                        .contains(beforeAnythingSubscribed.getId());
            } else {
                assertReceivesOnlyTheMarker(recorded);
            }
        }
    }

    @Nested
    @DisplayName("a failing handler")
    class AFailingHandler {

        @Test
        void is_retried_or_propagates_as_the_fixture_declares() {
            String id = subscriptionId();
            AtomicInteger calls = new AtomicInteger();
            RecordedEvents afterTheFailure = new RecordedEvents();
            // Throws once. Never forever: a retrying model has no attempt cap by default, so a handler that always
            // throws would spin until the test timed out instead of failing with a reason.
            Subscription subscription = subscriptionModel().subscribe(id, cloudEvent -> {
                if (calls.incrementAndGet() == 1) {
                    throw new IllegalStateException("failing on purpose, once");
                }
                afterTheFailure.accept(cloudEvent);
            });
            assertThat(subscription.waitUntilStarted(deliveryTimeout())).isTrue();
            CloudEvent event = ConformanceEvents.event("1", "NameDefined");

            if (fixture().retriesAFailingHandler()) {
                fixture().publish(List.of(event));

                List<CloudEvent> received = afterTheFailure.awaitAtLeast(1, deliveryTimeout());
                assertThat(idsOf(received))
                        .as("this model declares it retries, so the event must reach the handler on a later attempt "
                                + "rather than being lost to the first failure")
                        .containsExactly(event.getId());
            } else {
                assertThatThrownBy(() -> fixture().publish(List.of(event)))
                        .as("this model declares it does not retry, so the exception must reach whoever published the "
                                + "event rather than being swallowed")
                        .isInstanceOf(RuntimeException.class);
                assertThat(calls.get())
                        .as("the handler must have been called once, or nothing was delivered at all")
                        .isEqualTo(1);
            }
        }
    }

    @Nested
    @DisplayName("the life cycle")
    class TheLifeCycle {

        @Test
        void reports_a_running_subscription_as_running_and_not_paused() {
            String id = subscriptionId();
            subscribeAndWait(id);

            assertThat(subscriptionModel().isRunning(id)).as("a subscription that was just started is running").isTrue();
            assertThat(subscriptionModel().isPaused(id)).as("and it is not paused").isFalse();
        }

        @Test
        void reports_an_unknown_subscription_as_neither_running_nor_paused() {
            String neverSubscribed = subscriptionId();

            assertThat(subscriptionModel().isRunning(neverSubscribed)).isFalse();
            assertThat(subscriptionModel().isPaused(neverSubscribed)).isFalse();
        }

        @Test
        void a_paused_subscription_is_paused_and_not_running() {
            String id = subscriptionId();
            subscribeAndWait(id);

            subscriptionModel().pauseSubscription(id);

            assertThat(subscriptionModel().isPaused(id)).isTrue();
            assertThat(subscriptionModel().isRunning(id)).isFalse();
        }

        @Test
        void a_paused_subscription_receives_what_the_fixture_declares_and_delivers_again_once_resumed() {
            String id = subscriptionId();
            RecordedEvents recorded = subscribeAndWait(id);
            // One delivered event before pausing, so a model that resumes from where it last got to has a position to
            // resume from. Without this the test asks a different question of a change-stream model, which then resumes
            // from the present and misses the paused-window event for a reason that has nothing to do with pausing.
            CloudEvent beforePausing = ConformanceEvents.event("1", "NameDefined");
            publish(beforePausing);
            assertReceives(recorded, beforePausing);

            subscriptionModel().pauseSubscription(id);
            CloudEvent whilePaused = ConformanceEvents.event("2", "NameWasChanged");

            publish(whilePaused);
            subscriptionModel().resumeSubscription(id).waitUntilStarted(deliveryTimeout());

            if (fixture().deliversEventsPublishedWhilePaused()) {
                // Held for the subscription, so it arrives before the marker that was published after it.
                CloudEvent marker = ConformanceEvents.event("3", "MarkerEvent");
                publish(marker);
                // The wait is for the order the assertion is about, not a count: a model resuming through a replay
                // handed over to a live feed reaches a count of 2 while the marker is still crossing that handover,
                // and a count-wait would then assert on a list that was still growing.
                List<CloudEvent> received = recorded.awaitUntil(events -> {
                    List<String> ids = idsOf(events);
                    int held = ids.indexOf(whilePaused.getId());
                    return held >= 0 && ids.subList(held + 1, ids.size()).contains(marker.getId());
                }, deliveryTimeout());
                // A subsequence rather than the exact list, because a model resuming from the last event it delivered
                // rather than from just after it may hand that one over again. Redelivery is not forbidden by this
                // contract, so asserting the exact list here would reject an at-least-once model over something this
                // test is not about.
                assertThat(idsOf(received))
                        .as("this model declares it holds events for a paused subscription, so the held one must "
                                + "arrive, and before anything published after it")
                        .containsSubsequence(whilePaused.getId(), marker.getId());
            } else {
                // Dropped rather than deferred, so resuming must not replay it.
                assertReceivesOnlyTheMarker(recorded);
            }
        }

        @Test
        void pausing_one_subscription_leaves_another_delivering() {
            if (!fixture().acceptsSeveralSubscriptions()) {
                String only = subscriptionId();
                subscribeAndWait(only);

                assertThatThrownBy(() -> subscriptionModel().subscribe(subscriptionId(), new RecordedEvents()))
                        .as("this model declares it feeds one subscription, so a second must be refused rather than "
                                + "quietly sharing one delivery between two handlers")
                        .isInstanceOf(IllegalArgumentException.class);
                return;
            }

            String paused = subscriptionId();
            String stillRunning = subscriptionId();
            RecordedEvents pausedRecorded = subscribeAndWait(paused);
            RecordedEvents runningRecorded = subscribeAndWait(stillRunning);

            subscriptionModel().pauseSubscription(paused);
            CloudEvent event = ConformanceEvents.event("1", "NameDefined");
            publish(event);

            assertThat(idsOf(runningRecorded.awaitAtLeast(1, deliveryTimeout())))
                    .as("pausing one subscription says nothing about the others")
                    .containsExactly(event.getId());
            if (!fixture().deliversEventsPublishedWhilePaused()) {
                assertThat(pausedRecorded.soFar())
                        .as("and the paused one, on a model that drops rather than holds, received nothing")
                        .isEmpty();
            }
        }

        @Test
        void stop_leaves_every_running_subscription_paused_and_individually_resumable() {
            String id = subscriptionId();
            RecordedEvents recorded = subscribeAndWait(id);

            subscriptionModel().stop();

            assertThat(subscriptionModel().isRunning())
                    .as("the model itself is no longer running")
                    .isFalse();
            assertThat(subscriptionModel().isPaused(id))
                    .as("a subscription that was running is left paused, which is what lets a caller resume one of "
                            + "them on its own rather than having to start everything")
                    .isTrue();

            subscriptionModel().resumeSubscription(id).waitUntilStarted(deliveryTimeout());
            CloudEvent afterResume = ConformanceEvents.event("1", "NameDefined");
            publish(afterResume);

            assertThat(idsOf(recorded.awaitAtLeast(1, deliveryTimeout())))
                    .as("the resumed subscription delivers again")
                    .containsExactly(afterResume.getId());
        }

        @Test
        void resuming_one_subscription_after_stop_reopens_the_model_but_leaves_the_others_paused() {
            if (!fixture().acceptsSeveralSubscriptions()) {
                // Only one subscription can ever exist, so there is no sibling to leave paused, but the model-wide
                // gate reopening on resume is still this model's claim and stays asserted rather than going unchecked.
                String only = subscriptionId();
                subscribeAndWait(only);

                subscriptionModel().stop();
                subscriptionModel().resumeSubscription(only).waitUntilStarted(deliveryTimeout());

                assertThat(subscriptionModel().isRunning())
                        .as("resumeSubscription(String) reopens the model-wide gate even for a model that only ever "
                                + "has one subscription")
                        .isTrue();
                return;
            }
            String resumed = subscriptionId();
            String stillPaused = subscriptionId();
            RecordedEvents resumedRecorded = subscribeAndWait(resumed);
            RecordedEvents stillPausedRecorded = subscribeAndWait(stillPaused);

            subscriptionModel().stop();

            subscriptionModel().resumeSubscription(resumed).waitUntilStarted(deliveryTimeout());

            assertThat(subscriptionModel().isRunning())
                    .as("resumeSubscription(String) reopens the model-wide gate rather than scoping to the one "
                            + "subscription it resumed, so a caller must not read isRunning() as \"every subscription "
                            + "is going again\"")
                    .isTrue();
            assertThat(subscriptionModel().isPaused(stillPaused))
                    .as("a sibling that stop() paused is untouched by resuming the other one")
                    .isTrue();
            assertThat(subscriptionModel().isRunning(stillPaused))
                    .as("per-subscription reporting still says so, even though the model itself now reports running")
                    .isFalse();

            CloudEvent afterResume = ConformanceEvents.event("1", "NameDefined");
            publish(afterResume);

            assertThat(idsOf(resumedRecorded.awaitAtLeast(1, deliveryTimeout())))
                    .as("the resumed subscription actually delivers again, not just isRunning() reporting so")
                    .containsExactly(afterResume.getId());
            // Unconditional, unlike the deliversEventsPublishedWhilePaused()-guarded checks elsewhere in this class:
            // stillPaused is never resumed in this test, so even a model that holds events for later delivery has
            // nothing to hold them in yet. It is still paused, not merely dropping.
            assertThat(stillPausedRecorded.soFar())
                    .as("the still-paused sibling received nothing, even though the model itself now reports running")
                    .isEmpty();
        }

        @Test
        void start_after_stop_delivers_again() {
            String id = subscriptionId();
            RecordedEvents recorded = subscribeAndWait(id);
            subscriptionModel().stop();

            subscriptionModel().start();

            assertThat(subscriptionModel().isRunning()).isTrue();
            CloudEvent afterStart = ConformanceEvents.event("1", "NameDefined");
            publish(afterStart);
            assertThat(idsOf(recorded.awaitAtLeast(1, deliveryTimeout())))
                    .as("starting a stopped model brings its subscriptions back")
                    .containsExactly(afterStart.getId());
        }

        @Test
        void start_on_a_model_that_is_already_started_is_accepted() {
            String id = subscriptionId();
            RecordedEvents recorded = subscribeAndWait(id);

            subscriptionModel().start();

            assertThat(subscriptionModel().isRunning()).isTrue();
            CloudEvent afterStart = ConformanceEvents.event("1", "NameDefined");
            publish(afterStart);
            assertThat(idsOf(recorded.awaitAtLeast(1, deliveryTimeout())))
                    .as("starting a model that is already started neither fails nor disturbs a running subscription")
                    .containsExactly(afterStart.getId());
        }

        /**
         * {@code start(true)} resumes all subscriptions, so it reaches one that was paused on its own and not by
         * {@code stop()}. This is what makes {@code start()} safe for a caller that cannot see the current state.
         */
        @Test
        void start_on_a_running_model_resumes_a_subscription_paused_on_its_own() {
            String id = subscriptionId();
            RecordedEvents recorded = subscribeAndWait(id);
            subscriptionModel().pauseSubscription(id);

            subscriptionModel().start();

            assertThat(subscriptionModel().isRunning(id)).isTrue();
            CloudEvent afterStart = ConformanceEvents.event("1", "NameDefined");
            publish(afterStart);
            assertThat(idsOf(recorded.awaitAtLeast(1, deliveryTimeout())))
                    .as("the subscription delivers again after start() resumed it")
                    .containsExactly(afterStart.getId());
        }

        /**
         * A model whose {@code stop()}/{@code start()} walk a map of running or paused subscriptions while
         * pausing/resuming each one moves it to the other map can get away with it when there is only one or two
         * subscriptions to move, the way the rest of this class tests it. This one uses several, so an
         * implementation that visits an entry that has already moved, or misses one that has not, has enough of
         * them to make that visible instead of happening to land on the entries it iterates correctly.
         */
        @Test
        // Six subscriptions each waited on twice is twelve chained waits, so this one test's worst case is twelve
        // times the declared budget, 120 seconds at the default. The class timeout is 60, which would fire mid-wait
        // and report a TimeoutException instead of naming what never arrived. Scoped to this method rather than
        // raised on the class: SubscriptionModelConformance has 24 test methods, and a model that hangs on all of
        // them already runs the shard past its 20 minute kill at 60 seconds each, where it produces no report at all.
        @Timeout(150)
        void stop_pauses_and_start_resumes_every_one_of_several_running_subscriptions() {
            if (!fixture().acceptsSeveralSubscriptions()) {
                return;
            }
            Map<String, RecordedEvents> subscriptions = new LinkedHashMap<>();
            for (int i = 0; i < 6; i++) {
                String id = subscriptionId();
                subscriptions.put(id, subscribeAndWait(id));
            }

            subscriptionModel().stop();

            assertThat(subscriptions.keySet())
                    .as("stop() must leave every one of them paused")
                    .allSatisfy(id -> {
                        assertThat(subscriptionModel().isPaused(id)).as("%s is paused", id).isTrue();
                        assertThat(subscriptionModel().isRunning(id)).as("%s is not running", id).isFalse();
                    });

            subscriptionModel().start();

            assertThat(subscriptions.keySet())
                    .as("start() must resume every one of them, for the same reason")
                    .allSatisfy(id -> {
                        assertThat(subscriptionModel().isRunning(id)).as("%s is running again", id).isTrue();
                        assertThat(subscriptionModel().isPaused(id)).as("%s is not paused", id).isFalse();
                    });

            CloudEvent afterStart = ConformanceEvents.event("1", "NameDefined");
            publish(afterStart);
            subscriptions.values().forEach(recorded ->
                    assertThat(idsOf(recorded.awaitAtLeast(1, deliveryTimeout())))
                            .as("and each one actually delivers again, not just reports running")
                            .containsExactly(afterStart.getId()));
        }

        @Test
        void refuses_to_resume_a_subscription_that_is_not_paused() {
            String id = subscriptionId();
            subscribeAndWait(id);

            assertThatThrownBy(() -> subscriptionModel().resumeSubscription(id))
                    .as("resuming something that is already running is a caller mistake, and answering it silently "
                            + "would hide a lifecycle bug in the caller")
                    .isInstanceOf(SubscriptionAlreadyRunningException.class);
        }

        @Test
        void refuses_to_pause_a_subscription_it_does_not_have() {
            String neverSubscribed = subscriptionId();

            assertThatThrownBy(() -> subscriptionModel().pauseSubscription(neverSubscribed))
                    .as("an id this model never had is a different mistake from one it has in the wrong state, and a "
                            + "caller holding several models needs to tell them apart to find the one that owns an id")
                    .isInstanceOf(UnknownSubscriptionException.class);
        }

        @Test
        void refuses_to_pause_a_subscription_that_is_already_paused() {
            String id = subscriptionId();
            subscribeAndWait(id);
            subscriptionModel().pauseSubscription(id);

            assertThatThrownBy(() -> subscriptionModel().pauseSubscription(id))
                    .as("the model has this subscription, so the refusal says it is not running rather than that it "
                            + "does not exist")
                    .isInstanceOf(SubscriptionNotRunningException.class);
        }
    }

    @Nested
    @DisplayName("cancelling")
    class Cancelling {

        @Test
        void a_cancelled_subscription_stops_receiving() {
            String id = subscriptionId();
            RecordedEvents recorded = subscribeAndWait(id);
            CloudEvent beforeCancel = ConformanceEvents.event("1", "NameDefined");
            publish(beforeCancel);
            assertReceives(recorded, beforeCancel);

            subscriptionModel().cancelSubscription(id);

            // A second subscription is what proves the model is still delivering, so the first one's silence means
            // cancelled rather than broken. Where a model feeds only one subscription, cancelling freed the slot.
            RecordedEvents afterCancel = subscribeAndWait(subscriptionId());
            CloudEvent afterCancelEvent = ConformanceEvents.event("2", "NameWasChanged");
            publish(afterCancelEvent);
            if (fixture().replaysHistoryToANewSubscription()) {
                // A replaying model hands this subscription the earlier event before the new one. That is more than
                // the other branch asserts rather than less: the replay has to arrive, and in order.
                List<CloudEvent> received = afterCancel.awaitUntil(
                        events -> idsOf(events).contains(afterCancelEvent.getId()), deliveryTimeout());
                assertThat(idsOf(received))
                        .as("this model replays to a new subscription, so the second one owes the earlier event and "
                                + "then the new one")
                        .containsSubsequence(beforeCancel.getId(), afterCancelEvent.getId());
            } else {
                assertThat(idsOf(afterCancel.awaitAtLeast(1, deliveryTimeout()))).containsExactly(afterCancelEvent.getId());
            }
            assertThat(recorded.soFar())
                    .as("a cancelled subscription receives nothing further")
                    .isEmpty();
        }

        @Test
        void releases_the_subscription_id_for_reuse() {
            String id = subscriptionId();
            subscribeAndWait(id);
            subscriptionModel().cancelSubscription(id);

            RecordedEvents reused = subscribeAndWait(id);

            CloudEvent event = ConformanceEvents.event("1", "NameDefined");
            publish(event);
            assertThat(idsOf(reused.awaitAtLeast(1, deliveryTimeout())))
                    .as("cancelling frees the id, so the same one can be subscribed again and delivers")
                    .containsExactly(event.getId());
        }

        @Test
        void cancelling_a_subscription_it_has_never_seen_does_nothing() {
            String unknown = subscriptionId();

            subscriptionModel().cancelSubscription(unknown);

            assertThat(subscriptionModel().isRunning(unknown)).isFalse();
        }
    }
}
