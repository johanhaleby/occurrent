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

package org.occurrent.subscription.reactor.durable;

import io.cloudevents.CloudEvent;
import org.awaitility.core.ConditionTimeoutException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.DuplicateSubscriptionIdException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.UnknownSubscriptionException;
import org.occurrent.subscription.api.reactor.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.reactor.IntrospectableSubscriptions;
import org.occurrent.subscription.api.reactor.Subscription;
import org.occurrent.subscription.api.reactor.SubscriptionModel;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * A subscribe over a wrapped model that manages named subscriptions itself is refused when asking the wrapped model
 * whether it holds the id fails, and a wrapped model that answers {@link UnknownSubscriptionException} for an id it
 * does not hold does not refuse it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("ReactorDurableSubscriptionModelFailedDuplicateCheck")
class ReactorDurableSubscriptionModelFailedDuplicateCheckTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final String SUBSCRIPTION_ID = "sub";

    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
    private @Nullable ReactorDurableSubscriptionModel model;

    @AfterEach
    void shutdown() {
        if (model != null) {
            model.shutdown();
        }
    }

    @Nested
    @DisplayName("when the first lookup of the id in the wrapped model fails")
    class When_the_first_lookup_of_the_id_in_the_wrapped_model_fails {

        @ParameterizedTest(name = "{0}, {1}")
        @MethodSource("org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelFailedDuplicateCheckTest#everyKindOfWrappedModelAndStartAt")
        @DisplayName("throws IllegalStateException when the wrapped model holds the id and its lookup throws")
        void throws_IllegalStateException_when_the_wrapped_model_holds_the_id_and_its_lookup_throws(Lookup lookup, StartAt startAt) {
            // Given
            IllegalStateException lookupFailed = new IllegalStateException("lookup failed");
            WrappedModel wrapped = wrappedModel(lookup);
            wrapped.held.add(SUBSCRIPTION_ID);
            wrapped.failEveryLookup(lookupFailed);

            // When
            Throwable thrown = catchThrowable(() -> subscribe(startAt));

            // Then
            assertThat(thrown).as("the exception the subscribe threw").isSameAs(lookupFailed);
        }

        @ParameterizedTest(name = "{0}, {1}")
        @MethodSource("org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelFailedDuplicateCheckTest#everyKindOfWrappedModelAndStartAt")
        @DisplayName("does not hand the subscribe to the wrapped model when its lookup throws")
        void does_not_hand_the_subscribe_to_the_wrapped_model_when_its_lookup_throws(Lookup lookup, StartAt startAt) {
            // Given
            WrappedModel wrapped = wrappedModel(lookup);
            wrapped.held.add(SUBSCRIPTION_ID);
            wrapped.failEveryLookup(new IllegalStateException("lookup failed"));

            // When
            catchThrowable(() -> subscribeAndAwaitStart(startAt));

            // Then
            assertThat(wrapped.subscribed).as("subscribes the wrapped model got").isEmpty();
        }

        @ParameterizedTest(name = "{0}, {1}")
        @MethodSource("org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelFailedDuplicateCheckTest#everyKindOfWrappedModelAndStartAt")
        @DisplayName("starts normally when subscribed again after the lookup recovers")
        void starts_normally_when_subscribed_again_after_the_lookup_recovers(Lookup lookup, StartAt startAt) {
            // Given
            WrappedModel wrapped = wrappedModel(lookup);
            wrapped.failEveryLookup(new IllegalStateException("lookup failed"));
            catchThrowable(() -> subscribeAndAwaitStart(startAt));
            wrapped.subscribed.clear();
            wrapped.noLongerFails();

            // When
            Throwable notStarted = catchThrowable(() -> subscribeAndAwaitStart(startAt));

            // Then
            assertAll(
                    () -> assertThat(notStarted).as("why the second subscribe did not start").isNull(),
                    () -> assertThat(wrapped.subscribed).as("subscribes the wrapped model got after the lookup recovered").containsExactly(SUBSCRIPTION_ID)
            );
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelFailedDuplicateCheckTest#everyStartAt")
        @DisplayName("throws UnknownSubscriptionException when the wrapped model lists its ids by throwing it")
        void throws_UnknownSubscriptionException_when_the_wrapped_model_lists_its_ids_by_throwing_it(StartAt startAt) {
            // Given
            UnknownSubscriptionException unknown = new UnknownSubscriptionException(SUBSCRIPTION_ID);
            WrappedModel wrapped = wrappedModel(Lookup.LISTS_ITS_IDS);
            wrapped.failEveryLookup(unknown);

            // When
            Throwable thrown = catchThrowable(() -> subscribe(startAt));

            // Then
            assertThat(thrown).as("the exception the subscribe threw").isSameAs(unknown);
        }
    }

    @Nested
    @DisplayName("when only the second lookup of the id in the wrapped model fails, after the subscription-model default has set up the subscribe")
    class When_only_the_second_lookup_of_the_id_in_the_wrapped_model_fails {

        @ParameterizedTest(name = "{0}")
        @MethodSource("org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelFailedDuplicateCheckTest#everyKindOfWrappedModel")
        @DisplayName("throws IllegalStateException when the lookup after the subscribe is set up throws")
        void throws_IllegalStateException_when_the_lookup_after_the_subscribe_is_set_up_throws(Lookup lookup) {
            // Given
            IllegalStateException lookupFailed = new IllegalStateException("lookup failed");
            WrappedModel wrapped = wrappedModel(lookup);
            wrapped.failOnlyTheLookupNumbered(2, lookupFailed);

            // When
            Throwable thrown = catchThrowable(() -> subscribe(StartAt.subscriptionModelDefault()));

            // Then
            assertThat(thrown).as("the exception the subscribe threw").isSameAs(lookupFailed);
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelFailedDuplicateCheckTest#everyKindOfWrappedModel")
        @DisplayName("does not hand the subscribe to the wrapped model when the lookup after the subscribe is set up throws")
        void does_not_hand_the_subscribe_to_the_wrapped_model_when_the_lookup_after_the_subscribe_is_set_up_throws(Lookup lookup) {
            // Given
            WrappedModel wrapped = wrappedModel(lookup);
            wrapped.failOnlyTheLookupNumbered(2, new IllegalStateException("lookup failed"));

            // When
            catchThrowable(() -> subscribeAndAwaitStart(StartAt.subscriptionModelDefault()));

            // Then
            assertThat(wrapped.subscribed).as("subscribes the wrapped model got").isEmpty();
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelFailedDuplicateCheckTest#everyKindOfWrappedModel")
        @DisplayName("starts normally when subscribed again after the lookup recovers")
        void starts_normally_when_subscribed_again_after_the_lookup_recovers(Lookup lookup) {
            // Given
            WrappedModel wrapped = wrappedModel(lookup);
            wrapped.failOnlyTheLookupNumbered(2, new IllegalStateException("lookup failed"));
            catchThrowable(() -> subscribeAndAwaitStart(StartAt.subscriptionModelDefault()));
            wrapped.subscribed.clear();
            wrapped.noLongerFails();

            // When
            Throwable notStarted = catchThrowable(() -> subscribeAndAwaitStart(StartAt.subscriptionModelDefault()));

            // Then
            assertAll(
                    () -> assertThat(notStarted).as("why the second subscribe did not start").isNull(),
                    () -> assertThat(wrapped.subscribed).as("subscribes the wrapped model got after the lookup recovered").containsExactly(SUBSCRIPTION_ID)
            );
        }

        /**
         * A pause and a resume of the id come while the lookup after the subscribe is set up runs, and that lookup
         * throws, so the subscribe throws and nothing holds the id. The resume then fails with the failure of the
         * lookup, and does not report a start.
         */
        @ParameterizedTest(name = "{0}")
        @MethodSource("org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelFailedDuplicateCheckTest#everyKindOfWrappedModel")
        @DisplayName("a resume made while the lookup after the subscribe is set up throws fails with that failure")
        void a_resume_made_while_the_lookup_after_the_subscribe_is_set_up_throws_fails_with_that_failure(Lookup lookup) throws Exception {
            // Given
            IllegalStateException lookupFailed = new IllegalStateException("lookup failed");
            WrappedModel wrapped = wrappedModel(lookup);
            wrapped.failOnlyTheLookupNumbered(2, lookupFailed);
            wrapped.holdTheLookupNumbered(2);
            ExecutorService caller = Executors.newSingleThreadExecutor();
            try {
                CompletableFuture<Throwable> subscribing = CompletableFuture.supplyAsync(() -> catchThrowable(() -> subscribe(StartAt.subscriptionModelDefault())), caller);
                assertThat(wrapped.lookupHeld.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("lookup after the subscribe is set up held").isTrue();

                // When
                requireModel().pauseSubscription(SUBSCRIPTION_ID);
                Subscription resumed = requireModel().resumeSubscription(SUBSCRIPTION_ID);
                wrapped.lookupReleased.countDown();
                Throwable subscribeThrew = subscribing.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Throwable resumeFailed = catchThrowable(() -> resumed.waitUntilStarted().block(TIMEOUT));

                // Then
                assertAll(
                        () -> assertThat(subscribeThrew).as("the exception the subscribe threw").isSameAs(lookupFailed),
                        () -> assertThat(resumeFailed).as("the exception waiting for the start of the resume ended with").isSameAs(lookupFailed)
                );
            } finally {
                wrapped.lookupReleased.countDown();
                caller.shutdownNow();
            }
        }

        /**
         * A pause of the id comes while the lookup after the subscribe is set up runs, and that lookup finds the id held
         * by the wrapped model, so the subscribe is refused as a duplicate. The pause returned normally, so the wrapped
         * model, which holds the id, gets it.
         */
        @ParameterizedTest(name = "{0}")
        @MethodSource("org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelFailedDuplicateCheckTest#wrappedModelsThatHoldTheIdRunning")
        @DisplayName("a pause made while the lookup after the subscribe is set up finds the id held reaches the wrapped model")
        void a_pause_made_while_the_lookup_after_the_subscribe_is_set_up_finds_the_id_held_reaches_the_wrapped_model(Lookup lookup) throws Exception {
            // Given
            WrappedModel wrapped = wrappedModel(lookup);
            wrapped.holdTheLookupNumbered(2);
            ExecutorService caller = Executors.newSingleThreadExecutor();
            try {
                CompletableFuture<Throwable> subscribing = CompletableFuture.supplyAsync(() -> catchThrowable(() -> subscribe(StartAt.subscriptionModelDefault())), caller);
                assertThat(wrapped.lookupHeld.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).as("lookup after the subscribe is set up held").isTrue();

                // When
                Throwable pauseThrew = catchThrowable(() -> requireModel().pauseSubscription(SUBSCRIPTION_ID));
                wrapped.held.add(SUBSCRIPTION_ID);
                wrapped.lookupReleased.countDown();
                Throwable subscribeThrew = subscribing.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                untilPaused(wrapped);

                // Then
                assertAll(
                        () -> assertThat(subscribeThrew).as("the exception the subscribe threw").isInstanceOf(DuplicateSubscriptionIdException.class),
                        () -> assertThat(pauseThrew).as("the exception the pause threw").isNull(),
                        () -> assertThat(wrapped.paused).as("pauses the wrapped model got").containsExactly(SUBSCRIPTION_ID)
                );
            } finally {
                wrapped.lookupReleased.countDown();
                caller.shutdownNow();
            }
        }
    }

    @Nested
    @DisplayName("when a wrapped model that does not list its ids answers UnknownSubscriptionException for an id it does not hold")
    class When_a_wrapped_model_answers_UnknownSubscriptionException_for_an_id_it_does_not_hold {

        @ParameterizedTest(name = "{0}, {1}")
        @MethodSource("org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModelFailedDuplicateCheckTest#lifecycleOnlyWrappedModelsAndEveryStartAt")
        @DisplayName("starts the subscription")
        void starts_the_subscription(Lookup lookup, StartAt startAt) {
            // Given
            WrappedModel wrapped = wrappedModel(lookup);
            wrapped.failEveryLookup(new UnknownSubscriptionException(SUBSCRIPTION_ID));

            // When
            Throwable notStarted = catchThrowable(() -> subscribeAndAwaitStart(startAt));

            // Then
            assertAll(
                    () -> assertThat(notStarted).as("why the subscribe did not start").isNull(),
                    () -> assertThat(wrapped.subscribed).as("subscribes the wrapped model got").containsExactly(SUBSCRIPTION_ID)
            );
        }
    }

    private WrappedModel wrappedModel(Lookup lookup) {
        WrappedModel wrapped = lookup == Lookup.LISTS_ITS_IDS ? new ListingWrappedModel() : new WrappedModel(lookup);
        model = new ReactorDurableSubscriptionModel(wrapped, storage);
        return wrapped;
    }

    private Subscription subscribe(StartAt startAt) {
        return requireModel().subscribe(SUBSCRIPTION_ID, null, startAt, cloudEvent -> Mono.empty());
    }

    private void subscribeAndAwaitStart(StartAt startAt) {
        subscribe(startAt).waitUntilStarted().block(TIMEOUT);
    }

    // Waits a while for the pause to arrive and returns either way, since the assertion that follows says what arrived
    private static void untilPaused(WrappedModel wrapped) {
        try {
            await().atMost(Duration.ofSeconds(3)).until(() -> !wrapped.paused.isEmpty());
        } catch (ConditionTimeoutException notPaused) {
            // What was paused instead is asserted next
        }
    }

    private ReactorDurableSubscriptionModel requireModel() {
        return requireNonNull(model, "the durable model is created by wrappedModel");
    }

    static Stream<Arguments> everyKindOfWrappedModel() {
        return Stream.of(Lookup.values()).map(Arguments::of);
    }

    // The one that asks isPaused(id) answers a held id paused, so it never holds one running for a pause to reach
    static Stream<Arguments> wrappedModelsThatHoldTheIdRunning() {
        return Stream.of(Lookup.LISTS_ITS_IDS, Lookup.ASKS_IS_RUNNING).map(Arguments::of);
    }

    static Stream<Arguments> everyStartAt() {
        return Stream.of(
                Arguments.of(Named.of("StartAt.now()", StartAt.now())),
                Arguments.of(Named.of("StartAt.subscriptionModelDefault()", StartAt.subscriptionModelDefault())));
    }

    static Stream<Arguments> everyKindOfWrappedModelAndStartAt() {
        return Stream.of(Lookup.values()).flatMap(lookup -> everyStartAt().map(startAt -> Arguments.of(lookup, startAt.get()[0])));
    }

    static Stream<Arguments> lifecycleOnlyWrappedModelsAndEveryStartAt() {
        return Stream.of(Lookup.ASKS_IS_RUNNING, Lookup.ASKS_IS_PAUSED).flatMap(lookup -> everyStartAt().map(startAt -> Arguments.of(lookup, startAt.get()[0])));
    }

    // How the wrapped model is asked whether it holds the id
    private enum Lookup {
        LISTS_ITS_IDS("introspectable, lookup is subscriptionIds()"),
        ASKS_IS_RUNNING("lifecycle only, lookup is isRunning(id)"),
        ASKS_IS_PAUSED("lifecycle only, lookup is isPaused(id)");

        private final String description;

        Lookup(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    // A wrapped model that manages named subscriptions itself and answers a lookup of an id by throwing when told to.
    // A subscribe it gets for an id it holds replaces the subscription, so a subscribe handed over by mistake shows up in
    // subscribed and is not refused by the wrapped model.
    private static class WrappedModel implements CheckpointAwareSubscriptionModel, SubscriptionModel {
        final Set<String> held = ConcurrentHashMap.newKeySet();
        final List<String> subscribed = new CopyOnWriteArrayList<>();
        final List<String> paused = new CopyOnWriteArrayList<>();
        final AtomicInteger lookups = new AtomicInteger();
        final CountDownLatch lookupHeld = new CountDownLatch(1);
        final CountDownLatch lookupReleased = new CountDownLatch(1);
        volatile int heldLookup;
        volatile IntPredicate failsAt = __ -> false;
        volatile RuntimeException failure = new IllegalStateException("no failure set");
        private final Lookup lookup;
        // The number of the lookup by isRunning(id) that the next isPaused(id) answers for
        private final AtomicInteger askedRunning = new AtomicInteger();

        WrappedModel(Lookup lookup) {
            this.lookup = lookup;
        }

        // Only the lookup with that number, counting from 1, throws failure
        void failOnlyTheLookupNumbered(int number, RuntimeException failure) {
            this.failure = failure;
            this.failsAt = lookupNumber -> lookupNumber == number;
        }

        // The lookup with that number, counting from 1, waits in lookupHeld until lookupReleased opens, and then answers
        // or fails as it would have
        void holdTheLookupNumbered(int number) {
            this.heldLookup = number;
        }

        void failEveryLookup(RuntimeException failure) {
            this.failure = failure;
            this.failsAt = __ -> true;
        }

        void noLongerFails() {
            failsAt = __ -> false;
            lookups.set(0);
        }

        void lookedUp() {
            int number = lookups.incrementAndGet();
            askedRunning.set(number);
            if (lookup == Lookup.ASKS_IS_RUNNING) {
                answerLookup(number);
            }
        }

        void answerLookup(int number) {
            if (number == heldLookup) {
                lookupHeld.countDown();
                awaitReleased();
            }
            if (failsAt.test(number)) {
                throw failure;
            }
        }

        private void awaitReleased() {
            try {
                if (!lookupReleased.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new IllegalStateException("Held lookup was not released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            lookedUp();
            return lookup == Lookup.ASKS_IS_RUNNING && held.contains(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            int number = askedRunning.getAndSet(0);
            if (lookup == Lookup.ASKS_IS_PAUSED && number != 0) {
                answerLookup(number);
            }
            return lookup == Lookup.ASKS_IS_PAUSED && held.contains(subscriptionId);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Function<CloudEvent, Mono<Void>> action) {
            subscribed.add(subscriptionId);
            held.add(subscriptionId);
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public Mono<Void> waitUntilStarted() {
                    return Mono.empty();
                }
            };
        }

        @Override
        public Flux<CloudEvent> subscribe(@Nullable SubscriptionFilter filter, StartAt startAt) {
            return Flux.never();
        }

        @Override
        public Mono<Checkpoint> globalCheckpoint() {
            return Mono.just(new StringBasedCheckpoint("1"));
        }

        @Override
        public Mono<Checkpoint> globalCheckpointAsOfNow() {
            return Mono.just(new StringBasedCheckpoint("1"));
        }

        @Override
        public Mono<Void> cancelSubscription(String subscriptionId) {
            held.remove(subscriptionId);
            return Mono.empty();
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            paused.add(subscriptionId);
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public boolean isRunning() {
            return true;
        }
    }

    private static final class ListingWrappedModel extends WrappedModel implements IntrospectableSubscriptions {
        ListingWrappedModel() {
            super(Lookup.LISTS_ITS_IDS);
        }

        @Override
        public Set<String> subscriptionIds() {
            answerLookup(lookups.incrementAndGet());
            return Set.copyOf(held);
        }
    }
}
