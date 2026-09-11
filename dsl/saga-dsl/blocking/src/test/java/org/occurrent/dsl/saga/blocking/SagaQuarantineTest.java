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

package org.occurrent.dsl.saga.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import io.cloudevents.CloudEvent;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.*;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.HistoryRetainingSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscribable;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.SubscriptionModelWrapper;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.slf4j.LoggerFactory;

import io.cloudevents.core.builder.CloudEventBuilder;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * The behaviour <a href="https://github.com/johanhaleby/occurrent/issues/818">#818</a> asked for: one event a saga
 * cannot get through must not stop every other instance sharing the saga's single subscription. That covers an event
 * one instance cannot handle, and, since
 * <a href="https://github.com/johanhaleby/occurrent/issues/997">#997</a>, an event the saga cannot even work out an
 * instance for.
 */
@DisplayName("A saga delivery that keeps failing")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaQuarantineTest {

    private static final String POISON = "poison";
    private static final String HEALTHY = "healthy";
    private static final String TICKING = "ticking";
    private static final String PAYMENT_TIMER = "payment";
    private static final Duration BUDGET = Duration.ofMillis(300);

    private static final SagaRunnerConfig CONFIG = SagaRunnerConfig.defaults()
            .withTimerPollInterval(Duration.ofMillis(50))
            .withQuarantineAfter(BUDGET);

    sealed interface OrderEvent permits OrderPlaced, PaymentReserved {
        String eventId();

        String orderId();
    }

    record OrderPlaced(String eventId, String orderId) implements OrderEvent {
    }

    record PaymentReserved(String eventId, String orderId) implements OrderEvent {
    }

    sealed interface OrderCommand permits ShipOrder, CancelOrder {
    }

    record ShipOrder(String orderId) implements OrderCommand {
    }

    record CancelOrder(String orderId) implements OrderCommand {
    }

    sealed interface OrderState permits AwaitingPayment, Shipped {
    }

    record AwaitingPayment(String orderId) implements OrderState {
    }

    record Shipped(String orderId) implements OrderState {
    }

    // Reading these inside the saga keeps it a single definition rather than several that could drift apart.
    private volatile boolean reactionFails = true;

    /** What reacting to {@link PaymentReserved} throws for {@link #POISON}. The type is what each test is about. */
    private volatile Supplier<? extends Throwable> reactionFailure = () -> new IllegalStateException("this instance can never handle its payment");

    /** The event id the saga cannot correlate, or {@code null} when it correlates every event, which is most tests. */
    private volatile @Nullable String uncorrelatableEventId;

    /** What correlating {@link #uncorrelatableEventId} throws. */
    private volatile Supplier<? extends Throwable> correlationFailure = () -> new IllegalStateException("this event carries no correlation id");

    /** The event id the saga correlates to no instance, which the contract says is skipped rather than failed. */
    private volatile @Nullable String correlatesToNoInstance;

    // Long enough that a timer never fires during a test that is not about timers. The one that is shortens it.
    private volatile Duration paymentTimeout = Duration.ofMinutes(30);

    /** Reacting to {@link PaymentReserved} throws for {@link #POISON} and only for it, for as long as it is broken. */
    private Saga<OrderEvent, OrderState, OrderCommand> orderFulfillment() {
        return Saga.<OrderEvent, OrderState, OrderCommand>builder(null)
                .correlateAll(this::correlate)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new AwaitingPayment(e.orderId()))
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.startTimeout(PAYMENT_TIMER, paymentTimeout)))
                .evolve(PaymentReserved.class, (state, e) -> new Shipped(e.orderId()))
                .react(PaymentReserved.class, (state, e) -> {
                    if (e.orderId().equals(POISON) && reactionFails) {
                        throw raise(reactionFailure.get());
                    }
                    return List.of(SagaEffect.issue(new ShipOrder(e.orderId())), SagaEffect.cancelTimeout(PAYMENT_TIMER));
                })
                .reactOnTimeout(PAYMENT_TIMER, (state, t) -> List.of(SagaEffect.issue(new CancelOrder(t.sagaId()))))
                .isTerminal(state -> state instanceof Shipped)
                .build();
    }

    /**
     * The saga's correlation function, which is what {@code Saga.sagaId} calls on every event before anything else
     * happens to it. It throws for one event id and only for it, which is the shape of a saga whose id extractor reads
     * a correlation field that is null on one old event.
     */
    private String correlate(OrderEvent event) {
        if (event.eventId().equals(uncorrelatableEventId)) {
            throw raise(correlationFailure.get());
        }
        return event.eventId().equals(correlatesToNoInstance) ? null : event.orderId();
    }

    // Throws whatever it is handed, so a test can choose between a RuntimeException and an Error without the saga
    // needing two definitions. Declared as returning so the call site reads as the throw it is.
    private static RuntimeException raise(Throwable failure) {
        if (failure instanceof Error error) {
            throw error;
        }
        throw (RuntimeException) failure;
    }

    private InMemorySubscriptionModel subscriptionModel;
    private InMemoryEventStore eventStore;
    private CloudEventConverter<OrderEvent> converter;
    private SagaStateStore<OrderState> stateStore;
    private final List<OrderCommand> dispatched = new CopyOnWriteArrayList<>();
    private final List<SagaSubscription> subscriptionsToClose = new ArrayList<>();
    private final List<ReplayableSubscriptionModel> modelsToStop = new CopyOnWriteArrayList<>();

    @BeforeEach
    void createInstances() {
        subscriptionModel = new InMemorySubscriptionModel();
        eventStore = new InMemoryEventStore(subscriptionModel);
        converter = new JacksonCloudEventConverter.Builder<OrderEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(OrderEvent::eventId).build();
        stateStore = SagaStateStore.inMemory();
    }

    @AfterEach
    void shutdown() {
        subscriptionsToClose.forEach(SagaSubscription::close);
        // Closing a SagaSubscription stops its timer poller and nothing else, so each fake model's delivery thread has
        // to be stopped here or it keeps waking every 20 ms for the rest of the test JVM.
        modelsToStop.forEach(ReplayableSubscriptionModel::stopDelivering);
        subscriptionModel.shutdown();
    }

    private SagaSubscription run(SagaRunnerConfig config) {
        return run(subscriptionModel, config);
    }

    private SagaSubscription run(Subscribable model, SagaRunnerConfig config) {
        SagaSubscription subscription = SagaRunner.<OrderEvent, OrderCommand>agnostic(model, converter)
                .run("orders", orderFulfillment(), stateStore, dispatched::add, null, config);
        subscriptionsToClose.add(subscription);
        return subscription;
    }

    private void write(String orderId, OrderEvent... events) {
        eventStore.write(orderId, converter.toCloudEvents(List.of(events)));
    }

    @Nested
    class OnASubscriptionSharedWithOtherInstances {

        @Test
        void does_not_stop_the_other_instances_from_processing_the_events_queued_behind_it() {
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));

            // The failing event is pushed first, so the healthy instance's own event sits behind it in the
            // subscription's single ordered channel. Up to 0.33.0 it stayed there for good.
            model.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            model.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(HEALTHY)),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED)
            ));
        }

        @Test
        void does_not_stop_them_either_when_the_event_store_assigns_no_global_position() {
            // The gate at startup passes here, because whether the model can be repositioned is a question about the
            // model and not about what the events carry. Requiring a position in the failure record made quarantine
            // silently inert for exactly this deployment: every retry recorded nothing, so the budget never elapsed.
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(streamOnlyCloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(streamOnlyCloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            model.push(streamOnlyCloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            model.push(streamOnlyCloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(HEALTHY)),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().failure().input()).isEqualTo(POISON + "@2"),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().failure().position()).isNull(),
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED)
            ));
        }

        @Test
        void blocks_them_exactly_as_before_when_the_quarantine_budget_is_switched_off() throws Exception {
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            run(model, CONFIG.withQuarantineAfter(null));
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            model.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            model.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            TimeUnit.SECONDS.sleep(2);

            // The healthy instance's own event is still stuck behind the failing one, which is the bug #818 describes.
            assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY));
        }

        @Test
        void blocks_them_exactly_as_before_on_a_model_that_could_never_replay_the_event_it_stopped_on() throws Exception {
            // InMemorySubscriptionModel implements no RepositionableSubscriptions, so returning normally would
            // acknowledge an event nothing could ever hand back. The runner switches the budget off rather than
            // quarantine into that, which is ADR 134's ruling on a source that cannot replay.
            SagaSubscription subscription = run(subscriptionModel, CONFIG);
            write(POISON, new OrderPlaced("1", POISON));
            write(HEALTHY, new OrderPlaced("2", HEALTHY));
            write(POISON, new PaymentReserved("3", POISON));
            write(HEALTHY, new PaymentReserved("4", HEALTHY));

            TimeUnit.SECONDS.sleep(2);

            assertAll(
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().failure()).isNull(),
                    () -> assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY))
            );
        }

        @Test
        void blocks_them_exactly_as_before_on_a_wrapper_over_a_model_that_retains_nothing() throws Exception {
            // A plain wrapper declares no retention of its own, so the lookup unwraps to the delegate and the delegate
            // decides. The delegate here keeps nothing, which is the answer the whole chain gives.
            SagaSubscription subscription = run(new ForwardingWrapper(subscriptionModel), CONFIG);
            write(POISON, new OrderPlaced("1", POISON));
            write(HEALTHY, new OrderPlaced("2", HEALTHY));
            write(POISON, new PaymentReserved("3", POISON));
            write(HEALTHY, new PaymentReserved("4", HEALTHY));

            TimeUnit.SECONDS.sleep(2);

            assertAll(
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().failure()).isNull(),
                    () -> assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY))
            );
        }

        @Test
        void are_isolated_from_it_on_a_wrapper_whose_delegate_retains_what_it_delivered() {
            ReplayableSubscriptionModel retaining = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(new ForwardingWrapper(retaining), CONFIG);
            retaining.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            retaining.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            retaining.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            retaining.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED)
            ));
        }

        @Test
        void are_isolated_from_it_on_a_model_that_guarantees_it_holds_everything_without_being_repositionable() {
            ReplayableSubscriptionModel feed = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(new RetainsWithoutRepositioning(feed), CONFIG);
            feed.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            feed.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            feed.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            feed.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED)
            ));
        }

        /**
         * A model whose guarantee is wrong is caught on the event it is about to acknowledge. Quarantine was enabled
         * on the guarantee, the check disagreed for this event, and the instance keeps blocking rather than having
         * that event acknowledged away. Distinct from the feed that declares nothing, since here quarantine was
         * available and was refused on the event.
         */
        @Test
        void blocks_them_exactly_as_before_when_the_event_it_stopped_on_cannot_be_obtained_again() throws Exception {
            ReplayableSubscriptionModel feed = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(new GuaranteesMoreThanItHolds(feed), CONFIG);
            feed.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            feed.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            feed.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            feed.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            TimeUnit.SECONDS.sleep(2);

            assertAll(
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY))
            );
        }

        /**
         * A refused instance is re-offered the same event for as long as the source keeps retrying, so the refusal has
         * to be announced once rather than at that cadence. Retention is still rechecked every time, which is what
         * lets a store coming back be noticed.
         */
        @Test
        void says_why_it_refused_once_rather_than_on_every_redelivery() throws Exception {
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            Logger executionLog = (Logger) LoggerFactory.getLogger(SagaExecution.class);
            executionLog.addAppender(appender);
            try {
                ReplayableSubscriptionModel feed = new ReplayableSubscriptionModel();
                SagaSubscription subscription = run(new GuaranteesMoreThanItHolds(feed), CONFIG);
                feed.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
                feed.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));

                TimeUnit.SECONDS.sleep(3);

                long refusals = appender.list.stream()
                        .map(ILoggingEvent::getFormattedMessage)
                        .filter(message -> message.contains("is not quarantined"))
                        .count();
                assertAll(
                        () -> assertThat(refusals).isEqualTo(1),
                        () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE)
                );
            } finally {
                executionLog.detachAppender(appender);
                appender.stop();
            }
        }

        /**
         * A retention check can fail rather than answer, and an answer nobody got is not a yes. The refusal warning says
         * as much already, so the throw has to reach that refusal instead of the silent one every store failure produces.
         */
        @Test
        void blocks_them_and_says_why_when_the_retention_check_itself_throws() throws Exception {
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            Logger executionLog = (Logger) LoggerFactory.getLogger(SagaExecution.class);
            executionLog.addAppender(appender);
            try {
                ReplayableSubscriptionModel feed = new ReplayableSubscriptionModel();
                SagaSubscription subscription = run(new ThrowsWhenAskedAboutAnEvent(feed, Integer.MAX_VALUE), CONFIG);
                feed.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
                feed.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
                feed.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
                feed.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

                TimeUnit.SECONDS.sleep(2);

                List<ILoggingEvent> refusals = appender.list.stream()
                        .filter(event -> event.getFormattedMessage().contains("is not quarantined"))
                        .toList();
                assertAll(
                        () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                        () -> assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY)),
                        () -> assertThat(refusals).hasSize(1),
                        // What the saga threw is still the exception the log reports, with what the check threw under it,
                        // so an operator reading the refusal sees both rather than only the one that stopped the instance.
                        () -> assertThat(refusals.getFirst().getThrowableProxy().getClassName())
                                .isEqualTo(IllegalStateException.class.getName()),
                        () -> assertThat(refusals.getFirst().getThrowableProxy().getSuppressed())
                                .extracting(IThrowableProxy::getMessage).contains("the retention read is broken")
                );
            } finally {
                executionLog.detachAppender(appender);
                appender.stop();
            }
        }

        @Test
        void are_isolated_from_it_once_the_retention_check_can_answer_again() {
            // Retention is rechecked on every redelivery rather than remembered, which is what lets a store that was
            // unreachable and is now back be noticed. The instance quarantines on a later attempt.
            ReplayableSubscriptionModel feed = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(new ThrowsWhenAskedAboutAnEvent(feed, 2), CONFIG);
            feed.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            feed.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            feed.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            feed.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(HEALTHY))
            ));
        }

        /**
         * One extension that cannot be read must not discard a redelivery key the event does carry. Reading the three
         * together threw on the position before either stream value reached the record, so an event with a perfectly
         * good stream id and version looked like one carrying no key at all and nothing could ever budget it.
         */
        /**
         * Every catch under the delivery's own swallows what it caught and rethrows the original failure, so an
         * {@code OutOfMemoryError} raised by a recovery step would be absorbed and the saga's own exception reported
         * instead. That is the carve-out defeated one level down, and it would let the instance's budget keep running
         * while the process is out of heap.
         */
        @Test
        void lets_an_out_of_memory_error_out_of_the_retention_check_rather_than_absorbing_it() throws Exception {
            ReplayableSubscriptionModel feed = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(new ThrowsWhenAskedAboutAnEvent(feed, Integer.MAX_VALUE, () -> new OutOfMemoryError("Java heap space")), CONFIG);
            feed.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            feed.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));

            TimeUnit.SECONDS.sleep(2);

            assertAll(
                    () -> assertThat(feed.lastDeliveryFailure).isInstanceOf(OutOfMemoryError.class),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE)
            );
        }

        @Test
        void are_isolated_from_it_when_the_event_carries_a_position_that_is_not_a_number() {
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(unreadablePositionCloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(unreadablePositionCloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            model.push(unreadablePositionCloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            model.push(unreadablePositionCloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(HEALTHY)),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    // The stream key, since the position written beside it could not be read.
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().failure().input()).isEqualTo(POISON + "@2"),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().failure().position()).isNull(),
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED)
            ));
        }

        /**
         * An {@code Error} out of a reaction stops the instance for exactly as long as a {@code RuntimeException} does,
         * so it is the instance's failure in the only sense that matters here. A recursive {@code evolve} raising
         * {@link StackOverflowError} is the likeliest way a saga produces one.
         */
        @Test
        void are_isolated_from_it_when_its_reaction_throws_an_error_rather_than_an_exception() {
            reactionFailure = StackOverflowError::new;
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            model.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            model.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(HEALTHY)),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().failure().failureType()).isEqualTo(StackOverflowError.class.getName()),
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED)
            ));
        }

        /**
         * Running out of heap says something about the process, not about the instance holding the thread when it
         * happened, and any other instance running then would have met the same thing. Nothing in 0.34.0 releases an
         * instance from quarantine, so charging it would cost an arbitrary instance its state for a condition it had
         * nothing to do with.
         */
        @Test
        void blocks_them_exactly_as_before_when_the_jvm_ran_out_of_memory() throws Exception {
            reactionFailure = () -> new OutOfMemoryError("Java heap space");
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            model.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            model.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            TimeUnit.SECONDS.sleep(2);

            assertAll(
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().failure()).isNull(),
                    () -> assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY))
            );
        }

        /**
         * The carve-out reads what was thrown rather than what it wraps, and this test is what would fail if that were
         * changed, so the next reader can tell it was decided. Walking the cause chain is the alternative and it is
         * worse. An unrelated {@code OutOfMemoryError} buried under a genuinely broken instance's own failure would
         * exempt that instance forever, and a cause chain has no length limit and can be cyclic. A reaction that catches
         * one and wraps it has said the failure is its own, and it is taken at its word.
         */
        @Test
        void are_isolated_from_it_when_its_reaction_wraps_the_out_of_memory_error_in_its_own_exception() {
            reactionFailure = () -> new IllegalStateException("could not build the shipment", new OutOfMemoryError("Java heap space"));
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            model.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            model.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(HEALTHY)),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED)
            ));
        }

        @Test
        void blocks_them_exactly_as_before_on_a_feed_that_retains_nothing() throws Exception {
            ReplayableSubscriptionModel feed = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(new RetainsNothing(feed), CONFIG);
            feed.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            feed.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            feed.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            feed.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            TimeUnit.SECONDS.sleep(2);

            assertAll(
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().failure()).isNull(),
                    () -> assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY))
            );
        }
    }

    /**
     * <a href="https://github.com/johanhaleby/occurrent/issues/997">#997</a>. The saga asks for the instance id before
     * anything else happens to an event, so an id extractor that throws used to take the whole delivery down with
     * nothing recorded and nothing quarantined, and every instance of the saga waited behind the redelivery forever.
     * There is no instance to quarantine here, since the event reached none, so the budget is the delivery's own and
     * the subscription is let past it when that runs out.
     */
    @Nested
    @DisplayName("that the saga cannot work out an instance for")
    class ThatTheSagaCannotWorkOutAnInstanceFor {

        @BeforeEach
        void letTheReactionsSucceed() {
            // Nothing but the routing fails in these, so an outcome can only be about the routing.
            reactionFails = false;
        }

        private void pushTheHealthyEventBehindTheUncorrelatableOne(ReplayableSubscriptionModel model) {
            model.push(cloudEvent(HEALTHY, 1, new OrderPlaced("1", HEALTHY)));
            model.push(cloudEvent(POISON, 1, new OrderPlaced("2", POISON)));
            model.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));
            model.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));
        }

        @Test
        void does_not_stop_the_instances_whose_events_are_queued_behind_it() {
            uncorrelatableEventId = "3";
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            pushTheHealthyEventBehindTheUncorrelatableOne(model);

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(HEALTHY)),
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED),
                    // Nothing is quarantined and nothing is recorded, because the event reached no instance. The one
                    // that exists is the one the saga could correlate earlier, and it is untouched by this.
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().failure()).isNull()
            ));
        }

        @Test
        void does_not_stop_them_either_when_the_id_extractor_throws_an_error_rather_than_an_exception() {
            uncorrelatableEventId = "3";
            correlationFailure = StackOverflowError::new;
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            pushTheHealthyEventBehindTheUncorrelatableOne(model);

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED));
        }

        @Test
        void does_not_stop_them_either_when_it_is_the_converter_that_cannot_read_the_event() {
            converter = new CannotRead<>(converter, "3");
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            pushTheHealthyEventBehindTheUncorrelatableOne(model);

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(HEALTHY)),
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED)
            ));
        }

        /**
         * The budget is what separates a converter that is briefly unwell from one that will never read this event. A
         * schema registry down for thirty seconds is the plain case, and skipping on the first failure would drop every
         * event delivered while it was out.
         */
        @Test
        void is_not_skipped_when_the_saga_can_correlate_it_again_inside_the_budget() {
            uncorrelatableEventId = "3";
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            pushTheHealthyEventBehindTheUncorrelatableOne(model);
            uncorrelatableEventId = null;

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    // The event nobody could correlate is handled rather than skipped, so POISON is shipped too.
                    () -> assertThat(dispatched).containsExactlyInAnyOrder(new ShipOrder(POISON), new ShipOrder(HEALTHY)),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED)
            ));
        }

        /**
         * The routing budget ends when routing works, whatever the id turns out to be, so a later failure of the same
         * event starts a fresh one. Clearing it only after the whole delivery succeeded left the entry behind for an
         * event that correlated to no instance and for one whose instance quarantined, and a replay of that event then
         * inherited an elapsed budget and was skipped on its first failure with no budget at all.
         */
        @Test
        void is_given_a_fresh_budget_rather_than_an_elapsed_one_when_the_same_event_fails_to_correlate_again() throws Exception {
            uncorrelatableEventId = "3";
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            Logger executionLog = (Logger) LoggerFactory.getLogger(SagaExecution.class);
            executionLog.addAppender(appender);
            try {
                ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
                run(model, CONFIG);
                model.push(cloudEvent(HEALTHY, 1, new OrderPlaced("1", HEALTHY)));
                model.push(cloudEvent(POISON, 2, new PaymentReserved("3", POISON)));

                // The budget only exists once the routing has actually failed once, which the first-failure warning is
                // the signal for. Flipping the flag before that means no entry was ever written to go stale.
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(appender.list)
                        .anyMatch(event -> event.getFormattedMessage().contains("could not work out which instance")));

                // Correlates again, to no instance, which returns rather than processing anything. That is the case
                // that used to leave the entry behind, since only a delivery that got all the way through cleared it.
                uncorrelatableEventId = null;
                correlatesToNoInstance = "3";
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(model.acknowledged()).isEqualTo(2));

                // Past the budget before the event is offered again, which is what makes a surviving entry an elapsed
                // one rather than merely a stale one. Nothing is failing during this wait.
                TimeUnit.MILLISECONDS.sleep(BUDGET.toMillis() * 3);

                uncorrelatableEventId = "3";
                correlatesToNoInstance = null;
                model.rewindTo(1);
                model.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

                // Well inside a fresh budget, so the event is still being offered and everything behind it still waits.
                // An entry that survived would already be past its budget and the event would be skipped at once.
                TimeUnit.MILLISECONDS.sleep(BUDGET.toMillis() / 2);
                assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY));

                // And the fresh budget does run out, so this is a delay rather than a block.
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(dispatched).containsExactly(new ShipOrder(HEALTHY)));
            } finally {
                executionLog.detachAppender(appender);
                appender.stop();
            }
        }

        @Test
        void blocks_them_exactly_as_before_when_the_quarantine_budget_is_switched_off() throws Exception {
            uncorrelatableEventId = "3";
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            run(model, CONFIG.withQuarantineAfter(null));
            pushTheHealthyEventBehindTheUncorrelatableOne(model);

            TimeUnit.SECONDS.sleep(2);

            assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY));
        }

        @Test
        void blocks_them_exactly_as_before_on_a_feed_that_retains_nothing() throws Exception {
            uncorrelatableEventId = "3";
            ReplayableSubscriptionModel feed = new ReplayableSubscriptionModel();
            run(new RetainsNothing(feed), CONFIG);
            pushTheHealthyEventBehindTheUncorrelatableOne(feed);

            TimeUnit.SECONDS.sleep(2);

            assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY));
        }

        /**
         * Letting the subscription past acknowledges the event, so a model that guaranteed retention and then says no
         * for this event is refused, exactly as it is for a quarantine. The refusal is said once rather than at the
         * cadence the feed re-offers it.
         */
        @Test
        void blocks_them_exactly_as_before_when_the_event_cannot_be_obtained_again_and_says_why_once() throws Exception {
            uncorrelatableEventId = "3";
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            Logger executionLog = (Logger) LoggerFactory.getLogger(SagaExecution.class);
            executionLog.addAppender(appender);
            try {
                ReplayableSubscriptionModel feed = new ReplayableSubscriptionModel();
                run(new GuaranteesMoreThanItHolds(feed), CONFIG);
                pushTheHealthyEventBehindTheUncorrelatableOne(feed);

                TimeUnit.SECONDS.sleep(3);

                long refusals = appender.list.stream()
                        .map(ILoggingEvent::getFormattedMessage)
                        .filter(message -> message.contains("is not being let past it"))
                        .count();
                assertAll(
                        () -> assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY)),
                        () -> assertThat(refusals).isEqualTo(1)
                );
            } finally {
                executionLog.detachAppender(appender);
                appender.stop();
            }
        }

        /**
         * A skipped delivery is logged rather than recorded, because there is no instance to write a row on, so the log
         * line is all an operator gets. It names the event and the exception that stopped it.
         */
        @Test
        void says_what_it_skipped_and_what_stopped_it() {
            uncorrelatableEventId = "3";
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            Logger executionLog = (Logger) LoggerFactory.getLogger(SagaExecution.class);
            executionLog.addAppender(appender);
            try {
                ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
                SagaSubscription subscription = run(model, CONFIG);
                pushTheHealthyEventBehindTheUncorrelatableOne(model);

                await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED));

                List<ILoggingEvent> skips = appender.list.stream()
                        .filter(event -> event.getFormattedMessage().contains("is now being let past it"))
                        .toList();
                assertAll(
                        () -> assertThat(skips).hasSize(1),
                        () -> assertThat(skips.getFirst().getFormattedMessage()).contains(POISON + "@2"),
                        () -> assertThat(skips.getFirst().getThrowableProxy().getClassName()).isEqualTo(IllegalStateException.class.getName())
                );
            } finally {
                executionLog.detachAppender(appender);
                appender.stop();
            }
        }

        /**
         * Without a stream id and version or a global position there is nothing to tell one delivery of this event from
         * the next, so the budget can never elapse and the delivery keeps blocking. That is the same condition an
         * instance's quarantine turns on, which is the point. The conditions do not change with what failed.
         */
        @Test
        void blocks_them_exactly_as_before_when_the_event_carries_no_redelivery_key() throws Exception {
            uncorrelatableEventId = "3";
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            run(model, CONFIG.withRedeliveryDetection(RedeliveryDetection.BEST_EFFORT));
            model.push(converter.toCloudEvent(new OrderPlaced("1", HEALTHY)));
            model.push(converter.toCloudEvent(new PaymentReserved("3", POISON)));
            model.push(converter.toCloudEvent(new PaymentReserved("4", HEALTHY)));

            TimeUnit.SECONDS.sleep(2);

            assertThat(dispatched).doesNotContain(new ShipOrder(HEALTHY));
        }
    }

    @Nested
    class WhenItIsQuarantined {

        @Test
        void records_where_it_stopped_and_what_it_was_failing_with() {
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(POISON, 2, new PaymentReserved("2", POISON)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                SagaFailure failure = subscription.instances().find(POISON).orElseThrow().failure();
                assertAll(
                        () -> assertThat(failure).isNotNull(),
                        () -> assertThat(failure.failureType()).isEqualTo(IllegalStateException.class.getName()),
                        () -> assertThat(failure.failureMessage()).isEqualTo("this instance can never handle its payment"),
                        // The second event pushed onto this feed, so global position 2.
                        () -> assertThat(failure.position()).isEqualTo(2)
                );
            });
        }

        @Test
        void is_found_by_enumerating_the_quarantined_status_rather_than_the_active_one() {
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(POISON, 2, new PaymentReserved("2", POISON)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(subscription.instances().findByStatus(SagaStatus.QUARANTINED, farFuture(), 10))
                            .extracting(SagaInstance::sagaId).containsExactly(POISON),
                    () -> assertThat(subscription.instances().findByStatus(SagaStatus.ACTIVE, farFuture(), 10)).isEmpty()
            ));
        }

        @Test
        void fires_none_of_its_own_timers_while_the_poller_keeps_firing_everyone_else_s() {
            // Comfortably longer than the quarantine budget, so the poison instance is already quarantined by the time
            // its own timer comes due, which is the state this test is about.
            paymentTimeout = Duration.ofMillis(800);
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(TICKING, 1, new OrderPlaced("3", TICKING)));
            model.push(cloudEvent(POISON, 2, new PaymentReserved("2", POISON)));

            // The ticking instance's timeout proves the poller is alive and firing, so the quarantined instance's own
            // armed-and-overdue timer staying silent is the quarantine and not a stalled poller.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(dispatched).contains(new CancelOrder(TICKING)));
            assertThat(dispatched).doesNotContain(new CancelOrder(POISON));
        }
    }

    @Nested
    class WhenItsStateCanNoLongerBeDecoded {

        private final RefusesToDecodeOneInstance store = new RefusesToDecodeOneInstance();

        @BeforeEach
        void loadTheStoreThatCannotDecode() {
            // The saga itself handles everything here. What fails is loading the instance, which is the one failure that
            // used to leave no failure record behind, so the budget never ran out and the instance blocked forever.
            reactionFails = false;
            stateStore = store;
        }

        /**
         * The load-failure catch inside {@code process} answers whether this input would have been skipped anyway, and a
         * yes there returns normally, which acknowledges the event. A load that failed because the process is out of
         * heap says nothing about whether the input would have been skipped, so answering on that basis would
         * acknowledge the event with no sign anything went wrong. This is the fourth place a recovery step could
         * absorb a failure of the JVM, and it was found by listing every catch in the file rather than by reading.
         */
        @Test
        void an_out_of_memory_error_loading_a_quarantined_instance_is_not_swallowed_by_the_skip_it_would_have_taken() throws Exception {
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            await().atMost(Duration.ofSeconds(10)).until(() -> subscription.instances().find(POISON).isPresent());

            // Quarantine it first, so wouldHaveSkippedThisInput would answer yes for whatever comes next.
            reactionFails = true;
            model.push(cloudEvent(POISON, 2, new PaymentReserved("2", POISON)));
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED));

            // Now the load itself runs out of heap for that instance.
            reactionFails = false;
            store.failsToLoadWith(() -> new OutOfMemoryError("Java heap space"));
            store.cannotDecodeTheStateOf(POISON);
            model.push(cloudEvent(POISON, 3, new PaymentReserved("3", POISON)));

            TimeUnit.SECONDS.sleep(2);

            assertThat(model.lastDeliveryFailure).isInstanceOf(OutOfMemoryError.class);
        }

        @Test
        void the_instance_is_quarantined_and_the_other_instances_keep_going() {
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            await().atMost(Duration.ofSeconds(10)).until(() -> subscription.instances().find(POISON).isPresent());

            store.cannotDecodeTheStateOf(POISON);
            model.push(cloudEvent(POISON, 2, new PaymentReserved("2", POISON)));
            model.push(cloudEvent(HEALTHY, 1, new OrderPlaced("3", HEALTHY)));
            model.push(cloudEvent(HEALTHY, 2, new PaymentReserved("4", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(HEALTHY))
            ));
        }

        @Test
        void the_failure_record_names_the_event_the_instance_stopped_on_and_what_loading_it_threw() {
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            await().atMost(Duration.ofSeconds(10)).until(() -> subscription.instances().find(POISON).isPresent());

            store.cannotDecodeTheStateOf(POISON);
            model.push(cloudEvent(POISON, 2, new PaymentReserved("2", POISON)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                SagaFailure failure = subscription.instances().find(POISON).orElseThrow().failure();
                assertAll(
                        () -> assertThat(failure).isNotNull(),
                        () -> assertThat(failure.input()).isEqualTo(POISON + "@2"),
                        () -> assertThat(failure.failureMessage()).contains("can no longer be decoded")
                );
            });
        }

        @Test
        void the_state_the_instance_stopped_on_is_still_there_afterwards() {
            // It is what somebody repairs the converter for, so the write that suspends the instance must not be what
            // destroys it. The envelope that write carries holds no state at all, having been read without one.
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            await().atMost(Duration.ofSeconds(10)).until(() -> subscription.instances().find(POISON).isPresent());

            store.cannotDecodeTheStateOf(POISON);
            model.push(cloudEvent(POISON, 2, new PaymentReserved("2", POISON)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(store.theStoredStateOf(POISON)).isEqualTo(new AwaitingPayment(POISON))
            ));
        }

        @Test
        void a_later_event_for_the_quarantined_instance_does_not_block_the_channel_either() {
            // Loading it keeps failing after the quarantine, and an instance that is already suspended has to be skipped
            // on that failure rather than retried on it, or every event behind it stays where it is for good.
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            await().atMost(Duration.ofSeconds(10)).until(() -> subscription.instances().find(POISON).isPresent());
            store.cannotDecodeTheStateOf(POISON);
            model.push(cloudEvent(POISON, 2, new PaymentReserved("2", POISON)));
            await().atMost(Duration.ofSeconds(10)).until(() ->
                    subscription.instances().find(POISON).orElseThrow().status() == SagaStatus.QUARANTINED);

            model.push(cloudEvent(POISON, 3, new PaymentReserved("3", POISON)));
            model.push(cloudEvent(HEALTHY, 1, new OrderPlaced("4", HEALTHY)));
            model.push(cloudEvent(HEALTHY, 2, new PaymentReserved("5", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(dispatched).containsExactly(new ShipOrder(HEALTHY)));
        }

        @Test
        void a_redelivery_of_an_event_it_has_already_handled_does_not_block_the_channel_either() {
            // The instance is active and has not reached its budget, so neither the completed nor the quarantined answer
            // applies, and yet its watermarks already cover this event. Loading it is what failed, so without the
            // watermark answer the skip that was always going to happen turns into a permanent block on a replay.
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            await().atMost(Duration.ofSeconds(10)).until(() -> subscription.instances().find(POISON).isPresent());

            store.cannotDecodeTheStateOf(POISON);
            // The same stream id and version the instance has already folded, which is what a subscription replay after a
            // restart delivers, on a fresh global position.
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(HEALTHY, 1, new OrderPlaced("2", HEALTHY)));
            model.push(cloudEvent(HEALTHY, 2, new PaymentReserved("3", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(dispatched).containsExactly(new ShipOrder(HEALTHY)),
                    // Still active, so the channel moved on without the instance being quarantined for an event it had
                    // already handled.
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().failure()).isNull()
            ));
        }

        @Test
        void a_later_event_for_a_completed_instance_does_not_block_the_channel_either() {
            // A completed instance skips every event addressed to it too, and it is the one most likely to still be
            // around with a state nobody can decode, because completed instances are kept rather than deleted.
            ReplayableSubscriptionModel model = new ReplayableSubscriptionModel();
            SagaSubscription subscription = run(model, CONFIG);
            model.push(cloudEvent(POISON, 1, new OrderPlaced("1", POISON)));
            model.push(cloudEvent(POISON, 2, new PaymentReserved("2", POISON)));
            await().atMost(Duration.ofSeconds(10)).until(() ->
                    subscription.instances().find(POISON).orElseThrow().status() == SagaStatus.COMPLETED);

            store.cannotDecodeTheStateOf(POISON);
            model.push(cloudEvent(POISON, 3, new PaymentReserved("3", POISON)));
            model.push(cloudEvent(HEALTHY, 1, new OrderPlaced("4", HEALTHY)));
            model.push(cloudEvent(HEALTHY, 2, new PaymentReserved("5", HEALTHY)));

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertAll(
                    () -> assertThat(subscription.instances().find(HEALTHY).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(subscription.instances().find(POISON).orElseThrow().status()).isEqualTo(SagaStatus.COMPLETED)
            ));
        }
    }

    /**
     * An in-memory store that refuses to hand over one instance with its state, which is what a store whose converter no
     * longer understands what it wrote does. {@code findWithoutState} answers, because answering it decodes no state at
     * all, and {@code compareAndSaveWithoutState} keeps the stored state instead of taking it from the envelope. Those
     * are the two things {@link org.occurrent.dsl.saga.SagaStateStore} asks a store to override together, and the
     * MongoDB store does them with a projection and a {@code findAndModify}.
     */
    private static final class RefusesToDecodeOneInstance implements SagaStateStore<OrderState>, SagaStateStoreQueries<OrderState> {

        private final SagaStateStore<OrderState> delegate = SagaStateStore.inMemory();
        private final Set<String> undecodable = ConcurrentHashMap.newKeySet();

        private volatile Supplier<? extends Throwable> loadFailure = () -> new IllegalStateException("the state can no longer be decoded");

        void cannotDecodeTheStateOf(String sagaId) {
            undecodable.add(sagaId);
        }

        void failsToLoadWith(Supplier<? extends Throwable> failure) {
            loadFailure = failure;
        }

        @Nullable OrderState theStoredStateOf(String sagaId) {
            return delegate.find(sagaId).map(SagaEnvelope::state).orElse(null);
        }

        @Override
        public Optional<SagaEnvelope<OrderState>> find(String sagaId) {
            Optional<SagaEnvelope<OrderState>> found = delegate.find(sagaId);
            if (found.isPresent() && undecodable.contains(sagaId)) {
                throw raise(loadFailure.get());
            }
            return found;
        }

        @Override
        public Optional<SagaEnvelope<OrderState>> findWithoutState(String sagaId) {
            return delegate.find(sagaId).map(envelope -> withState(envelope, null));
        }

        @Override
        public boolean compareAndSave(String sagaId, SagaEnvelope<OrderState> envelope, long expectedVersion) {
            return delegate.compareAndSave(sagaId, envelope, expectedVersion);
        }

        @Override
        public boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<OrderState> envelope, long expectedVersion) {
            return delegate.compareAndSave(sagaId, withState(envelope, theStoredStateOf(sagaId)), expectedVersion);
        }

        @Override
        public List<SagaEnvelope<OrderState>> findWithDueTimers(Instant now, int limit) {
            return delegate.findWithDueTimers(now, limit);
        }

        @Override
        public List<SagaEnvelope<OrderState>> findByStatus(SagaStatus status, Instant updatedBefore, int limit) {
            return ((SagaStateStoreQueries<OrderState>) delegate).findByStatus(status, updatedBefore, limit);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }

        private static SagaEnvelope<OrderState> withState(SagaEnvelope<OrderState> envelope, @Nullable OrderState state) {
            return new SagaEnvelope<>(envelope.sagaId(), state, envelope.status(), envelope.version(), envelope.timers(),
                    envelope.streamWatermarks(), envelope.positionWatermark(), envelope.createdAt(), envelope.updatedAt(),
                    envelope.completedAt(), envelope.currentStep(), envelope.started(), envelope.failure());
        }
    }

    /**
     * A wrapper that declares no retention of its own, so a lookup for it unwraps to the delegate and the delegate
     * answers. It does declare {@link RepositionableSubscriptions}, which is what the wrappers a saga actually runs
     * behind do, meaning {@code CompetingConsumerSubscriptionModel} and {@code DurableSubscriptionModel}.
     */
    private record ForwardingWrapper(SubscriptionModel delegate)
            implements Subscribable, SubscriptionModelWrapper, RepositionableSubscriptions {

        @Override
        public SubscriptionModel getWrappedSubscriptionModel() {
            return delegate;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            return RepositionableSubscriptions.findIn(delegate)
                    .orElseThrow(() -> new UnsupportedOperationException(delegate.getClass().getSimpleName() + " is not repositionable"))
                    .resumeSubscription(subscriptionId, startAt);
        }
    }

    /**
     * Retains what it delivers while refusing to resume at a chosen position, which is the combination the old gate
     * turned away. Deliberately not a {@link SubscriptionModelWrapper}, so the lookup answers from here and never
     * reaches the delegate's repositioning.
     */
    private record RetainsWithoutRepositioning(ReplayableSubscriptionModel delegate)
            implements Subscribable, HistoryRetainingSubscriptions {

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public boolean retains(CloudEvent event) {
            return true;
        }

        @Override
        public boolean retainsEveryEvent() {
            return true;
        }
    }

    /**
     * Claims to hold everything and then answers no for the event it is asked about, which is a model whose guarantee
     * is wrong. The runner only enables quarantine on the guarantee, so this is the one way the per-event check is
     * still reached, and it is why that check is made rather than trusted.
     */
    private record GuaranteesMoreThanItHolds(ReplayableSubscriptionModel delegate)
            implements Subscribable, HistoryRetainingSubscriptions {

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public boolean retains(CloudEvent event) {
            return false;
        }

        @Override
        public boolean retainsEveryEvent() {
            return true;
        }
    }

    /**
     * Guarantees it holds everything and then throws when asked about an event, which is a model whose retention read is
     * broken rather than one whose answer is no. It throws for the first {@code throwForTheFirst} questions and answers
     * yes after that, so one test can watch the refusal and another can watch the recovery.
     */
    private static final class ThrowsWhenAskedAboutAnEvent implements Subscribable, HistoryRetainingSubscriptions {

        private final ReplayableSubscriptionModel delegate;
        private final int throwForTheFirst;
        private final AtomicInteger asked = new AtomicInteger();

        private final Supplier<? extends Throwable> checkFailure;

        private ThrowsWhenAskedAboutAnEvent(ReplayableSubscriptionModel delegate, int throwForTheFirst) {
            this(delegate, throwForTheFirst, () -> new IllegalStateException("the retention read is broken"));
        }

        private ThrowsWhenAskedAboutAnEvent(ReplayableSubscriptionModel delegate, int throwForTheFirst, Supplier<? extends Throwable> checkFailure) {
            this.delegate = delegate;
            this.throwForTheFirst = throwForTheFirst;
            this.checkFailure = checkFailure;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }

        @Override
        public boolean retains(CloudEvent event) {
            if (asked.incrementAndGet() <= throwForTheFirst) {
                throw raise(checkFailure.get());
            }
            return true;
        }

        @Override
        public boolean retainsEveryEvent() {
            return true;
        }
    }

    /**
     * Delivers and re-offers exactly as {@link RetainsWithoutRepositioning} does and keeps nothing, which is what a
     * push feed is. Paired with that one deliberately, since the two differ in retention and in nothing else, so a
     * difference in outcome can only be retention.
     */
    private record RetainsNothing(ReplayableSubscriptionModel delegate) implements Subscribable {

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            return delegate.subscribe(subscriptionId, filter, startAt, action);
        }
    }

    /**
     * A converter that cannot read one event, which is what a renamed event class, or a payload its mapper chokes on,
     * looks like from the runner's side. Everything else goes through untouched.
     */
    private record CannotRead<T>(CloudEventConverter<T> delegate, String unreadableCloudEventId) implements CloudEventConverter<T> {

        @Override
        public CloudEvent toCloudEvent(T domainEvent) {
            return delegate.toCloudEvent(domainEvent);
        }

        @Override
        public T toDomainEvent(CloudEvent cloudEvent) {
            if (cloudEvent.getId().equals(unreadableCloudEventId)) {
                throw new IllegalStateException("no class is registered for this event type any more");
            }
            return delegate.toDomainEvent(cloudEvent);
        }

        @Override
        public String getCloudEventType(Class<? extends T> type) {
            return delegate.getCloudEventType(type);
        }
    }

    private static Instant farFuture() {
        return Instant.now().plus(Duration.ofDays(1));
    }

    // The stream and position extensions an event store writes, added by hand because this feed is a plain list. Without
    // them the runner refuses the event outright, since it could not tell a redelivery from a new event.
    private CloudEvent cloudEvent(String streamId, long streamVersion, OrderEvent event) {
        position.incrementAndGet();
        return OccurrentCloudEventExtension.withPosition(streamOnlyCloudEvent(streamId, streamVersion, event), position.get());
    }

    /**
     * What an event store that assigns no global position writes: the stream extensions and nothing else. A store built
     * with {@code withoutStreamPosition()} feeds a saga events of this shape, and so does an upgraded deployment whose
     * existing collection left stream position disabled.
     */
    private CloudEvent streamOnlyCloudEvent(String streamId, long streamVersion, OrderEvent event) {
        return CloudEventBuilder.v1(converter.toCloudEvent(event))
                .withExtension(OccurrentCloudEventExtension.occurrent(streamId, streamVersion))
                .build();
    }

    /**
     * What a feed that writes the stream extensions correctly and the position badly delivers. The event still has a
     * redelivery key, its stream id with its version, so reading the position must not take that key with it.
     */
    private CloudEvent unreadablePositionCloudEvent(String streamId, long streamVersion, OrderEvent event) {
        return CloudEventBuilder.v1(streamOnlyCloudEvent(streamId, streamVersion, event))
                .withExtension(OccurrentCloudEventExtension.POSITION, "not-a-number")
                .build();
    }

    private final java.util.concurrent.atomic.AtomicLong position = new java.util.concurrent.atomic.AtomicLong();

    /**
     * A subscription model that keeps every event it is handed, which is what the runner requires before it turns
     * quarantine on and which {@link InMemorySubscriptionModel} does not do. Deliberately minimal, meaning one
     * subscription, one delivery thread, and a retry loop that keeps re-offering an event whose handler threw, which is
     * what makes a time budget reachable at all.
     */
    private final class ReplayableSubscriptionModel implements SubscriptionModel, RepositionableSubscriptions, HistoryRetainingSubscriptions {

        @Override
        public boolean retains(CloudEvent event) {
            return true;
        }

        @Override
        public boolean retainsEveryEvent() {
            return true;
        }


        // What the handler last threw back at the feed, which is what the transport sees and decides on.
        private volatile @Nullable Throwable lastDeliveryFailure;

        private final List<CloudEvent> log = new CopyOnWriteArrayList<>();
        private volatile @Nullable Consumer<CloudEvent> action;
        private volatile @Nullable String subscriptionId;
        private volatile boolean running;
        // The 0-based index of the next event to deliver, which is the 1-based position of the previous one.
        private volatile int nextIndex;
        private volatile @Nullable Thread deliverer;

        void stopDelivering() {
            running = false;
            Thread thread = deliverer;
            if (thread != null) {
                thread.interrupt();
            }
        }

        void push(CloudEvent event) {
            log.add(event);
        }

        int acknowledged() {
            return nextIndex;
        }

        // Offer an event the feed has already moved past, which is what a repositioned subscription or a catch-up does.
        void rewindTo(int index) {
            nextIndex = index;
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            this.subscriptionId = subscriptionId;
            this.action = action;
            this.running = true;
            Thread thread = new Thread(this::deliver, "replayable-" + subscriptionId);
            thread.setDaemon(true);
            thread.start();
            this.deliverer = thread;
            // Registered where the thread is actually created, so teardown stops it without every test remembering to.
            modelsToStop.add(this);
            return new ReplayableSubscription(subscriptionId);
        }

        private void deliver() {
            while (!Thread.currentThread().isInterrupted()) {
                Consumer<CloudEvent> current = action;
                if (!running || current == null || nextIndex >= log.size()) {
                    sleepBriefly();
                    continue;
                }
                try {
                    current.accept(log.get(nextIndex));
                    nextIndex++;
                } catch (Throwable e) {
                    lastDeliveryFailure = e;
                    // Left where it is, so the same event is offered again. That is what every transport this design
                    // works on does, and it is what lets a failure last long enough to reach the budget. Throwable
                    // rather than RuntimeException because RetryExecution, which is what the MongoDB models re-offer
                    // through, catches Throwable, so a feed that died on an Error would be a feed no model is.
                    sleepBriefly();
                }
            }
        }

        private void sleepBriefly() {
            try {
                TimeUnit.MILLISECONDS.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            nextIndex = (int) GlobalCheckpoint.positionOf(((StartAt.StartAtCheckpoint) startAt).checkpoint);
            running = true;
            return new ReplayableSubscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            running = true;
            return new ReplayableSubscription(subscriptionId);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            running = false;
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            running = false;
            action = null;
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return running && subscriptionId.equals(this.subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return !isRunning(subscriptionId);
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
            running = true;
        }

        @Override
        public void stop() {
            running = false;
        }

        private record ReplayableSubscription(String id) implements Subscription {
            @Override
            public boolean waitUntilStarted(Duration timeout) {
                return true;
            }
        }
    }
}
