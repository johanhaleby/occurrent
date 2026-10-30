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

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.saga.HeldEvent;
import org.occurrent.dsl.saga.HeldEventRef;
import org.occurrent.dsl.saga.InMemorySagaStateStore;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEffect;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaHeldEvents;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.ItemAdded;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OpenLedger;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.Order;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OrderCommand;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OrderEvent;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.OrderPlaced;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.RecordingDispatcher;
import org.occurrent.dsl.saga.blocking.HeldOrderSaga.Reserve;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.ORDER;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.STREAM;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.itemAt;
import static org.occurrent.dsl.saga.blocking.HeldOrderSaga.placedAt;

/**
 * On a feed that does not declare commit order, an event below the start's version on the start's own stream is held and
 * waits for a start below it. These tests drive the executor and an in-memory store the way a delivery does, and look
 * at the reactions that ran, the held index and the instance status.
 */
@DisplayName("A held copy that waits for a start below it")
@DisplayNameGeneration(ReplaceUnderscores.class)
class SagaWaitingHeldCopyTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String OTHER_STREAM = "other-stream";
    private static final String BOOK = "book";
    private static final String DONE = "done";
    private static final String THIRD_STREAM = "third-stream";

    private final ScriptedStore<Order> store = new ScriptedStore<>();
    private final RecordingDispatcher dispatcher = new RecordingDispatcher();
    private final List<Throwable> refused = new CopyOnWriteArrayList<>();
    private final AtomicReference<Instant> clock = new AtomicReference<>(NOW);
    private SagaExecution<OrderEvent, Order, OrderCommand> execution;

    private void start(SagaRunnerConfig config) {
        start(HeldOrderSaga.saga(), config);
    }

    private void start(Saga<OrderEvent, Order, OrderCommand> saga, SagaRunnerConfig config) {
        execution = executionOf(saga, config, HeldOrderSaga.converter(), true);
    }

    private SagaExecution<OrderEvent, Order, OrderCommand> executionOf(Saga<OrderEvent, Order, OrderCommand> saga, SagaRunnerConfig config,
                                                                       CloudEventConverter<OrderEvent> converter, boolean holds) {
        return new SagaExecution<>("orders", saga, store, dispatcher, converter, config, event -> true, clock::get, holds);
    }

    // A feed that does not declare commit order is one whose deliveries hold, and one that does is not.
    private void startOnAFeedInCommitOrder(SagaRunnerConfig config) {
        execution = executionOf(HeldOrderSaga.saga(), config, HeldOrderSaga.converter(), false);
    }

    // The failure is returned and also kept, so a test can say that every delivery was acknowledged.
    private @Nullable Throwable deliver(CloudEvent event) {
        Throwable thrown = catchThrowable(() -> execution.onCloudEvent(event));
        if (thrown != null) {
            refused.add(thrown);
        }
        return thrown;
    }

    private long reactionsTo(String item) {
        return dispatcher.issued().stream().filter(command -> command instanceof Reserve reserve && reserve.item().equals(item)).count();
    }

    private long ledgersOpened() {
        return dispatcher.issued().stream().filter(command -> command instanceof OpenLedger).count();
    }

    private List<String> heldKeys() {
        return store.heldIndex(ORDER).stream().map(HeldEventRef::key).sorted().toList();
    }

    private SagaStatus statusOfTheInstance() {
        return store.find(ORDER).orElseThrow().status();
    }

    private @Nullable Instant failingSinceOf(String key) {
        return store.heldIndex(ORDER).stream().filter(ref -> ref.key().equals(key)).findFirst().orElseThrow().failingSince();
    }

    private void passes(Duration time) {
        clock.updateAndGet(instant -> instant.plus(time));
    }

    private static CloudEvent itemOn(String streamId, long streamVersion, String item) {
        CloudEvent plain = HeldOrderSaga.converter().toCloudEvent(new ItemAdded("item-" + item, ORDER, item));
        return CloudEventBuilder.v1(plain).withExtension(OccurrentCloudEventExtension.occurrent(streamId, streamVersion)).build();
    }

    // The same saga as the shared one, and an item named done completes the instance.
    private static Saga<OrderEvent, Order, OrderCommand> sagaCompletingOnDone() {
        return Saga.<OrderEvent, Order, OrderCommand>builder(null)
                .correlateAll(OrderEvent::orderId)
                .startsOn(OrderPlaced.class)
                .evolve(OrderPlaced.class, (state, e) -> new Order(List.of()))
                .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.orderId()))))
                .evolve(ItemAdded.class, (state, e) -> state.withItem(e.item()))
                .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                .isTerminal(order -> order != null && order.items().contains(DONE))
                .build();
    }

    @Nested
    @DisplayName("on a feed that does not declare commit order")
    class WaitRule {

        @Test
        void an_item_below_a_start_that_arrives_after_it_waits_and_runs_once_when_a_start_below_it_arrives() {
            // Given
            start(SagaRunnerConfig.defaults());
            deliver(itemAt(4, BOOK));
            deliver(placedAt(5));
            long reactionsWhileWaiting = reactionsTo(BOOK);
            List<String> heldWhileWaiting = heldKeys();

            // When
            deliver(placedAt(3));

            // Then
            assertAll(
                    () -> assertThat(refused).as("every delivery was acknowledged").isEmpty(),
                    () -> assertThat(reactionsWhileWaiting).as("the item reaction while only the start at 5 has been applied").isZero(),
                    () -> assertThat(heldWhileWaiting).as("the copy is still held while only the start at 5 has been applied").containsExactly(STREAM + "@4"),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction after the start at 3").isEqualTo(1),
                    () -> assertThat(heldKeys()).as("the copy after the start at 3").isEmpty()
            );
        }

        @Test
        void an_item_held_after_the_start_waits_and_runs_once_when_a_start_below_it_arrives() {
            // Given
            start(SagaRunnerConfig.defaults());
            deliver(placedAt(5));
            deliver(itemAt(4, BOOK));
            long reactionsWhileWaiting = reactionsTo(BOOK);
            List<String> heldWhileWaiting = heldKeys();

            // When
            deliver(placedAt(3));

            // Then
            assertAll(
                    () -> assertThat(refused).as("every delivery was acknowledged").isEmpty(),
                    () -> assertThat(reactionsWhileWaiting).as("the item reaction while only the start at 5 has been applied").isZero(),
                    () -> assertThat(heldWhileWaiting).as("the copy is held while only the start at 5 has been applied").containsExactly(STREAM + "@4"),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction after the start at 3").isEqualTo(1),
                    () -> assertThat(heldKeys()).as("the copy after the start at 3").isEmpty()
            );
        }

        @Test
        void an_item_delivered_after_both_starts_with_the_lower_start_applied_first_runs_once() {
            // Given
            start(SagaRunnerConfig.defaults());
            deliver(placedAt(5));
            deliver(placedAt(3));

            // When
            deliver(itemAt(4, BOOK));

            // Then
            assertAll(
                    () -> assertThat(refused).as("every delivery was acknowledged").isEmpty(),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction").isEqualTo(1),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty()
            );
        }

        @Test
        void a_start_below_the_item_that_commits_after_the_item_held_and_read_the_instance_applies_the_copy_once() {
            // Given
            start(SagaRunnerConfig.defaults());
            deliver(placedAt(5));
            AtomicBoolean raced = new AtomicBoolean();
            AtomicReference<Throwable> hookFailure = new AtomicReference<>();
            Runnable startBelow = () -> {
                raced.set(true);
                hookFailure.set(catchThrowable(() -> execution.onCloudEvent(placedAt(3))));
            };
            // The hold has inserted, and the next read of the instance still sees only the start at 5.
            store.once(ScriptedStore.Point.AFTER_HOLD, () -> store.once(ScriptedStore.Point.AFTER_INSTANCE_READ, startBelow));

            // When
            deliver(itemAt(4, BOOK));

            // Then
            assertAll(
                    () -> assertThat(raced).as("the start at 3 committed between the item's hold and its re-read of the instance").isTrue(),
                    () -> assertThat(hookFailure.get()).as("the start at 3 delivered inside the window was acknowledged").isNull(),
                    () -> assertThat(refused).as("every delivery was acknowledged").isEmpty(),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction").isEqualTo(1),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty()
            );
        }

        @Test
        void a_start_below_the_item_whose_list_of_held_events_runs_before_the_item_holds_applies_the_copy_once() {
            // Given
            start(SagaRunnerConfig.defaults());
            deliver(placedAt(5));
            AtomicBoolean raced = new AtomicBoolean();
            AtomicReference<Throwable> hookFailure = new AtomicReference<>();
            Runnable startBelow = () -> {
                raced.set(true);
                hookFailure.set(catchThrowable(() -> execution.onCloudEvent(placedAt(3))));
            };
            // The item has read the instance, and its hold has not inserted yet.
            store.once(ScriptedStore.Point.BEFORE_HOLD, startBelow);

            // When
            deliver(itemAt(4, BOOK));

            // Then
            assertAll(
                    () -> assertThat(raced).as("the start at 3 committed and listed the held events before the item's hold inserted").isTrue(),
                    () -> assertThat(hookFailure.get()).as("the start at 3 delivered inside the window was acknowledged").isNull(),
                    () -> assertThat(refused).as("every delivery was acknowledged").isEmpty(),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction").isEqualTo(1),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty()
            );
        }

        @Test
        void an_item_whose_start_never_arrives_never_reacts_and_completing_the_instance_releases_the_copy() {
            // Given
            start(sagaCompletingOnDone(), SagaRunnerConfig.defaults());
            deliver(placedAt(5));
            deliver(itemAt(4, BOOK));

            // When
            deliver(itemAt(6, DONE));

            // Then
            assertAll(
                    () -> assertThat(refused).as("every delivery was acknowledged").isEmpty(),
                    () -> assertThat(statusOfTheInstance()).as("the instance status").isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(reactionsTo(DONE)).as("the reaction of the event that completes the instance").isEqualTo(1),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction").isZero(),
                    () -> assertThat(heldKeys()).as("the held index after the instance completed").isEmpty()
            );
        }

        @Test
        void an_item_on_another_stream_held_before_the_start_runs_once_when_the_start_arrives_without_waiting() {
            // Given
            start(SagaRunnerConfig.defaults());
            deliver(itemOn(OTHER_STREAM, 2, BOOK));
            List<String> heldBeforeTheStart = heldKeys();

            // When
            deliver(placedAt(5));

            // Then
            assertAll(
                    () -> assertThat(refused).as("every delivery was acknowledged").isEmpty(),
                    () -> assertThat(heldBeforeTheStart).as("the copy is held before the start").containsExactly(OTHER_STREAM + "@2"),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction after the start").isEqualTo(1),
                    () -> assertThat(heldKeys()).as("the held index after the start").isEmpty()
            );
        }

        @Test
        void a_start_below_the_waiting_copies_that_races_a_lower_start_leaves_the_lowest_start_and_applies_each_copy_once() {
            // Given
            start(SagaRunnerConfig.defaults());
            deliver(placedAt(5));
            deliver(itemAt(4, "c4"));
            deliver(itemAt(2, "c2"));
            AtomicBoolean raced = new AtomicBoolean();
            AtomicReference<Throwable> hookFailure = new AtomicReference<>();
            // The start at 1 has read the instance, and the start at 3 commits before the start at 1 saves.
            store.once(ScriptedStore.Point.AFTER_INSTANCE_READ, () -> {
                raced.set(true);
                hookFailure.set(catchThrowable(() -> execution.onCloudEvent(placedAt(3))));
            });

            // When
            Throwable lowest = deliver(placedAt(1));

            // Then
            assertAll(
                    () -> assertThat(raced).as("the start at 3 committed between the read and the save of the start at 1").isTrue(),
                    () -> assertThat(hookFailure.get()).as("the start at 3 was acknowledged").isNull(),
                    () -> assertThat(lowest).as("the start at 1 is acknowledged").isNull(),
                    () -> assertThat(store.find(ORDER).orElseThrow().startMark().version()).as("the start of the instance").isEqualTo(1),
                    () -> assertThat(reactionsTo("c4")).as("the reaction of the copy at 4").isEqualTo(1),
                    () -> assertThat(reactionsTo("c2")).as("the reaction of the copy at 2").isEqualTo(1),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty()
            );
        }

        @Test
        void a_delivery_on_another_stream_that_read_the_start_before_a_lower_start_applied_the_waiting_copy_does_not_run_the_copy_again() {
            // Given
            start(SagaRunnerConfig.defaults());
            deliver(placedAt(5));
            deliver(itemAt(4, "c4"));
            AtomicBoolean raced = new AtomicBoolean();
            AtomicReference<Throwable> hookFailure = new AtomicReference<>();
            // The delivery has read the start at 5, and the start at 3 commits and applies the copy before it saves.
            store.once(ScriptedStore.Point.AFTER_INSTANCE_READ, () -> {
                raced.set(true);
                hookFailure.set(catchThrowable(() -> execution.onCloudEvent(placedAt(3))));
            });

            // When
            Throwable other = deliver(itemOn(OTHER_STREAM, 1, "other"));

            // Then
            assertAll(
                    () -> assertThat(raced).as("the start at 3 committed between the read and the save of the delivery").isTrue(),
                    () -> assertThat(hookFailure.get()).as("the start at 3 was acknowledged").isNull(),
                    () -> assertThat(other).as("the delivery is acknowledged").isNull(),
                    () -> assertThat(reactionsTo("c4")).as("the reaction of the copy at 4").isEqualTo(1),
                    () -> assertThat(reactionsTo("other")).as("the reaction of the delivery, which dispatches again when its save is lost").isGreaterThanOrEqualTo(1),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the process stops between the save that applies a held copy and the release of it")
    class CrashBetweenTheApplySaveAndTheRelease {

        private static final String FAR = "far";

        // The copy at 2 is held before the start, the start applies it in its save, and the release of it stops once.
        private void appliedCopyWhoseReleaseStopped() {
            deliver(itemAt(2, BOOK));
            store.failNextRelease();
            deliver(placedAt(1));
            assertThat(store.releasesThatStopped()).as("the release of the applied copy stopped once").isEqualTo(1);
            assertThat(reactionsTo(BOOK)).as("the copy was applied by its save").isEqualTo(1);
        }

        @Test
        void a_delivery_of_the_same_type_beyond_the_default_window_releases_the_copy_without_applying_it_again() {
            // Given
            start(SagaRunnerConfig.defaults());
            appliedCopyWhoseReleaseStopped();

            // When
            deliver(itemAt(302, FAR));

            // Then
            assertAll(
                    () -> assertThat(heldKeys()).as("the held index").isEmpty(),
                    () -> assertThat(reactionsTo(BOOK)).as("the reaction of the copy").isEqualTo(1),
                    () -> assertThat(reactionsTo(FAR)).as("the reaction of the delivery 300 versions ahead").isEqualTo(1)
            );
        }

        @Test
        void a_delivery_of_another_type_300_versions_ahead_releases_the_copy_without_applying_it_again() {
            // Given
            start(SagaRunnerConfig.defaults());
            appliedCopyWhoseReleaseStopped();

            // When
            deliver(placedAt(302));

            // Then
            assertAll(
                    () -> assertThat(heldKeys()).as("the held index").isEmpty(),
                    () -> assertThat(reactionsTo(BOOK)).as("the reaction of the copy").isEqualTo(1),
                    () -> assertThat(ledgersOpened()).as("the ledgers opened by the start at 1 and the start at 302").isEqualTo(2)
            );
        }

        @Test
        void a_delivery_of_the_same_type_one_small_window_ahead_releases_the_copy_without_applying_it_again() {
            // Given
            start(SagaRunnerConfig.defaults().withReorderWindow(2));
            appliedCopyWhoseReleaseStopped();

            // When
            deliver(itemAt(4, FAR));

            // Then
            assertAll(
                    () -> assertThat(heldKeys()).as("the held index").isEmpty(),
                    () -> assertThat(reactionsTo(BOOK)).as("the reaction of the copy").isEqualTo(1),
                    () -> assertThat(reactionsTo(FAR)).as("the reaction of the delivery beyond the small window").isEqualTo(1)
            );
        }

        @Test
        void a_copy_applied_behind_the_window_whose_release_keeps_stopping_is_not_applied_again_and_does_not_hold_back_the_deliveries_after_it() {
            // Given
            start(SagaRunnerConfig.defaults().withReorderWindow(2));
            deliver(placedAt(1));
            for (long version = 5; version <= 8; version++) {
                deliver(itemOn(OTHER_STREAM, version, "t" + version));
            }
            CloudEvent old = itemOn(OTHER_STREAM, 1, "old");
            store.delegate.hold(ORDER, new HeldEvent(ORDER, new HeldEventRef(OTHER_STREAM + "@1", OTHER_STREAM, 1L, null, old.getType(), clock.get(), false, null), old));
            store.failNextRelease();
            store.failNextRelease();

            // When
            Throwable firstRelease = deliver(itemAt(2, "item2"));
            long reactionsToOldAfterTheFirst = reactionsTo("old");
            Throwable secondRelease = deliver(itemOn(OTHER_STREAM, 9, "t9"));
            long reactionsToT9AfterTheSecond = reactionsTo("t9");
            Throwable redelivered = deliver(itemOn(OTHER_STREAM, 9, "t9"));
            Throwable next = deliver(itemOn(OTHER_STREAM, 10, "t10"));

            // Then
            assertAll(
                    () -> assertThat(firstRelease).as("the first release stopped, so the delivery is refused").isNotNull(),
                    () -> assertThat(reactionsToOldAfterTheFirst).as("the copy was applied by the save before the release").isEqualTo(1),
                    () -> assertThat(secondRelease).as("the second release stopped, so the delivery is refused").isNotNull(),
                    () -> assertThat(reactionsToT9AfterTheSecond).as("nothing of the delivery ran while the release stops").isZero(),
                    () -> assertThat(redelivered).as("the redelivery is acknowledged").isNull(),
                    () -> assertThat(next).as("the next delivery is acknowledged").isNull(),
                    () -> assertThat(reactionsTo("old")).as("the reaction of the copy after the releases").isEqualTo(1),
                    () -> assertThat(reactionsTo("t9")).as("the reaction of the refused delivery").isEqualTo(1),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when the save that applies a held copy loses to another writer")
    class LostApplySave {

        // The save of another writer that read the instance before the copy was held, so it does not apply the copy.
        private void anotherWriterSavesWithoutTheCopy() {
            SagaEnvelope<Order> read = store.delegate.find(ORDER).orElseThrow();
            SagaEnvelope<Order> bumped = new SagaEnvelope<>(read.sagaId(), read.state(), read.status(), read.version() + 1, read.timers(), read.streamWatermarks(),
                    read.positionWatermark(), read.createdAt(), read.updatedAt(), read.completedAt(), read.currentStep(), read.started(), read.failure(),
                    read.streamMarks(), read.startMark());
            assertThat(store.delegate.compareAndSave(ORDER, bumped, read.version())).as("the other writer saved").isTrue();
        }

        private List<String> itemsOfTheInstance() {
            return store.find(ORDER).orElseThrow().state().items();
        }

        @Test
        void the_copy_is_not_released_and_is_applied_once_by_the_next_delivery() {
            // Given
            start(SagaRunnerConfig.defaults());
            deliver(placedAt(5));
            deliver(itemAt(4, BOOK));
            AtomicBoolean raced = new AtomicBoolean();
            // The start at 3 saves first, and the save that applies the copy at 4 is the second one.
            store.once(ScriptedStore.Point.BEFORE_COMPARE_AND_SAVE, () -> store.once(ScriptedStore.Point.BEFORE_COMPARE_AND_SAVE, () -> {
                raced.set(true);
                anotherWriterSavesWithoutTheCopy();
            }));

            // When
            Throwable lower = deliver(placedAt(3));
            List<String> heldAfterTheLostSave = heldKeys();
            List<String> itemsAfterTheLostSave = itemsOfTheInstance();
            Throwable next = deliver(itemAt(6, "next"));

            // Then
            assertAll(
                    () -> assertThat(raced).as("the other writer saved just before the apply save").isTrue(),
                    () -> assertThat(lower).as("the start at 3 is acknowledged").isNull(),
                    () -> assertThat(next).as("the next delivery is acknowledged").isNull(),
                    () -> assertThat(itemsAfterTheLostSave).as("the saved items, after the lost save").doesNotContain(BOOK),
                    () -> assertThat(heldAfterTheLostSave).as("the held index while the copy is unapplied").containsExactly(STREAM + "@4"),
                    () -> assertThat(itemsOfTheInstance()).as("the saved items, after the next delivery").containsExactly(BOOK, "next"),
                    () -> assertThat(heldKeys()).as("the held index once the copy is applied").isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when a keyed delivery meets a failing held copy and the source parks it")
    class KeyedDriverWithAFailingCopy {

        private static final String T1 = "T1";

        @Test
        void the_refused_delivery_runs_once_when_the_source_replays_it_after_the_copy_is_fixed() {
            // Given
            start(SagaRunnerConfig.defaults().withReorderWindow(2));
            dispatcher.failReservationOf(T1);
            deliver(itemOn(OTHER_STREAM, 1, T1));
            deliver(placedAt(1));

            // When
            Throwable refusedForTheCopy = deliver(itemAt(2, "item2"));
            dispatcher.stopFailingReservationOf(T1);
            Throwable third = deliver(itemAt(3, "item3"));
            Throwable fourth = deliver(itemAt(4, "item4"));
            Throwable fifth = deliver(itemAt(5, "item5"));
            deliver(itemAt(2, "item2"));
            long reactionsAfterTheReplay = reactionsTo("item2");
            deliver(itemAt(2, "item2"));
            long reactionsAfterAFurtherDelivery = reactionsTo("item2");

            // Then
            assertAll(
                    () -> assertThat(refusedForTheCopy).as("item 2 is refused for the failure of T1").isNotNull().hasRootCause(dispatcher.failure),
                    () -> assertThat(third).as("item 3 is acknowledged").isNull(),
                    () -> assertThat(fourth).as("item 4 is acknowledged").isNull(),
                    () -> assertThat(fifth).as("item 5 is acknowledged").isNull(),
                    () -> assertThat(reactionsAfterTheReplay).as("the reaction of item 2 after the replay").isEqualTo(1),
                    () -> assertThat(reactionsAfterAFurtherDelivery).as("the reaction of item 2 after one more delivery of it").isEqualTo(1)
            );
        }
    }

    @Nested
    @DisplayName("when a held copy keeps failing while other deliveries meet it")
    class CopyClock {

        private static final String T1 = "T1";
        private final Duration budget = Duration.ofMinutes(5);

        private void instanceWithAFailingCopyOfAnotherStream(SagaRunnerConfig config) {
            start(config.withReorderWindow(2).withQuarantineAfter(budget));
            dispatcher.failReservationOf(T1);
            deliver(itemOn(OTHER_STREAM, 1, T1));
            deliver(placedAt(1));
        }

        @Test
        void the_copy_is_charged_from_the_first_sweep_that_failed_and_a_second_driver_does_not_move_it() {
            // Given
            instanceWithAFailingCopyOfAnotherStream(SagaRunnerConfig.defaults());
            Instant firstFailure = failingSinceOf(OTHER_STREAM + "@1");
            passes(Duration.ofMinutes(1));

            // When
            Throwable first = deliver(itemAt(2, "item2"));
            passes(Duration.ofMinutes(1));
            Throwable second = deliver(itemAt(3, "item3"));

            // Then
            assertAll(
                    () -> assertThat(firstFailure).as("the clock of the copy once the start swept it").isNotNull(),
                    () -> assertThat(first).as("the first driver is refused for the copy").isNotNull().hasRootCause(dispatcher.failure),
                    () -> assertThat(second).as("the second driver is refused for the copy").isNotNull().hasRootCause(dispatcher.failure),
                    () -> assertThat(failingSinceOf(OTHER_STREAM + "@1")).as("the clock of the copy after two other drivers").isEqualTo(firstFailure),
                    () -> assertThat(statusOfTheInstance()).as("the instance status inside the budget").isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(store.find(ORDER).orElseThrow().failure()).as("no failure record for the copy inside the budget").isNull()
            );
        }

        @Test
        void a_refused_driver_is_kept_as_a_plain_held_copy_that_has_no_clock_and_its_reaction_waits_for_the_copy() {
            // Given
            instanceWithAFailingCopyOfAnotherStream(SagaRunnerConfig.defaults());

            // When
            Throwable refusedDriver = deliver(itemAt(2, "item2"));

            // Then
            assertAll(
                    () -> assertThat(refusedDriver).as("the driver is refused for the copy").isNotNull().hasRootCause(dispatcher.failure),
                    () -> assertThat(heldKeys()).as("the copy and the driver").containsExactly(STREAM + "@2", OTHER_STREAM + "@1"),
                    () -> assertThat(failingSinceOf(STREAM + "@2")).as("the clock of the driver").isNull(),
                    () -> assertThat(reactionsTo("item2")).as("the reaction of the driver while the copy fails").isZero()
            );
        }

        @Test
        void a_kept_driver_is_applied_by_the_sweep_of_the_next_delivery_while_the_copy_still_fails() {
            // Given
            instanceWithAFailingCopyOfAnotherStream(SagaRunnerConfig.defaults());
            deliver(itemAt(2, "item2"));

            // When
            deliver(itemAt(3, "item3"));

            // Then
            assertAll(
                    () -> assertThat(reactionsTo("item2")).as("the reaction of the first driver").isEqualTo(1),
                    () -> assertThat(reactionsTo("item3")).as("the reaction of the second driver").isZero(),
                    () -> assertThat(heldKeys()).as("the copy that fails and the second driver").containsExactly(STREAM + "@3", OTHER_STREAM + "@1")
            );
        }

        @Test
        void the_instance_is_quarantined_at_the_key_of_the_copy_across_several_deliveries_once_its_budget_has_run_out() {
            // Given
            instanceWithAFailingCopyOfAnotherStream(SagaRunnerConfig.defaults());
            deliver(itemAt(2, "item2"));
            passes(budget.minusSeconds(1));
            Throwable justInside = deliver(itemAt(3, "item3"));
            passes(Duration.ofSeconds(2));

            // When
            Throwable pastTheBudget = deliver(itemAt(4, "item4"));

            // Then
            SagaEnvelope<Order> instance = store.find(ORDER).orElseThrow();
            assertAll(
                    () -> assertThat(justInside).as("the delivery one second inside the budget is refused").isNotNull(),
                    () -> assertThat(pastTheBudget).as("the delivery past the budget is acknowledged").isNull(),
                    () -> assertThat(instance.status()).as("the instance status").isEqualTo(SagaStatus.QUARANTINED),
                    () -> assertThat(instance.failure()).as("the failure record").isNotNull().satisfies(failure -> assertThat(failure.input()).isEqualTo(OTHER_STREAM + "@1")),
                    () -> assertThat(instance.failure().firstFailedAt()).as("the record is dated at the clock of the copy").isEqualTo(failingSinceOf(OTHER_STREAM + "@1")),
                    () -> assertThat(heldKeys()).as("the copy stays held for an operator").contains(OTHER_STREAM + "@1")
            );
        }

        @Test
        void a_driver_that_cannot_be_kept_is_refused_and_the_instance_is_not_quarantined_even_past_the_budget() {
            // Given
            instanceWithAFailingCopyOfAnotherStream(SagaRunnerConfig.defaults());
            deliver(itemAt(2, "item2"));
            passes(budget.plusSeconds(1));
            store.once(ScriptedStore.Point.BEFORE_HOLD, () -> {
                throw new IllegalStateException("the hold of the driver stopped");
            });

            // When
            Throwable driverThatCannotBeKept = deliver(itemAt(3, "item3"));

            // Then
            assertAll(
                    () -> assertThat(driverThatCannotBeKept).as("the delivery is refused").isNotNull(),
                    () -> assertThat(statusOfTheInstance()).as("the instance status").isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(heldKeys()).as("the driver was not kept").doesNotContain(STREAM + "@3")
            );
        }

        @Test
        void the_delivery_that_follows_a_driver_that_could_not_be_kept_quarantines_the_instance_past_the_budget() {
            // Given
            instanceWithAFailingCopyOfAnotherStream(SagaRunnerConfig.defaults());
            deliver(itemAt(2, "item2"));
            passes(budget.plusSeconds(1));
            store.once(ScriptedStore.Point.BEFORE_HOLD, () -> {
                throw new IllegalStateException("the hold of the driver stopped");
            });
            deliver(itemAt(3, "item3"));

            // When
            Throwable next = deliver(itemAt(3, "item3"));

            // Then
            assertAll(
                    () -> assertThat(next).as("the delivery is acknowledged").isNull(),
                    () -> assertThat(statusOfTheInstance()).as("the instance status").isEqualTo(SagaStatus.QUARANTINED)
            );
        }

        @Test
        void a_copy_that_another_delivery_applied_while_the_sweep_was_failing_on_it_does_not_quarantine_the_instance_past_the_budget() {
            // Given
            instanceWithAFailingCopyOfAnotherStream(SagaRunnerConfig.defaults());
            passes(budget.plusSeconds(1));
            // While the sweep of the second delivery reacts to the copy, another delivery's sweep applies it.
            dispatcher.beforeReservationOf(T1, () -> {
                dispatcher.stopFailingReservationOf(T1);
                deliver(itemAt(2, "item2"));
                dispatcher.failReservationOf(T1);
            });

            // When
            deliver(itemAt(3, "item3"));

            // Then
            SagaEnvelope<Order> instance = store.find(ORDER).orElseThrow();
            assertAll(
                    () -> assertThat(reactionsTo(T1)).as("the reaction of the copy the other delivery applied").isEqualTo(1),
                    () -> assertThat(instance.status()).as("the instance status").isEqualTo(SagaStatus.ACTIVE),
                    () -> assertThat(instance.failure()).as("the failure record").isNull()
            );
        }
    }

    @Nested
    @DisplayName("when the body of an event cannot be converted at the moment it is held")
    class BodyThatCannotBeConverted {

        // Converts every event but fails every conversion of the armed event after the first, until it is healed.
        private final class ConverterThatBreaksAfterTheFirstConversion implements CloudEventConverter<OrderEvent> {
            private final CloudEventConverter<OrderEvent> delegate = HeldOrderSaga.converter();
            private final Map<String, Integer> conversions = new ConcurrentHashMap<>();
            private volatile @Nullable String armedId;
            private volatile boolean healed;

            void arm(CloudEvent event) {
                armedId = event.getId();
            }

            void heal() {
                healed = true;
            }

            @Override
            public CloudEvent toCloudEvent(OrderEvent domainEvent) {
                return delegate.toCloudEvent(domainEvent);
            }

            @Override
            public OrderEvent toDomainEvent(CloudEvent cloudEvent) {
                int seen = conversions.merge(cloudEvent.getId(), 1, Integer::sum);
                if (!healed && cloudEvent.getId().equals(armedId) && seen > 1) {
                    throw new IllegalStateException("cannot convert " + cloudEvent.getId());
                }
                return delegate.toDomainEvent(cloudEvent);
            }

            @Override
            public String getCloudEventType(Class<? extends OrderEvent> type) {
                return delegate.getCloudEventType(type);
            }
        }

        private final ConverterThatBreaksAfterTheFirstConversion converter = new ConverterThatBreaksAfterTheFirstConversion();

        private void startWithTheBrokenConverter() {
            execution = executionOf(HeldOrderSaga.saga(), SagaRunnerConfig.defaults(), converter, true);
        }

        @Test
        void the_copy_is_flagged_as_a_start_event_so_that_it_does_not_wait() {
            // Given
            startWithTheBrokenConverter();
            CloudEvent item = itemAt(4, BOOK);
            converter.arm(item);

            // When
            deliver(item);

            // Then
            assertAll(
                    () -> assertThat(refused).as("the delivery was acknowledged").isEmpty(),
                    () -> assertThat(store.heldIndex(ORDER)).as("the held index").singleElement().satisfies(ref -> assertThat(ref.startEvent()).isTrue())
            );
        }

        @Test
        void the_copy_is_applied_rather_than_left_waiting_and_the_conversion_failure_surfaces_when_the_instance_starts() {
            // Given
            startWithTheBrokenConverter();
            CloudEvent item = itemAt(4, BOOK);
            converter.arm(item);
            deliver(item);

            // When
            Throwable startRefused = deliver(placedAt(5));
            Throwable nextRefused = deliver(itemAt(6, "item6"));

            // Then
            assertAll(
                    () -> assertThat(startRefused == null ? nextRefused : startRefused).as("a delivery is refused for the conversion failure").isNotNull().hasRootCauseMessage("cannot convert " + item.getId()),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction while the body cannot be converted").isZero(),
                    () -> assertThat(heldKeys()).as("the copy is not released").contains(STREAM + "@4")
            );
        }

        @Test
        void the_copy_applies_once_when_the_body_can_be_converted_again() {
            // Given
            startWithTheBrokenConverter();
            CloudEvent item = itemAt(4, BOOK);
            converter.arm(item);
            deliver(item);
            deliver(placedAt(5));
            converter.heal();

            // When
            deliver(itemAt(6, "item6"));

            // Then
            assertAll(
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction").isEqualTo(1),
                    () -> assertThat(reactionsTo("item6")).as("the reaction of the delivery that found the copy").isEqualTo(1),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("on a feed in commit order and on the same instance as a waiting copy")
    class CommitOrderedFeedBesideAWaitingCopy {

        @Test
        void an_event_below_the_start_on_the_stream_is_skipped_and_not_held() {
            // Given
            start(SagaRunnerConfig.defaults());
            deliver(placedAt(5));
            deliver(itemAt(4, BOOK));
            startOnAFeedInCommitOrder(SagaRunnerConfig.defaults());

            // When
            Throwable skipped = deliver(itemAt(3, "skipped"));

            // Then
            assertAll(
                    () -> assertThat(skipped).as("the delivery is acknowledged").isNull(),
                    () -> assertThat(reactionsTo("skipped")).as("the reaction of the event below the start").isZero(),
                    () -> assertThat(heldKeys()).as("only the copy that was held before").containsExactly(STREAM + "@4")
            );
        }

        @Test
        void a_copy_from_another_stream_still_applies_while_the_copy_below_the_start_keeps_waiting() {
            // Given
            start(SagaRunnerConfig.defaults());
            deliver(itemOn(OTHER_STREAM, 2, "other"));
            deliver(placedAt(5));
            deliver(itemAt(4, BOOK));
            startOnAFeedInCommitOrder(SagaRunnerConfig.defaults());

            // When
            Throwable next = deliver(itemAt(6, "next"));

            // Then
            assertAll(
                    () -> assertThat(next).as("the delivery is acknowledged").isNull(),
                    () -> assertThat(reactionsTo("other")).as("the copy of the other stream, applied by the sweep").isEqualTo(1),
                    () -> assertThat(reactionsTo(BOOK)).as("the copy below the start on the start's stream").isZero(),
                    () -> assertThat(reactionsTo("next")).as("the delivery above the start").isEqualTo(1),
                    () -> assertThat(heldKeys()).as("the held index").containsExactly(STREAM + "@4")
            );
        }

        @Test
        void an_event_below_the_start_is_refused_and_not_held_while_a_copy_left_from_before_the_deploy_keeps_failing() {
            // Given
            start(SagaRunnerConfig.defaults());
            dispatcher.failReservationOf("x");
            deliver(itemOn(OTHER_STREAM, 1, "x"));
            deliver(placedAt(5));
            startOnAFeedInCommitOrder(SagaRunnerConfig.defaults());

            // When
            Throwable refusedBelowTheStart = deliver(itemAt(4, "c4"));

            // Then
            assertAll(
                    () -> assertThat(refusedBelowTheStart).as("the delivery is refused for the copy that fails").isNotNull().hasRootCause(dispatcher.failure),
                    () -> assertThat(heldKeys()).as("the held index, with nothing stored for the event below the start").doesNotContain(STREAM + "@4"),
                    () -> assertThat(reactionsTo("c4")).as("the reaction of the event below the start").isZero()
            );
        }
    }

    /**
     * The flag of a held copy that says whether it is a start event is computed when it is held, from the definition of the
     * saga at that time. A deploy that changes the start event types does not update such a copy, and these
     * tests record what the executor of the new definition does with it. They describe the behaviour as it is.
     */
    @Nested
    @DisplayName("when a deploy changes the start event types and a held copy has the flag of the old definition")
    class StartEventTypesChangedByADeploy {

        private static final String LATE = "late";

        // The shared saga, which starts on an order only, or the same saga that starts on an item as well.
        private Saga<OrderEvent, Order, OrderCommand> definition(boolean itemsStartToo) {
            Saga.Builder<OrderEvent, Order, OrderCommand> builder = Saga.<OrderEvent, Order, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class);
            if (itemsStartToo) {
                builder = builder.startsOn(ItemAdded.class);
            }
            return builder
                    .evolve(OrderPlaced.class, (state, e) -> new Order(state == null ? List.of() : state.items()))
                    .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new OpenLedger(e.orderId()))))
                    .evolve(ItemAdded.class, (state, e) -> (state == null ? new Order(List.of()) : state).withItem(e.item()))
                    .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                    .isTerminal(order -> order != null && order.items().contains(DONE))
                    .build();
        }

        private SagaExecution<OrderEvent, Order, OrderCommand> executionOf(boolean itemsStartToo) {
            return SagaWaitingHeldCopyTest.this.executionOf(definition(itemsStartToo), SagaRunnerConfig.defaults(), HeldOrderSaga.converter(), true);
        }

        private @Nullable Throwable deliverTo(SagaExecution<OrderEvent, Order, OrderCommand> target, CloudEvent event) {
            Throwable thrown = catchThrowable(() -> target.onCloudEvent(event));
            if (thrown != null) {
                refused.add(thrown);
            }
            return thrown;
        }

        // Held while the start at 5 was applied, by a definition under which an item does not start an instance.
        private SagaExecution<OrderEvent, Order, OrderCommand> itemWaitingBelowTheStartUnderTheOldDefinition() {
            SagaExecution<OrderEvent, Order, OrderCommand> old = executionOf(false);
            deliverTo(old, placedAt(5));
            deliverTo(old, itemAt(4, BOOK));
            assertThat(store.heldIndex(ORDER)).as("the copy waits under the old definition with the flag unset").singleElement().satisfies(ref -> assertThat(ref.startEvent()).isFalse());
            return old;
        }

        @Test
        void a_copy_flagged_as_not_a_start_event_keeps_waiting_when_the_new_definition_starts_on_its_type() {
            // Given
            itemWaitingBelowTheStartUnderTheOldDefinition();
            SagaExecution<OrderEvent, Order, OrderCommand> redeployed = executionOf(true);

            // When
            Throwable next = deliverTo(redeployed, placedAt(6));

            // Then
            assertAll(
                    () -> assertThat(next).as("the delivery is acknowledged").isNull(),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction").isZero(),
                    () -> assertThat(heldKeys()).as("the copy keeps waiting").containsExactly(STREAM + "@4"),
                    () -> assertThat(statusOfTheInstance()).as("the instance status").isEqualTo(SagaStatus.ACTIVE)
            );
        }

        @Test
        void the_redelivery_of_a_copy_flagged_as_not_a_start_event_is_applied_once_as_a_start_event_and_lowers_the_start_under_the_new_definition() {
            // Given
            itemWaitingBelowTheStartUnderTheOldDefinition();
            SagaExecution<OrderEvent, Order, OrderCommand> redeployed = executionOf(true);

            // When
            Throwable redelivered = deliverTo(redeployed, itemAt(4, BOOK));
            long reactionsAfterTheRedelivery = reactionsTo(BOOK);
            deliverTo(redeployed, itemAt(4, BOOK));
            deliverTo(redeployed, placedAt(7));

            // Then
            assertAll(
                    () -> assertThat(redelivered).as("the redelivery is acknowledged").isNull(),
                    () -> assertThat(reactionsAfterTheRedelivery).as("the item reaction after the redelivery").isEqualTo(1),
                    () -> assertThat(store.find(ORDER).orElseThrow().startMark()).as("the start mark").isEqualTo(new SagaEnvelope.StartMark(STREAM, 4)),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty(),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction after one more redelivery and one more event").isEqualTo(1)
            );
        }

        @Test
        void a_start_event_below_a_copy_flagged_as_not_a_start_event_applies_the_copy_under_the_new_definition() {
            // Given
            itemWaitingBelowTheStartUnderTheOldDefinition();
            SagaExecution<OrderEvent, Order, OrderCommand> redeployed = executionOf(true);

            // When
            deliverTo(redeployed, itemAt(3, LATE));

            // Then
            assertAll(
                    () -> assertThat(refused).as("every delivery was acknowledged").isEmpty(),
                    () -> assertThat(reactionsTo(LATE)).as("the item reaction of the start event at 3").isEqualTo(1),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction of the copy at 4").isEqualTo(1),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty()
            );
        }

        @Test
        void completing_the_instance_releases_a_copy_flagged_as_not_a_start_event_without_a_reaction_under_the_new_definition() {
            // Given
            itemWaitingBelowTheStartUnderTheOldDefinition();
            SagaExecution<OrderEvent, Order, OrderCommand> redeployed = executionOf(true);

            // When
            deliverTo(redeployed, itemAt(6, DONE));

            // Then
            assertAll(
                    () -> assertThat(refused).as("every delivery was acknowledged").isEmpty(),
                    () -> assertThat(statusOfTheInstance()).as("the instance status").isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction of the copy").isZero(),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty()
            );
        }

        // Kept by a definition under which an item starts an instance, behind a copy of another stream that fails.
        private void itemKeptAsAStartEventUnderTheOldDefinition() {
            SagaExecution<OrderEvent, Order, OrderCommand> old = executionOf(true);
            dispatcher.failReservationOf(BOOK + "-other");
            deliverTo(executionOf(false), itemOn(OTHER_STREAM, 1, BOOK + "-other"));
            deliverTo(old, placedAt(5));
            deliverTo(old, itemAt(4, BOOK));
            assertThat(store.heldIndex(ORDER)).as("the item is kept with the flag set").filteredOn(ref -> ref.key().equals(STREAM + "@4")).singleElement().satisfies(ref -> assertThat(ref.startEvent()).isTrue());
            dispatcher.stopFailingReservationOf(BOOK + "-other");
        }

        @Test
        void a_copy_flagged_as_a_start_event_is_applied_at_once_without_waiting_when_the_new_definition_no_longer_starts_on_its_type() {
            // Given
            itemKeptAsAStartEventUnderTheOldDefinition();
            SagaExecution<OrderEvent, Order, OrderCommand> redeployed = executionOf(false);

            // When
            Throwable next = deliverTo(redeployed, placedAt(6));

            // Then
            assertAll(
                    () -> assertThat(next).as("the delivery is acknowledged").isNull(),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction, below the start at 5").isEqualTo(1),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty()
            );
        }

        @Test
        void a_start_below_a_copy_flagged_as_a_start_event_still_runs_the_start_once_after_the_copy_was_applied() {
            // Given
            itemKeptAsAStartEventUnderTheOldDefinition();
            SagaExecution<OrderEvent, Order, OrderCommand> redeployed = executionOf(false);
            deliverTo(redeployed, placedAt(6));

            // When
            Throwable lower = deliverTo(redeployed, placedAt(3));

            // Then
            assertAll(
                    () -> assertThat(lower).as("the start at 3 is acknowledged").isNull(),
                    () -> assertThat(ledgersOpened()).as("the ledgers opened by the starts at 5 and 3 and the placement at 6").isEqualTo(3),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction").isEqualTo(1),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty()
            );
        }
    }

    @Nested
    @DisplayName("when a sweep applies a copy that changes what the other copies wait for")
    class SweepThatChangesWhatCopiesWaitFor {

        private static final String T1 = "T1";
        // Named so that its key sorts before the keys of the order stream, which the sweep orders equal hold times by.
        private static final String STREAM_SWEPT_FIRST = "a-" + OTHER_STREAM;
        private static final String HIGH = "high";
        private static final String TRIGGER = "trigger";

        // The shared saga, except that a placement reserves under its own event id, so a test can make a placement fail.
        private Saga<OrderEvent, Order, OrderCommand> sagaReservingOnPlacement() {
            return Saga.<OrderEvent, Order, OrderCommand>builder(null)
                    .correlateAll(OrderEvent::orderId)
                    .startsOn(OrderPlaced.class)
                    .evolve(OrderPlaced.class, (state, e) -> new Order(List.of()))
                    .react(OrderPlaced.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.eventId()))))
                    .evolve(ItemAdded.class, (state, e) -> state.withItem(e.item()))
                    .react(ItemAdded.class, (state, e) -> List.of(SagaEffect.issue(new Reserve(e.orderId(), e.item()))))
                    .isTerminal(order -> order != null && order.items().contains(DONE))
                    .build();
        }

        // The start at 5 is applied while a copy of another stream fails, and the sweep reaches that copy first, so every
        // delivery after it is refused for the copy and kept as a plain held copy, with nothing else applied.
        private void startAtFiveBehindAFailingCopy() {
            start(sagaReservingOnPlacement(), SagaRunnerConfig.defaults().withQuarantineAfter(Duration.ofMinutes(5)));
            dispatcher.failReservationOf(T1);
            deliver(itemOn(STREAM_SWEPT_FIRST, 1, T1));
            deliver(placedAt(5));
        }

        private List<String> reservedItems() {
            return dispatcher.issued().stream().filter(Reserve.class::isInstance).map(command -> ((Reserve) command).item()).toList();
        }

        @Test
        void a_start_event_kept_below_the_mark_lowers_the_start_and_applies_the_waiting_item_and_a_higher_copy_in_version_order_in_the_same_delivery() {
            // Given
            startAtFiveBehindAFailingCopy();
            deliver(itemAt(4, BOOK));
            deliver(itemAt(6, HIGH));
            deliver(placedAt(3));
            dispatcher.stopFailingReservationOf(T1);
            int reservedBefore = reservedItems().size();

            // When
            Throwable next = deliver(itemAt(8, TRIGGER));

            // Then
            assertAll(
                    () -> assertThat(next).as("the delivery is acknowledged").isNull(),
                    () -> assertThat(reservedItems().subList(reservedBefore, reservedItems().size())).as("the reservations of the delivery, in the order they ran")
                            .containsExactly(T1, "placed-3", BOOK, HIGH, TRIGGER),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty()
            );
        }

        @Test
        void a_copy_that_completes_the_instance_in_a_sweep_releases_the_waiting_copy_in_the_same_delivery() {
            // Given
            startAtFiveBehindAFailingCopy();
            deliver(itemAt(4, BOOK));
            deliver(itemAt(6, DONE));
            dispatcher.stopFailingReservationOf(T1);

            // When
            Throwable next = deliver(itemAt(8, TRIGGER));

            // Then
            assertAll(
                    () -> assertThat(next).as("the delivery is acknowledged").isNull(),
                    () -> assertThat(statusOfTheInstance()).as("the instance status").isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(reactionsTo(DONE)).as("the reaction of the copy that completes the instance").isEqualTo(1),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction of the copy that waited").isZero(),
                    () -> assertThat(heldKeys()).as("the held index").isEmpty()
            );
        }

        @Test
        void the_redelivery_of_an_item_that_waits_below_the_start_is_acknowledged_without_a_reaction_and_leaves_one_copy() {
            // Given
            start(SagaRunnerConfig.defaults());
            deliver(placedAt(5));
            deliver(itemAt(4, BOOK));

            // When
            Throwable redelivered = deliver(itemAt(4, BOOK));

            // Then
            assertAll(
                    () -> assertThat(redelivered).as("the redelivery is acknowledged").isNull(),
                    () -> assertThat(reactionsTo(BOOK)).as("the item reaction").isZero(),
                    () -> assertThat(heldKeys()).as("the held index").containsExactly(STREAM + "@4")
            );
        }

        @Test
        void a_delivery_whose_own_sweep_applies_a_copy_that_completes_the_instance_is_acknowledged_and_its_reaction_does_not_run() {
            // Given
            start(sagaCompletingOnDone(), SagaRunnerConfig.defaults().disableQuarantine());
            dispatcher.failReservationOf(DONE);
            deliver(itemOn(OTHER_STREAM, 1, DONE));
            deliver(placedAt(5));
            dispatcher.stopFailingReservationOf(DONE);

            // When
            Throwable delivered = deliver(itemOn(THIRD_STREAM, 1, BOOK));

            // Then
            assertAll(
                    () -> assertThat(delivered).as("the delivery is acknowledged").isNull(),
                    () -> assertThat(statusOfTheInstance()).as("the instance status").isEqualTo(SagaStatus.COMPLETED),
                    () -> assertThat(reactionsTo(DONE)).as("the reaction of the copy that completes the instance").isEqualTo(1),
                    () -> assertThat(reactionsTo(BOOK)).as("the reaction of the delivery the copy completed the instance before").isZero()
            );
        }

        @Test
        void an_event_held_before_the_start_runs_before_the_copy_that_completes_the_instance_in_the_same_sweep() {
            // Given
            start(sagaCompletingOnDone(), SagaRunnerConfig.defaults().disableQuarantine());
            dispatcher.failReservationOf(DONE);
            deliver(itemOn(OTHER_STREAM, 1, DONE));
            deliver(itemOn(THIRD_STREAM, 1, BOOK));
            deliver(placedAt(5));
            dispatcher.stopFailingReservationOf(DONE);

            // When
            Throwable next = deliver(itemOn(THIRD_STREAM, 2, "next"));

            // Then
            assertAll(
                    () -> assertThat(next).as("the delivery is acknowledged").isNull(),
                    () -> assertThat(reactionsTo(BOOK)).as("the reaction of the event that was held").isEqualTo(1),
                    () -> assertThat(reactionsTo(DONE)).as("the reaction of the copy that completes the instance").isEqualTo(1),
                    () -> assertThat(statusOfTheInstance()).as("the instance status").isEqualTo(SagaStatus.COMPLETED)
            );
        }
    }

    /**
     * Holds events by delegating to the in-memory store. A test can run something once at a point of a delivery, to
     * interleave another delivery there, and can make the next release stop before it deletes the copy.
     */
    private static final class ScriptedStore<S> implements SagaStateStore<S>, SagaHeldEvents<S> {

        enum Point {
            BEFORE_HOLD, AFTER_HOLD, AFTER_INSTANCE_READ, BEFORE_COMPARE_AND_SAVE
        }

        private final InMemorySagaStateStore<S> delegate = new InMemorySagaStateStore<>();
        private final Map<Point, Runnable> scripted = new ConcurrentHashMap<>();
        private final AtomicInteger releasesToStop = new AtomicInteger();
        private final AtomicInteger releasesThatStopped = new AtomicInteger();

        /** Runs once, the next time a delivery reaches the point. */
        void once(Point point, Runnable action) {
            scripted.put(point, action);
        }

        void failNextRelease() {
            releasesToStop.incrementAndGet();
        }

        int releasesThatStopped() {
            return releasesThatStopped.get();
        }

        private void reach(Point point) {
            Runnable action = scripted.remove(point);
            if (action != null) {
                action.run();
            }
        }

        @Override
        public Optional<SagaEnvelope<S>> find(String sagaId) {
            Optional<SagaEnvelope<S>> found = delegate.find(sagaId);
            reach(Point.AFTER_INSTANCE_READ);
            return found;
        }

        @Override
        public Optional<SagaEnvelope<S>> findWithoutState(String sagaId) {
            Optional<SagaEnvelope<S>> found = delegate.findWithoutState(sagaId);
            reach(Point.AFTER_INSTANCE_READ);
            return found;
        }

        @Override
        public boolean compareAndSave(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
            reach(Point.BEFORE_COMPARE_AND_SAVE);
            return delegate.compareAndSave(sagaId, envelope, expectedVersion);
        }

        @Override
        public boolean compareAndSaveWithoutState(String sagaId, SagaEnvelope<S> envelope, long expectedVersion) {
            return delegate.compareAndSaveWithoutState(sagaId, envelope, expectedVersion);
        }

        @Override
        public List<SagaEnvelope<S>> findWithDueTimers(Instant now, int limit) {
            return delegate.findWithDueTimers(now, limit);
        }

        @Override
        public void delete(String sagaId) {
            delegate.delete(sagaId);
        }

        @Override
        public boolean hold(String sagaId, HeldEvent event) {
            reach(Point.BEFORE_HOLD);
            boolean inserted = delegate.hold(sagaId, event);
            reach(Point.AFTER_HOLD);
            return inserted;
        }

        @Override
        public void failing(String sagaId, String key, Instant at) {
            delegate.failing(sagaId, key, at);
        }

        @Override
        public List<HeldEventRef> heldIndex(String sagaId) {
            return delegate.heldIndex(sagaId);
        }

        @Override
        public List<HeldEvent> held(String sagaId, Collection<String> keys) {
            return delegate.held(sagaId, keys);
        }

        @Override
        public void release(String sagaId, String key) {
            if (releasesToStop.getAndUpdate(remaining -> remaining > 0 ? remaining - 1 : 0) > 0) {
                releasesThatStopped.incrementAndGet();
                throw new IllegalStateException("stopped before the release of " + key);
            }
            delegate.release(sagaId, key);
        }
    }
}
