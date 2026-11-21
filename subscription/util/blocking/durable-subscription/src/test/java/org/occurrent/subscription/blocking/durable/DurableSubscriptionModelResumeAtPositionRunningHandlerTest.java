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

package org.occurrent.subscription.blocking.durable;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointAwareCloudEvent;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.CheckpointWriteConditionNotFulfilledException;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.SubscriptionFilter;
import org.occurrent.subscription.api.blocking.CheckpointAwareSubscriptionModel;
import org.occurrent.subscription.api.blocking.QuietPositionReportingSubscriptions;
import org.occurrent.subscription.api.blocking.RepositionableSubscriptions;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.inmemory.InMemoryCheckpointStorage;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.occurrent.subscription.util.predicate.EveryN.everyEvent;

/**
 * A resume at a position on a {@link DurableSubscriptionModel} whose wrapped model returned from the pause while a
 * handler was still running, so that handler returns after the resume has stored its position. The wrapped model is a
 * fake that lets the pause return at once, and the handler is held on a latch until the test releases it.
 */
@DisplayName("DurableSubscriptionModelResumeAtPositionRunningHandler")
@DisplayNameGeneration(ReplaceUnderscores.class)
class DurableSubscriptionModelResumeAtPositionRunningHandlerTest {

    private static final String SUBSCRIPTION_ID = "subscription";
    private static final Checkpoint START = new StringBasedCheckpoint("start");
    private static final Checkpoint EVENT_OF_THE_RUNNING_HANDLER = new StringBasedCheckpoint("event-of-the-running-handler");
    private static final Checkpoint LATER_THAN_THE_EVENT = new StringBasedCheckpoint("later-than-the-event");
    private static final Checkpoint EARLIER_THAN_THE_EVENT = new StringBasedCheckpoint("earlier-than-the-event");
    private static final Checkpoint EVENT_AFTER_THE_RESUME = new StringBasedCheckpoint("event-after-the-resume");
    private static final Checkpoint EVENT_READ_BEFORE_THE_PAUSE = new StringBasedCheckpoint("event-read-before-the-pause");
    private static final Checkpoint EVENT_OF_THE_RUN_THE_FAILED_RESUME_STARTED = new StringBasedCheckpoint("event-of-the-run-the-failed-resume-started");
    private static final Checkpoint EVENT_STORED_BEFORE_THE_RESUME = new StringBasedCheckpoint("event-stored-before-the-resume");
    private static final Checkpoint QUIET = new StringBasedCheckpoint("quiet");

    private final PausableModel wrapped = new PausableModel();
    private final InMemoryCheckpointStorage storage = new InMemoryCheckpointStorage();
    private final CountDownLatch handlerStarted = new CountDownLatch(1);
    private final CountDownLatch handlerMayReturn = new CountDownLatch(1);
    private final ExecutorService handlerThreads = Executors.newCachedThreadPool();

    @AfterEach
    void releaseTheHandlerAndStopItsThreads() {
        handlerMayReturn.countDown();
        handlerThreads.shutdownNow();
    }

    @Nested
    @DisplayName("when a handler is still running after the pause returned")
    class When_a_handler_is_still_running_after_the_pause_returned {

        @Test
        void a_resume_at_a_later_position_leaves_that_position_stored_once_the_handler_has_returned() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeFromTheStoredStart(model);
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);

            // When
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(LATER_THAN_THE_EVENT));
            handlerReturns(handler);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(LATER_THAN_THE_EVENT);
        }

        @Test
        void a_resume_at_an_earlier_position_does_not_skip_the_replay_the_caller_asked_for_after_a_restart() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeFromTheStoredStart(model);
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);

            // When
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT));
            handlerReturns(handler);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(EARLIER_THAN_THE_EVENT);
        }

        @Test
        void an_event_delivered_after_the_resume_still_stores_its_checkpoint() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeFromTheStoredStart(model);
            deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT));

            // When
            wrapped.deliver(SUBSCRIPTION_ID, EVENT_AFTER_THE_RESUME);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(EVENT_AFTER_THE_RESUME);
        }

        @Test
        void a_resume_at_a_position_the_wrapped_model_cannot_restart_at_stores_nothing_so_the_running_handler_still_stores_its_checkpoint() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeFromTheStoredStart(model);
            wrapped.cannotRestartAt.add(EARLIER_THAN_THE_EVENT);
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);

            // When
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT));
            handlerReturns(handler);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(EVENT_OF_THE_RUNNING_HANDLER);
        }
    }

    @Nested
    @DisplayName("when a quiet position was read before the resume")
    class When_a_quiet_position_was_read_before_the_resume {

        @Test
        void the_quiet_position_saved_after_the_resume_is_not_stored_over_the_position_of_the_resume() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
            subscribeFromTheStoredStart(model);
            Consumer<Checkpoint> saverReadBeforeTheResume = aSaverOnceTheIntervalHasPassed();
            model.pauseSubscription(SUBSCRIPTION_ID);
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT));

            // When
            saverReadBeforeTheResume.accept(QUIET);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(EARLIER_THAN_THE_EVENT);
        }
    }

    @Nested
    @DisplayName("when the handler that started before the resume has returned")
    class When_the_handler_that_started_before_the_resume_has_returned {

        @Test
        void a_quiet_position_is_saved_without_a_new_delivery() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
            subscribeFromTheStoredStart(model);
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT));
            handlerReturns(handler);

            // When
            // Then
            await().atMost(2, SECONDS).untilAsserted(() -> {
                Consumer<Checkpoint> saver = wrapped.beforeReading(SUBSCRIPTION_ID);
                if (saver != null) {
                    saver.accept(QUIET);
                }
                assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(QUIET);
            });
        }
    }

    @Nested
    @DisplayName("when the wrapped model accepted a subscribe that then failed")
    class When_the_wrapped_model_accepted_a_subscribe_that_then_failed {

        @Test
        void a_handler_of_that_run_still_running_after_a_resume_at_a_position_does_not_store_its_checkpoint_over_it() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            wrapped.failsAfterAccepting = true;
            storage.save(SUBSCRIPTION_ID, START);
            assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> {
                handlerStarted.countDown();
                awaitQuietly(handlerMayReturn);
            })).isInstanceOf(IllegalStateException.class);
            assertThat(model.idsWithUntrackedRegistrations()).isEqualTo(1);
            CompletableFuture<Void> handler = CompletableFuture.runAsync(() -> wrapped.deliver(SUBSCRIPTION_ID, EVENT_OF_THE_RUNNING_HANDLER), handlerThreads);
            assertThat(awaitLatch(handlerStarted)).as("handler started").isTrue();
            model.pauseSubscription(SUBSCRIPTION_ID);

            // When
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(LATER_THAN_THE_EVENT));
            handlerReturns(handler);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(LATER_THAN_THE_EVENT);
        }

        @Test
        void a_run_of_that_subscribe_that_read_before_the_resume_does_not_store_the_checkpoint_of_its_event_over_the_position_of_the_resume() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            wrapped.failsAfterAccepting = true;
            storage.save(SUBSCRIPTION_ID, START);
            assertThatThrownBy(() -> model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> {
            })).isInstanceOf(IllegalStateException.class);
            assertThat(model.idsWithUntrackedRegistrations()).isEqualTo(1);
            CountDownLatch hasRead = new CountDownLatch(1);
            CompletableFuture<Void> run = CompletableFuture.runAsync(() -> {
                wrapped.beforeReading(SUBSCRIPTION_ID);
                hasRead.countDown();
                awaitQuietly(handlerMayReturn);
                wrapped.deliver(SUBSCRIPTION_ID, EVENT_READ_BEFORE_THE_PAUSE);
            }, handlerThreads);
            assertThat(awaitLatch(hasRead)).as("read before the pause").isTrue();
            model.pauseSubscription(SUBSCRIPTION_ID);
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT));

            // When
            handlerReturns(run);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(EARLIER_THAN_THE_EVENT);
        }
    }

    @Nested
    @DisplayName("when the wrapped model starts a run during the subscribe")
    class When_the_wrapped_model_starts_a_run_during_the_subscribe {

        @Test
        void the_checkpoint_of_the_event_that_run_read_is_not_stored_over_the_position_of_a_resume_after_the_subscribe_returned() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            CountDownLatch hasRead = new CountDownLatch(1);
            AtomicReference<CompletableFuture<Void>> run = new AtomicReference<>();
            wrapped.duringSubscribe = () -> {
                run.set(CompletableFuture.runAsync(() -> {
                    wrapped.beforeReading(SUBSCRIPTION_ID);
                    hasRead.countDown();
                    awaitQuietly(handlerMayReturn);
                    wrapped.deliver(SUBSCRIPTION_ID, EVENT_READ_BEFORE_THE_PAUSE);
                }, handlerThreads));
                assertThat(awaitLatch(hasRead)).as("read during the subscribe").isTrue();
            };
            subscribeFromTheStoredStart(model);
            model.pauseSubscription(SUBSCRIPTION_ID);
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT));

            // When
            handlerReturns(run.get());

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(EARLIER_THAN_THE_EVENT);
        }
    }

    @Nested
    @DisplayName("when the run that read before the pause calls the action after the resume")
    class When_the_run_that_read_before_the_pause_calls_the_action_after_the_resume {

        @Test
        void the_checkpoint_of_the_event_it_read_is_not_stored_over_the_position_of_the_resume() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeFromTheStoredStart(model);
            CountDownLatch hasRead = new CountDownLatch(1);
            CompletableFuture<Void> run = CompletableFuture.runAsync(() -> {
                wrapped.beforeReading(SUBSCRIPTION_ID);
                hasRead.countDown();
                awaitQuietly(handlerMayReturn);
                wrapped.deliver(SUBSCRIPTION_ID, EVENT_READ_BEFORE_THE_PAUSE);
            }, handlerThreads);
            assertThat(awaitLatch(hasRead)).as("read before the pause").isTrue();
            model.pauseSubscription(SUBSCRIPTION_ID);
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT));

            // When
            handlerReturns(run);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(EARLIER_THAN_THE_EVENT);
        }

        @Test
        void the_checkpoint_of_the_event_it_read_is_not_stored_over_the_position_of_the_resume_when_no_quiet_position_is_saved() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).neverSaveQuietPosition());
            subscribeFromTheStoredStart(model);
            CountDownLatch hasRead = new CountDownLatch(1);
            CompletableFuture<Void> run = CompletableFuture.runAsync(() -> {
                wrapped.beforeReading(SUBSCRIPTION_ID);
                hasRead.countDown();
                awaitQuietly(handlerMayReturn);
                wrapped.deliver(SUBSCRIPTION_ID, EVENT_READ_BEFORE_THE_PAUSE);
            }, handlerThreads);
            assertThat(awaitLatch(hasRead)).as("read before the pause").isTrue();
            model.pauseSubscription(SUBSCRIPTION_ID);
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT));

            // When
            handlerReturns(run);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(EARLIER_THAN_THE_EVENT);
        }
    }

    @Nested
    @DisplayName("when a resume at a position fails")
    class When_a_resume_at_a_position_fails {

        @Test
        void a_resume_whose_save_of_the_position_is_refused_does_not_let_the_running_handler_store_its_checkpoint() {
            // Given
            InMemoryCheckpointStorage refusing = new InMemoryCheckpointStorage() {
                @Override
                public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                    if (checkpoint.equals(EARLIER_THAN_THE_EVENT)) {
                        throw new CheckpointWriteConditionNotFulfilledException(subscriptionId, OptionalLong.empty(), condition);
                    }
                    return super.save(subscriptionId, checkpoint, condition);
                }
            };
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, refusing);
            refusing.save(SUBSCRIPTION_ID, START);
            model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> {
                handlerStarted.countDown();
                awaitQuietly(handlerMayReturn);
            });
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);

            // When
            assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT)))
                    .isInstanceOf(CheckpointWriteConditionNotFulfilledException.class);
            handlerReturns(handler);

            // Then
            assertThat(refusing.read(SUBSCRIPTION_ID)).isEqualTo(START);
        }

        @Test
        void a_resume_whose_wrapped_resume_throws_puts_back_the_previous_checkpoint_and_the_running_handler_does_not_store_its_checkpoint() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeFromTheStoredStart(model);
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);
            wrapped.failsResume = true;

            // When
            assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT)))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(START);
            handlerReturns(handler);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(START);
        }

        @Test
        void a_resume_that_fails_and_a_later_one_that_succeeds_do_not_let_the_run_the_failed_one_started_store_its_checkpoint() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage);
            subscribeFromTheStoredStart(model);
            model.pauseSubscription(SUBSCRIPTION_ID);
            CountDownLatch hasRead = new CountDownLatch(1);
            CompletableFuture<Void> run = new CompletableFuture<>();
            wrapped.whenResuming = () -> {
                wrapped.whenResuming = () -> {
                };
                CompletableFuture.runAsync(() -> {
                    wrapped.beforeReading(SUBSCRIPTION_ID);
                    hasRead.countDown();
                    awaitQuietly(handlerMayReturn);
                    wrapped.deliver(SUBSCRIPTION_ID, EVENT_OF_THE_RUN_THE_FAILED_RESUME_STARTED);
                }, handlerThreads).whenComplete((result, failure) -> {
                    if (failure == null) {
                        run.complete(null);
                    } else {
                        run.completeExceptionally(failure);
                    }
                });
                assertThat(awaitLatch(hasRead)).as("read by the run of the failed resume").isTrue();
            };
            wrapped.failsResume = true;
            assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT)))
                    .isInstanceOf(IllegalStateException.class);
            wrapped.failsResume = false;
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(LATER_THAN_THE_EVENT));

            // When
            handlerReturns(run);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(LATER_THAN_THE_EVENT);
        }

        @Test
        void a_resume_whose_save_of_the_position_throws_after_storing_it_does_not_let_the_running_handler_store_its_checkpoint_over_it() {
            // Given
            InMemoryCheckpointStorage losingTheReply = new InMemoryCheckpointStorage() {
                @Override
                public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                    Checkpoint saved = super.save(subscriptionId, checkpoint, condition);
                    if (checkpoint.equals(EARLIER_THAN_THE_EVENT)) {
                        throw new RuntimeException("The reply of the save was lost");
                    }
                    return saved;
                }
            };
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, losingTheReply);
            subscribeFromTheStoredStart(model, losingTheReply);
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);

            // When
            assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT)))
                    .hasMessage("The reply of the save was lost");
            handlerReturns(handler);

            // Then
            assertThat(losingTheReply.read(SUBSCRIPTION_ID)).isEqualTo(EARLIER_THAN_THE_EVENT);
        }

        @Test
        void a_resume_whose_wrapped_resume_throws_and_whose_put_back_throws_too_leaves_its_position_stored_once_the_running_handler_has_returned() {
            // Given
            AtomicBoolean putBackFails = new AtomicBoolean();
            InMemoryCheckpointStorage failingToPutBack = new InMemoryCheckpointStorage() {
                @Override
                public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                    if (putBackFails.get() && checkpoint.equals(START)) {
                        throw new IllegalStateException("The put-back failed");
                    }
                    return super.save(subscriptionId, checkpoint, condition);
                }
            };
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, failingToPutBack);
            subscribeFromTheStoredStart(model, failingToPutBack);
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);
            wrapped.failsResume = true;
            putBackFails.set(true);

            // When
            assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("resume failed for " + SUBSCRIPTION_ID);
            handlerReturns(handler);

            // Then
            assertThat(failingToPutBack.read(SUBSCRIPTION_ID)).isEqualTo(EARLIER_THAN_THE_EVENT);
        }

        @Test
        void a_quiet_position_is_saved_again_once_a_later_resume_at_a_position_has_stored_its_position() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
            subscribeFromTheStoredStart(model);
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);
            wrapped.failsResume = true;
            assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT)))
                    .isInstanceOf(IllegalStateException.class);
            handlerReturns(handler);
            wrapped.failsResume = false;

            // When
            model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(LATER_THAN_THE_EVENT));

            // Then
            await().atMost(2, SECONDS).untilAsserted(() -> {
                Consumer<Checkpoint> saver = wrapped.beforeReading(SUBSCRIPTION_ID);
                if (saver != null) {
                    saver.accept(QUIET);
                }
                assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(QUIET);
            });
        }

        @Test
        void no_quiet_position_is_saved_once_the_put_back_has_replaced_the_position_a_handler_returning_during_the_resume_found_stored() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
            subscribeFromTheStoredStart(model);
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);
            wrapped.whenResuming = () -> {
                wrapped.whenResuming = () -> {
                };
                handlerReturns(handler);
            };
            wrapped.failsResume = true;

            // When
            assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT)))
                    .isInstanceOf(IllegalStateException.class);

            // Then
            await().during(300, MILLISECONDS).atMost(2, SECONDS).untilAsserted(() -> {
                Consumer<Checkpoint> saver = wrapped.beforeReading(SUBSCRIPTION_ID);
                if (saver != null) {
                    saver.accept(QUIET);
                }
                assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(START);
            });
        }

        @Test
        void a_quiet_position_is_saved_after_a_failed_resume_put_back_the_checkpoint_of_an_event_stored_before_it() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
            subscribeFromTheStoredStart(model);
            wrapped.deliver(SUBSCRIPTION_ID, EVENT_STORED_BEFORE_THE_RESUME);
            model.pauseSubscription(SUBSCRIPTION_ID);
            wrapped.failsResume = true;

            // When
            assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT)))
                    .isInstanceOf(IllegalStateException.class);

            // Then
            assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(EVENT_STORED_BEFORE_THE_RESUME);
            await().atMost(2, SECONDS).untilAsserted(() -> {
                Consumer<Checkpoint> saver = wrapped.beforeReading(SUBSCRIPTION_ID);
                if (saver != null) {
                    saver.accept(QUIET);
                }
                assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(QUIET);
            });
        }

        @Test
        void a_quiet_position_is_saved_when_the_put_back_failed_and_a_delivery_that_read_before_the_resume_ended_during_it() {
            // Given
            AtomicBoolean putBackFails = new AtomicBoolean();
            InMemoryCheckpointStorage failingToPutBack = new InMemoryCheckpointStorage() {
                @Override
                public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                    if (putBackFails.get() && checkpoint.equals(START)) {
                        throw new IllegalStateException("The put-back failed");
                    }
                    return super.save(subscriptionId, checkpoint, condition);
                }
            };
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, failingToPutBack, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
            subscribeFromTheStoredStart(model, failingToPutBack);
            wrapped.beforeReading(SUBSCRIPTION_ID);
            model.pauseSubscription(SUBSCRIPTION_ID);
            wrapped.whenResuming = () -> {
                wrapped.whenResuming = () -> {
                };
                wrapped.deliver(SUBSCRIPTION_ID, EVENT_AFTER_THE_RESUME);
            };
            wrapped.failsResume = true;
            putBackFails.set(true);

            // When
            assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("resume failed for " + SUBSCRIPTION_ID);

            // Then
            assertThat(failingToPutBack.read(SUBSCRIPTION_ID)).isEqualTo(EARLIER_THAN_THE_EVENT);
            await().atMost(2, SECONDS).untilAsserted(() -> {
                Consumer<Checkpoint> saver = wrapped.beforeReading(SUBSCRIPTION_ID);
                if (saver != null) {
                    saver.accept(QUIET);
                }
                assertThat(failingToPutBack.read(SUBSCRIPTION_ID)).isEqualTo(QUIET);
            });
        }

        @Test
        void no_quiet_position_is_saved_when_a_delivery_stored_its_checkpoint_while_the_resume_read_the_previous_one_and_the_put_back_replaced_it() {
            // Given
            AtomicBoolean deliversOnTheNextRead = new AtomicBoolean();
            InMemoryCheckpointStorage deliveringOnRead = new InMemoryCheckpointStorage() {
                @Override
                public @Nullable Checkpoint read(String subscriptionId) {
                    Checkpoint read = super.read(subscriptionId);
                    if (deliversOnTheNextRead.compareAndSet(true, false)) {
                        wrapped.deliver(subscriptionId, EVENT_STORED_BEFORE_THE_RESUME);
                    }
                    return read;
                }
            };
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, deliveringOnRead, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
            subscribeFromTheStoredStart(model, deliveringOnRead);
            model.pauseSubscription(SUBSCRIPTION_ID);
            wrapped.failsResume = true;
            deliversOnTheNextRead.set(true);

            // When
            assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT)))
                    .isInstanceOf(IllegalStateException.class);

            // Then
            assertThat(deliveringOnRead.read(SUBSCRIPTION_ID)).isEqualTo(START);
            await().during(300, MILLISECONDS).atMost(2, SECONDS).untilAsserted(() -> {
                Consumer<Checkpoint> saver = wrapped.beforeReading(SUBSCRIPTION_ID);
                if (saver != null) {
                    saver.accept(QUIET);
                }
                assertThat(deliveringOnRead.read(SUBSCRIPTION_ID)).isEqualTo(START);
            });
        }

        @Test
        void no_quiet_position_is_saved_while_the_checkpoint_the_failed_resume_refused_is_the_latest_one() {
            // Given
            DurableSubscriptionModel model = new DurableSubscriptionModel(wrapped, storage, new DurableSubscriptionModelConfig(everyEvent()).saveQuietPositionEvery(Duration.ofMillis(1)));
            subscribeFromTheStoredStart(model);
            CompletableFuture<Void> handler = deliverAHandlerThatStaysRunning(EVENT_OF_THE_RUNNING_HANDLER);
            model.pauseSubscription(SUBSCRIPTION_ID);
            wrapped.failsResume = true;
            assertThatThrownBy(() -> model.resumeSubscription(SUBSCRIPTION_ID, StartAt.checkpoint(EARLIER_THAN_THE_EVENT)))
                    .isInstanceOf(IllegalStateException.class);

            // When
            handlerReturns(handler);

            // Then
            await().during(300, MILLISECONDS).atMost(2, SECONDS).untilAsserted(() -> {
                Consumer<Checkpoint> saver = wrapped.beforeReading(SUBSCRIPTION_ID);
                if (saver != null) {
                    saver.accept(QUIET);
                }
                assertThat(storage.read(SUBSCRIPTION_ID)).isEqualTo(START);
            });
        }
    }

    private Consumer<Checkpoint> aSaverOnceTheIntervalHasPassed() {
        AtomicReference<Consumer<Checkpoint>> saver = new AtomicReference<>();
        await().atMost(5, SECONDS).until(() -> {
            saver.set(wrapped.beforeReading(SUBSCRIPTION_ID));
            return saver.get() != null;
        });
        return saver.get();
    }

    private void subscribeFromTheStoredStart(DurableSubscriptionModel model) {
        subscribeFromTheStoredStart(model, storage);
    }

    private void subscribeFromTheStoredStart(DurableSubscriptionModel model, InMemoryCheckpointStorage checkpointStorage) {
        checkpointStorage.save(SUBSCRIPTION_ID, START);
        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), cloudEvent -> {
            if (positionOf(cloudEvent).equals(EVENT_OF_THE_RUNNING_HANDLER)) {
                handlerStarted.countDown();
                awaitQuietly(handlerMayReturn);
            }
        });
    }

    private CompletableFuture<Void> deliverAHandlerThatStaysRunning(Checkpoint position) {
        CompletableFuture<Void> handler = CompletableFuture.runAsync(() -> wrapped.deliver(SUBSCRIPTION_ID, position), handlerThreads);
        assertThat(awaitLatch(handlerStarted)).as("handler started").isTrue();
        return handler;
    }

    private void handlerReturns(CompletableFuture<Void> handler) {
        handlerMayReturn.countDown();
        try {
            handler.get(5, SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new AssertionError("The handler did not return", e);
        }
    }

    private static Checkpoint positionOf(CloudEvent cloudEvent) {
        return ((CheckpointAwareCloudEvent) cloudEvent).getCheckpoint();
    }

    private static boolean awaitLatch(CountDownLatch latch) {
        try {
            return latch.await(5, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        awaitLatch(latch);
    }

    /**
     * A wrapped model that returns from a pause at once whatever its handlers are doing, restarts at any position
     * unless told otherwise, and lets the test read and deliver by hand.
     */
    private static final class PausableModel implements CheckpointAwareSubscriptionModel, RepositionableSubscriptions, QuietPositionReportingSubscriptions {
        final Set<Checkpoint> cannotRestartAt = ConcurrentHashMap.newKeySet();
        volatile boolean failsAfterAccepting;
        volatile boolean failsResume;
        volatile Runnable duringSubscribe = () -> {
        };
        volatile Runnable whenResuming = () -> {
        };
        private final Map<String, Consumer<CloudEvent>> actions = new ConcurrentHashMap<>();
        private final Set<String> paused = ConcurrentHashMap.newKeySet();
        private final List<QuietPositionListener> listeners = new CopyOnWriteArrayList<>();

        void deliver(String subscriptionId, Checkpoint position) {
            CloudEvent cloudEvent = CloudEventBuilder.v1().withId(UUID.randomUUID().toString()).withSource(URI.create("urn:test")).withType("type").build();
            actions.get(subscriptionId).accept(new CheckpointAwareCloudEvent(cloudEvent, position));
        }

        @Nullable Consumer<Checkpoint> beforeReading(String subscriptionId) {
            return listeners.isEmpty() ? null : listeners.getFirst().beforeReading(subscriptionId);
        }

        @Override
        public Subscription subscribe(String subscriptionId, @Nullable SubscriptionFilter filter, StartAt startAt, Consumer<CloudEvent> action) {
            actions.put(subscriptionId, action);
            duringSubscribe.run();
            if (failsAfterAccepting) {
                throw new IllegalStateException("failed after accepting " + subscriptionId);
            }
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId, StartAt startAt) {
            whenResuming.run();
            if (failsResume) {
                throw new IllegalStateException("resume failed for " + subscriptionId);
            }
            if (!paused.remove(subscriptionId)) {
                throw new IllegalStateException("Subscription " + subscriptionId + " is not paused");
            }
            return subscription(subscriptionId);
        }

        @Override
        public Subscription resumeSubscription(String subscriptionId) {
            throw new UnsupportedOperationException("a plain resume is not expected");
        }

        @Override
        public boolean restartsAt(Checkpoint checkpoint) {
            return !cannotRestartAt.contains(checkpoint);
        }

        @Override
        public void pauseSubscription(String subscriptionId) {
            paused.add(subscriptionId);
        }

        @Override
        public void cancelSubscription(String subscriptionId) {
            actions.remove(subscriptionId);
            paused.remove(subscriptionId);
        }

        @Override
        public boolean isPaused(String subscriptionId) {
            return paused.contains(subscriptionId);
        }

        @Override
        public boolean isRunning(String subscriptionId) {
            return actions.containsKey(subscriptionId) && !paused.contains(subscriptionId);
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public @Nullable Checkpoint globalCheckpoint() {
            return null;
        }

        @Override
        public void addQuietPositionListener(QuietPositionListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeQuietPositionListener(QuietPositionListener listener) {
            listeners.remove(listener);
        }

        @Override
        public void stop() {
        }

        @Override
        public void start(boolean resumeSubscriptionsAutomatically) {
        }

        @Override
        public void shutdown() {
        }

        private static Subscription subscription(String subscriptionId) {
            return new Subscription() {
                @Override
                public String id() {
                    return subscriptionId;
                }

                @Override
                public boolean waitUntilStarted(Duration timeout) {
                    return true;
                }
            };
        }
    }
}
