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

package org.occurrent.springboot.reactor;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.occurrent.springboot.reactor.LateRegistrationOnANonBlockingThreadTest.RecordingDelegate;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A subscription the starter registers with a {@code BEGINNING} start and the default resume behaviour reads the
 * stored position itself to decide between replaying and resuming. Subscribed again once the cancel of the same id has
 * completed, it has to find the position the cancel deleted gone rather than resume from it.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(30)
class StartPositionSupportAfterACancelTest {

    private static final String SUBSCRIPTION_ID = "sub";
    private static final StringBasedCheckpoint REACHED_BEFORE_THE_CANCEL = new StringBasedCheckpoint("reached-before-the-cancel");

    @Test
    void a_subscription_starting_at_the_beginning_subscribed_once_the_cancel_of_its_id_has_completed_replays_from_the_beginning() {
        whereTheResubscriptionStarts(startPositionSupport -> startPositionSupport.generateAgnosticStartAt(SUBSCRIPTION_ID, StartPosition.BEGINNING, -1, ResumeBehavior.DEFAULT),
                startAt -> assertThat(startAt).as("start position of the subscription made once the cancel had completed").hasToString(StartAt.checkpoint(GlobalCheckpoint.of(0)).toString()));
    }

    @Test
    void a_dcb_subscription_starting_at_the_beginning_subscribed_once_the_cancel_of_its_id_has_completed_replays_from_the_beginning() {
        whereTheResubscriptionStarts(startPositionSupport -> startPositionSupport.generateDcbStartAt(SUBSCRIPTION_ID, StartPosition.BEGINNING, -1, ResumeBehavior.DEFAULT).toStartAt(),
                startAt -> assertThat(startAt).as("start position of the DCB subscription made once the cancel had completed").hasToString(StartAt.checkpoint(GlobalCheckpoint.of(0)).toString()));
    }

    // The cancel deletes the only stored position, and with none stored a BEGINNING start replays from position 0,
    // which is what the assertion checks for. The delegate reports GlobalCheckpoint.of(1) as where the feed is now.
    private static void whereTheResubscriptionStarts(Function<StartPositionSupport, StartAt> startAt, Consumer<StartAt> assertion) {
        SlowDeleteCheckpointStorage storage = new SlowDeleteCheckpointStorage();
        new ApplicationContextRunner()
                .withBean(CheckpointStorage.class, () -> storage)
                .run(context -> {
                    RecordingDelegate delegate = new RecordingDelegate();
                    ReactorDurableSubscriptionModel model = new ReactorDurableSubscriptionModel(delegate, storage);
                    try {
                        // Given
                        storage.stored.put(SUBSCRIPTION_ID, REACHED_BEFORE_THE_CANCEL);
                        model.subscribe(SUBSCRIPTION_ID, null, StartAt.subscriptionModelDefault(), __ -> Mono.empty());

                        // When
                        model.cancelSubscription(SUBSCRIPTION_ID).block(Duration.ofSeconds(5));
                        model.subscribe(SUBSCRIPTION_ID, null, startAt.apply(new StartPositionSupport(context)), __ -> Mono.empty())
                                .waitUntilStarted().block(Duration.ofSeconds(5));

                        // Then
                        assertion.accept(delegate.startedAt.get(SUBSCRIPTION_ID));
                    } finally {
                        model.shutdown();
                    }
                });
    }

    // Deletes only some time after it is asked to, the way a store under load does
    private static class SlowDeleteCheckpointStorage implements CheckpointStorage {
        final Map<String, Checkpoint> stored = new ConcurrentHashMap<>();

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return Mono.fromSupplier(() -> stored.get(subscriptionId));
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
            return Mono.fromSupplier(() -> {
                stored.put(subscriptionId, checkpoint);
                return checkpoint;
            });
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.delay(Duration.ofMillis(300)).then(Mono.fromRunnable(() -> stored.remove(subscriptionId)));
        }
    }
}
