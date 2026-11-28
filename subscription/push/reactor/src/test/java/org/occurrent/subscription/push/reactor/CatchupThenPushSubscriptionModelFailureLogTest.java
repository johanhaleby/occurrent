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

package org.occurrent.subscription.push.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.inmemory.reactor.InMemoryCheckpointStorage;
import org.occurrent.subscription.internal.SharedStorageMessages;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Under {@code startupMode = BACKGROUND} nobody waits for the replay, so the log is the only place a failed catch-up
 * reaches anyone, and it is where the recovery is told.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class CatchupThenPushSubscriptionModelFailureLogTest {

    private final List<CatchupThenPushSubscriptionModel> models = new ArrayList<>();

    @BeforeEach
    @AfterEach
    void forgetWhatWasLogged() {
        CapturingLoggerProvider.clear();
    }

    @AfterEach
    void shutDownTheModels() {
        models.forEach(CatchupThenPushSubscriptionModel::shutdown);
    }

    @Test
    void a_failed_catch_up_logs_how_to_change_what_is_stored_when_several_instances_run_it() {
        failCatchUp("failed-catch-up");
        failCatchUp("failed-catch-up-neighbour");

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(loggedFor(Level.ERROR, "failed-catch-up")).singleElement().satisfies(logged ->
                        assertThat(logged.message())
                                .startsWith("Catch-up failed for subscription failed-catch-up. Its registration on the live feed is kept")
                                .endsWith(SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT)));
    }

    @Test
    void a_catch_up_that_refused_a_checkpoint_that_is_not_a_catch_up_marker_logs_to_cancel_only_when_nothing_else_uses_it_and_not_to_cancel_unconditionally() {
        refuseCheckpointThatIsNotAMarker("refused-checkpoint");
        refuseCheckpointThatIsNotAMarker("refused-checkpoint-neighbour");

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(loggedFor(Level.ERROR, "refused-checkpoint")).singleElement().satisfies(logged ->
                        assertThat(logged.message())
                                .startsWith("Catch-up failed for subscription refused-checkpoint, because the checkpoint stored under its id is not a catch-up marker.")
                                .contains("cancel it only when nothing else uses that checkpoint.")
                                .endsWith(SharedStorageMessages.NOT_A_MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT + " " + SharedStorageMessages.NEW_ID_WHEN_SEVERAL_INSTANCES_RUN_IT)
                                .doesNotContain(SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT)
                                .doesNotContain("Cancel the subscription and subscribe again once the cause is fixed")));
    }

    @Test
    void a_cancel_whose_catch_up_marker_delete_fails_warns_to_cancel_again_and_how_to_delete_the_marker_when_several_instances_run_it() {
        cancelWhoseMarkerDeleteFails("failed-marker-delete");
        cancelWhoseMarkerDeleteFails("failed-marker-delete-neighbour");

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(loggedFor(Level.WARN, "failed-marker-delete")).singleElement().satisfies(logged ->
                        assertThat(logged.message())
                                .startsWith("Could not delete the catch-up marker of cancelled subscription failed-marker-delete.")
                                .endsWith("unless the subscription is cancelled again first. " + SharedStorageMessages.MARKER_WHEN_SEVERAL_INSTANCES_RUN_IT)));
    }

    /**
     * The logger binding keeps every line any test in the module logs, so a test reads only the lines that name its own
     * subscription id.
     */
    private static List<CapturingLoggerProvider.Logged> loggedFor(Level level, String subscriptionId) {
        return CapturingLoggerProvider.logged(CatchupThenPushSubscriptionModel.class, level).stream()
                .filter(logged -> logged.message().contains("subscription " + subscriptionId + ".") || logged.message().contains("subscription " + subscriptionId + ","))
                .toList();
    }

    private void failCatchUp(String subscriptionId) {
        PushSubscriptionModel liveFeed = new PushSubscriptionModel();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(failingReader(), liveFeed, null);
        models.add(model);

        model.subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty());
    }

    private void refuseCheckpointThatIsNotAMarker(String subscriptionId) {
        InMemoryReactivePositionOrderedReader store = new InMemoryReactivePositionOrderedReader();
        InMemoryCheckpointStorage checkpoints = new InMemoryCheckpointStorage();
        checkpoints.save(subscriptionId, new StringBasedCheckpoint("resume-token-xyz")).block(Duration.ofSeconds(5));
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, new PushSubscriptionModel(), checkpoints);
        models.add(model);

        model.subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty());
    }

    private void cancelWhoseMarkerDeleteFails(String subscriptionId) {
        InMemoryReactivePositionOrderedReader store = new InMemoryReactivePositionOrderedReader();
        store.append(List.of(CloudEventBuilder.v1().withId("1").withSource(URI.create("urn:occurrent:test")).withType("Created").build()));
        MarkerStoreWhoseDeleteFails markers = new MarkerStoreWhoseDeleteFails();
        CatchupThenPushSubscriptionModel model = new CatchupThenPushSubscriptionModel(store, new PushSubscriptionModel(), markers);
        models.add(model);
        model.subscribe(subscriptionId, null, StartAt.subscriptionModelDefault(), cloudEvent -> Mono.empty()).waitUntilStarted().block(Duration.ofSeconds(5));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(markers.read(subscriptionId).hasElement().block()).as("catch-up marker written").isTrue());

        model.cancelSubscription(subscriptionId);
    }

    private static final class MarkerStoreWhoseDeleteFails implements CheckpointStorage {
        private final InMemoryCheckpointStorage backing = new InMemoryCheckpointStorage();

        @Override
        public Mono<Checkpoint> read(String subscriptionId) {
            return backing.read(subscriptionId);
        }

        @Override
        public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition writeCondition) {
            return backing.save(subscriptionId, checkpoint, writeCondition);
        }

        @Override
        public Mono<Long> writeVersion(String subscriptionId) {
            return backing.writeVersion(subscriptionId);
        }

        @Override
        public Mono<Void> delete(String subscriptionId) {
            return Mono.error(new RuntimeException("The marker store is unavailable"));
        }
    }

    private static PositionOrderedReader failingReader() {
        return new PositionOrderedReader() {
            @Override
            public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
                return Flux.error(new IllegalStateException("replay boom"));
            }

            @Override
            public Mono<Long> currentPosition() {
                return Mono.just(0L);
            }

            @Override
            public boolean writesPosition() {
                return true;
            }
        };
    }
}
