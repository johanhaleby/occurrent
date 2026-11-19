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

package org.occurrent.dsl.saga.mongodb.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.dsl.saga.Saga;
import org.occurrent.dsl.saga.SagaEnvelope;
import org.occurrent.dsl.saga.SagaStateStore;
import org.occurrent.dsl.saga.SagaStatus;
import org.occurrent.dsl.saga.blocking.SagaRunner;
import org.occurrent.dsl.saga.blocking.SagaRunnerConfig;
import org.occurrent.dsl.saga.blocking.SagaSubscription;
import org.occurrent.dsl.saga.flow.Continuation;
import org.occurrent.dsl.saga.flow.FlowSaga;
import org.occurrent.dsl.saga.flow.FlowState;
import org.occurrent.dsl.saga.flow.StepCondition;
import org.occurrent.dsl.saga.internal.SagaEnvelopes;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Docker-based. A flow saga run by the saga runner on a {@link SpringMongoSagaStateStore} has its received events
 * written through the runner's converter. The second delivery reads the first one back from MongoDB, and its reaction
 * needs the start event that only the first delivery held.
 */
@Testcontainers
@DisplayNameGeneration(ReplaceUnderscores.class)
@Timeout(60)
class SpringMongoSagaStateStoreFlowRunnerTest {

    @Container
    private static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion().withReuse(true);

    private static final String REVIEW = "review-1";

    sealed interface ReviewEvent permits ReviewRequested, Approved {
        String eventId();

        String reviewId();
    }

    record ReviewRequested(String eventId, String reviewId) implements ReviewEvent {
    }

    record Approved(String eventId, String reviewId) implements ReviewEvent {
    }

    sealed interface ReviewCommand permits Publish {
    }

    record Publish(String reviewId) implements ReviewCommand {
    }

    private final InMemorySubscriptionModel subscriptionModel = new InMemorySubscriptionModel();
    private final InMemoryEventStore eventStore = new InMemoryEventStore(subscriptionModel);
    private final CloudEventConverter<ReviewEvent> converter = new JacksonCloudEventConverter.Builder<ReviewEvent>(new ObjectMapper(), URI.create("urn:test"))
            .idMapper(ReviewEvent::eventId).build();
    private final List<ReviewCommand> dispatched = new CopyOnWriteArrayList<>();
    private SagaStateStore<FlowState<ReviewEvent>> stateStore;
    private @Nullable SagaSubscription subscription;

    // FlowState.class erases its type argument, as in the other flow tests of this module
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Class<FlowState<ReviewEvent>> rawFlowStateType() {
        return (Class) FlowState.class;
    }

    @BeforeEach
    void createTheStore() {
        ConnectionString connectionString = new ConnectionString(mongoDBContainer.getReplicaSetUrl("saga-flow-runner-" + UUID.randomUUID().toString().substring(0, 8)));
        stateStore = new SpringMongoSagaStateStore<>(new MongoTemplate(MongoClients.create(connectionString), requireNonNull(connectionString.getDatabase())),
                "saga-flow-runner", rawFlowStateType(), converter);
    }

    @AfterEach
    void shutdown() {
        if (subscription != null) {
            subscription.close();
        }
        subscriptionModel.shutdown();
    }

    private Saga<ReviewEvent, FlowState<ReviewEvent>, ReviewCommand> saga() {
        return FlowSaga.<ReviewEvent, ReviewCommand>builder()
                .startsOn(ReviewRequested.class)
                .correlateAll(ReviewEvent::reviewId)
                .step("awaiting-approval", step -> step
                        .on(StepCondition.event(Approved.class), Continuation.end(),
                                received -> List.of(new Publish(received.initiating(ReviewRequested.class).reviewId()))))
                .build();
    }

    private void write(ReviewEvent event) {
        eventStore.write(REVIEW, converter.toCloudEvents(List.of(event)));
    }

    private Optional<SagaEnvelope<FlowState<ReviewEvent>>> find() {
        return SagaEnvelopes.of(stateStore, converter).find(REVIEW);
    }

    private SagaEnvelope<FlowState<ReviewEvent>> stored() {
        return find().orElseThrow();
    }

    @Test
    void the_received_events_of_the_first_delivery_round_trip_through_mongo_to_the_second_delivery() {
        subscription = SagaRunner.<ReviewEvent, ReviewCommand>agnostic(subscriptionModel, converter)
                .run("flow-runner", saga(), stateStore, dispatched::add, null, SagaRunnerConfig.defaults().withTimerPollInterval(Duration.ofHours(1)));
        subscription.waitUntilStarted();

        write(new ReviewRequested("e1", REVIEW));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Optional<SagaEnvelope<FlowState<ReviewEvent>>> found = find();
            assertThat(found).as("the first delivery is stored").isPresent();
            assertThat(found.orElseThrow().state().received()).as("and reads back from MongoDB with its received event")
                    .containsExactly(new ReviewRequested("e1", REVIEW));
        });
        write(new Approved("e2", REVIEW));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(dispatched)
                .as("the second delivery's reaction found the start event the first delivery stored")
                .containsExactly(new Publish(REVIEW)));
        assertAll(
                () -> assertThat(stored().status()).isEqualTo(SagaStatus.COMPLETED),
                () -> assertThat(stored().state().received()).containsExactly(new ReviewRequested("e1", REVIEW), new Approved("e2", REVIEW))
        );
    }
}
