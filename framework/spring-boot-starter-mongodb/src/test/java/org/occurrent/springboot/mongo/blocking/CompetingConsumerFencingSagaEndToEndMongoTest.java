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

package org.occurrent.springboot.mongo.blocking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.springboot.mongo.blocking.SagaAnnotationMongoTest.OrderPlaced;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.CheckpointWriteCondition;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoCheckpointStorage;
import org.occurrent.subscription.mongodb.spring.blocking.SpringMongoLeaseCompetingConsumerStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static java.time.Duration.ofSeconds;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Scenario 4 of the checkpoint fence's end-to-end proof (ADR 116, #665): the registrar-driven starter path. The
 * fence is wired by hand everywhere else this epic tests it. The starter reaches it differently, through
 * {@code SagaAnnotationRegistrar} (and {@code ProjectionAnnotationRegistrar}) pulling {@code CheckpointStorage} and
 * {@code CompetingConsumerStrategy} out of the application context and building a
 * {@code CatchupThenPushSubscriptionModel} itself, per subscription, at registration time. A wiring site missed
 * there writes {@code any()} silently and nothing but a real subscription running through it would catch that, which
 * is why hand-wired coverage elsewhere in this epic does not stand in for this test.
 * <p>
 * Reuses {@link SagaAnnotationMongoTest}'s application and domain. Its saga registers a real, competing-consumer
 * gated event subscription named {@code order-fulfillment}, confirmed by that class's own
 * {@code gates_the_timer_poller_with_a_lease_keyed_apart_from_the_event_subscription} test. Only one strategy bean
 * is in the context (the starter's own {@code occurrentCompetingConsumerStrategy}), so
 * {@code CompetingConsumerFencingWiringTest}'s "which bean wins" question does not apply here. This test is about
 * whether the wiring that bean reaches actually produces a fenced write against a real MongoDB, not about bean
 * resolution.
 */
@DisplayName("Checkpoint fence through the starter's saga registrar (ADR 116, #665)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = SagaAnnotationMongoTest.SagaApplication.class,
        properties = {
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:saga-annotation-test",
                "occurrent.saga.timer-poll-interval=150ms"
        }
)
@Import(SagaAnnotationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class CompetingConsumerFencingSagaEndToEndMongoTest {

    private static final String SUBSCRIPTION_ID = "order-fulfillment";
    private static final String CHECKPOINT_COLLECTION = "subscriptions";
    private static final String LOCKS_COLLECTION = "competing-consumer-locks";

    @Autowired
    private ApplicationService<SagaAnnotationMongoTest.OrderEvent> applicationService;
    @Autowired
    private MongoOperations mongoOperations;
    @Autowired
    private SpringMongoLeaseCompetingConsumerStrategy appStrategy;

    @Test
    void a_registrar_driven_checkpoint_write_is_stamped_with_a_real_token_and_refused_for_a_stale_holder() {
        // Given
        // The saga's registrar-driven subscription is live and has already checkpointed at least once, from an
        // event this test publishes itself rather than trusting startup timing.
        applicationService.execute("scenario4-seed", events -> List.of(new OrderPlaced("scenario4-seed")));
        await("the registrar-driven subscription checkpoints the seed event").atMost(ofSeconds(30))
                .untilAsserted(() -> assertThat(appStrategy.fencingToken(SUBSCRIPTION_ID)).isPresent());

        OptionalLong appToken = appStrategy.fencingToken(SUBSCRIPTION_ID);
        SpringMongoCheckpointStorage checkpointStorage = new SpringMongoCheckpointStorage(mongoOperations, CHECKPOINT_COLLECTION);
        await("the checkpoint document itself is stamped with the app's real token, not left absent by a missed wiring site (any())").atMost(ofSeconds(30))
                .untilAsserted(() -> assertThat(checkpointStorage.writeVersion(SUBSCRIPTION_ID)).isEqualTo(appToken));

        // When
        // A rival steals the subscription's lease, out of band, the same way the sibling tests in this epic do
        // (direct write, ADR 114's database clock, no waiting on any scheduled refresh).
        mongoOperations.getCollection(LOCKS_COLLECTION).updateOne(eq("_id", SUBSCRIPTION_ID), set("expiresAt", Instant.now().minusSeconds(2)));
        SpringMongoLeaseCompetingConsumerStrategy rivalStrategy = new SpringMongoLeaseCompetingConsumerStrategy.Builder(mongoOperations).collectionName(LOCKS_COLLECTION).build();
        try {
            assertThat(rivalStrategy.registerCompetingConsumer(SUBSCRIPTION_ID, "rival-" + UUID.randomUUID())).isTrue();
            OptionalLong rivalToken = rivalStrategy.fencingToken(SUBSCRIPTION_ID);
            assertThat(rivalToken).isPresent();
            assertThat(rivalToken.getAsLong()).as("a genuine takeover raises the fencing token").isGreaterThan(appToken.getAsLong());

            // The rival's own write, standing in for another node's redelivery landing first. This test's own
            // subject is the registrar wiring, not the race, and CheckpointFenceLeaseTransferTest already proves the
            // race outcome through live delivery.
            Checkpoint currentCheckpoint = requireNonNull(checkpointStorage.read(SUBSCRIPTION_ID));
            checkpointStorage.save(SUBSCRIPTION_ID, currentCheckpoint, CheckpointWriteCondition.notOlderThan(rivalToken.getAsLong()));

            // Then
            // The app's own, still-live registrar-driven subscription attempts a write under its stale token when
            // the next event arrives, and that write is refused. The stored version must stay at the rival's and
            // never revert to the app's stale one, even once the app has had time to try.
            applicationService.execute("scenario4-after-steal", events -> List.of(new OrderPlaced("scenario4-after-steal")));
            await("the stored checkpoint never regresses to the app's stale token").during(ofSeconds(5)).atMost(ofSeconds(30))
                    .untilAsserted(() -> assertThat(checkpointStorage.writeVersion(SUBSCRIPTION_ID)).isEqualTo(rivalToken));
        } finally {
            rivalStrategy.shutdown();
        }
    }
}
