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

package org.occurrent.springboot.mongo.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.springboot.reactor.ComposedCatchupModel;
import org.occurrent.subscription.AgnosticSubscriptionFilter;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.StartAt;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.occurrent.subscription.reactor.durable.ReactorDurableSubscriptionModel;
import org.occurrent.subscription.reactor.durable.catchup.ReactorCatchupSubscriptionModel;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.filter.Filter.type;

/**
 * The durable model the starter builds with its default properties, over a catch-up model over
 * {@code ReactorMongoSubscriptionModel}, saves the quiet position of a subscription that only has events it doesn't
 * match written after its last one. It saves at most once a minute by default, so the test waits that long.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
// The cloud event source has no default, so it's the only property set
@SpringBootTest(classes = ReactiveQuietPositionMongoTest.QuietPositionApplication.class, properties = "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:reactive-quiet-position-test")
@Import(ReactiveQuietPositionMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@DirtiesContext
@Timeout(180)
class ReactiveQuietPositionMongoTest {

    private static final String SUBSCRIPTION_ID = "reactive-quiet-position";
    private static final Duration DEFAULT_QUIET_POSITION_SAVE_INTERVAL = Duration.ofMinutes(1);
    private static final Duration WAIT_FOR_A_QUIET_SAVE = DEFAULT_QUIET_POSITION_SAVE_INTERVAL.plusSeconds(15);

    @Autowired
    private ReactorDurableSubscriptionModel subscriptionModel;

    @Autowired
    private ComposedCatchupModel composedCatchupModel;

    @Autowired
    private EventStore eventStore;

    @Autowired
    private CheckpointStorage checkpointStorage;

    private final AtomicBoolean writingOtherEvents = new AtomicBoolean();

    @AfterEach
    void stopWritingOtherEvents() {
        writingOtherEvents.set(false);
    }

    @Test
    void the_durable_model_of_the_starter_saves_the_quiet_position_of_a_subscription() {
        // Given
        assertThat(composedCatchupModel.catchupModelFor(subscriptionModel)).as("catch-up model the starter composed").containsInstanceOf(ReactorCatchupSubscriptionModel.class);
        CopyOnWriteArrayList<CloudEvent> delivered = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(SUBSCRIPTION_ID, AgnosticSubscriptionFilter.filter(type("Matching")), StartAt.subscriptionModelDefault(), event -> Mono.fromRunnable(() -> delivered.add(event)))
                .waitUntilStarted(ofSeconds(10)).block();
        write("Matching");
        await().atMost(ofSeconds(10)).until(() -> delivered.size() == 1 && storedPosition() != null);
        String positionOfTheMatchedEvent = storedPosition();

        // When
        writeOtherEventsUntilStopped();

        // Then
        long waitEnds = System.nanoTime() + WAIT_FOR_A_QUIET_SAVE.toNanos();
        await().atMost(WAIT_FOR_A_QUIET_SAVE.plusSeconds(10)).pollInterval(ofSeconds(1))
                .until(() -> !positionOfTheMatchedEvent.equals(storedPosition()) || System.nanoTime() > waitEnds);
        assertThat(storedPosition()).as("checkpoint stored a minute after only events the subscription doesn't match were written").isNotEqualTo(positionOfTheMatchedEvent);
        assertThat(delivered).as("events delivered to the subscription").hasSize(1);
    }

    private void writeOtherEventsUntilStopped() {
        writingOtherEvents.set(true);
        Thread.ofPlatform().daemon().start(() -> {
            while (writingOtherEvents.get()) {
                write("Other");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
    }

    private void write(String type) {
        CloudEvent event = CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("urn:occurrent:reactive-quiet-position-test"))
                .withType(type)
                .withTime(OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .withDataContentType("application/json")
                .withData("{}".getBytes(StandardCharsets.UTF_8))
                .build();
        eventStore.write(UUID.randomUUID().toString(), Flux.just(event)).block(ofSeconds(10));
    }

    private @Nullable String storedPosition() {
        return checkpointStorage.read(SUBSCRIPTION_ID).map(Checkpoint::asString).block(ofSeconds(10));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {

        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return ReplicaSetReadyMongoDBContainer.withDefaultVersion();
        }
    }

    @SpringBootApplication
    @EnableOccurrentReactive
    static class QuietPositionApplication {
    }
}
