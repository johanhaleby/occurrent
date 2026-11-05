/*
 *
 *  Copyright 2026 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.mongo.reactor;

import jakarta.annotation.PostConstruct;
import org.jspecify.annotations.Nullable;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.api.reactor.CheckpointStorage;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Boots an application twice against the same MongoDB data, closing the first context before the second boots, so that
 * a test sees what a restart does to a subscription and not what a pause does. Each boot writes the events named by
 * {@code test.before-subscribe} before any subscription registers and, from an {@link ApplicationRunner}, the events
 * named by {@code test.after-start}. The last of those is a marker: it is written after everything else, so once a
 * subscriber has received it, whatever it was going to deliver for the events before it has arrived and the whole
 * delivered list can be asserted.
 * <p>
 * The first boot writes {@code historic-1} and {@code historic-2} before the subscription registers and
 * {@code live-1} after, the second boot {@code while-down-1} before it registers and {@code live-2} after. The
 * subscription id doubles as the name of the database, so each scenario starts from an empty one.
 */
final class ApplicationRestart {

    // What has to have happened before the first boot is closed. A subscription that resumes from a stored position
    // resumes from whatever was stored when the context closed, and the position of an event is stored after its
    // handler returned, so closing on the delivery alone races the store.
    enum FirstBootEnds {
        // The marker reached the handler, which is all a subscription that does not resume from a stored position needs.
        WHEN_THE_MARKER_IS_DELIVERED,
        // The marker reached the handler and its checkpoint is what the checkpoint storage holds.
        WHEN_THE_MARKER_IS_CHECKPOINTED,
        // No event is written after the subscription starts, so there is no marker. The initial checkpoint is what the
        // checkpoint storage holds, which is later than the subscription being registered as running.
        WHEN_THE_INITIAL_CHECKPOINT_IS_STORED
    }

    // What a test asserts about a boot beyond the events its subscriber delivered.
    @FunctionalInterface
    interface BootCheck {
        void after(ConfigurableApplicationContext context, String marker);
    }

    static final BootCheck NOTHING = (context, marker) -> {
    };

    interface Recorder {
        List<String> names();

        @Nullable String checkpointOf(String name);
    }

    private final MongoDBContainer mongoDBContainer;
    private final Class<?> application;
    private final String capabilities;

    ApplicationRestart(MongoDBContainer mongoDBContainer, Class<?> application, String capabilities) {
        this.mongoDBContainer = mongoDBContainer;
        this.application = application;
        this.capabilities = capabilities;
    }

    <S extends Recorder> void run(String subscriptionId, Class<S> subscriberType, FirstBootEnds firstBootEnds,
                                  List<String> expectedFirstBoot, List<String> expectedSecondBoot) {
        run(subscriptionId, subscriberType, firstBootEnds, expectedFirstBoot, expectedSecondBoot, NOTHING);
    }

    <S extends Recorder> void run(String subscriptionId, Class<S> subscriberType, FirstBootEnds firstBootEnds,
                                  List<String> expectedFirstBoot, List<String> expectedSecondBoot, BootCheck bootCheck) {
        String firstBootAfterStart = firstBootEnds == FirstBootEnds.WHEN_THE_INITIAL_CHECKPOINT_IS_STORED ? "" : "live-1";
        ConfigurableApplicationContext firstBoot = SpringApplication.run(application,
                bootArgs(subscriptionId, "historic-1,historic-2", firstBootAfterStart));
        try {
            S subscriber = firstBoot.getBean(subscriberType);
            switch (firstBootEnds) {
                case WHEN_THE_MARKER_IS_DELIVERED -> awaitDelivered(subscriber, "live-1");
                case WHEN_THE_MARKER_IS_CHECKPOINTED -> {
                    awaitDelivered(subscriber, "live-1");
                    awaitCheckpointed(firstBoot, subscriptionId, subscriber, "live-1");
                }
                case WHEN_THE_INITIAL_CHECKPOINT_IS_STORED -> awaitInitialCheckpoint(firstBoot, subscriptionId);
            }
            assertThat(subscriber.names()).containsExactlyElementsOf(expectedFirstBoot);
            bootCheck.after(firstBoot, "live-1");
        } finally {
            firstBoot.close();
        }

        ConfigurableApplicationContext secondBoot = SpringApplication.run(application,
                bootArgs(subscriptionId, "while-down-1", "live-2"));
        try {
            S subscriber = secondBoot.getBean(subscriberType);
            awaitDelivered(subscriber, "live-2");
            assertThat(subscriber.names()).containsExactlyElementsOf(expectedSecondBoot);
            bootCheck.after(secondBoot, "live-2");
        } finally {
            secondBoot.close();
        }
    }

    static void awaitDelivered(Recorder subscriber, String marker) {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(subscriber.names()).contains(marker));
    }

    // The durable model registers the subscription as running before it writes the first position, so running does not
    // mean the position a restart resumes from is stored.
    static void awaitInitialCheckpoint(ConfigurableApplicationContext context, String subscriptionId) {
        CheckpointStorage storage = context.getBean(CheckpointStorage.class);
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(storage.read(subscriptionId).blockOptional()).isPresent());
    }

    // The checkpoint is stored after the handler returned, so the marker being delivered does not mean it is stored.
    static void awaitCheckpointed(ConfigurableApplicationContext context, String subscriptionId, Recorder subscriber, String marker) {
        CheckpointStorage storage = context.getBean(CheckpointStorage.class);
        String markerCheckpoint = subscriber.checkpointOf(marker);
        assertThat(markerCheckpoint).describedAs("the checkpoint %s was delivered with", marker).isNotNull();
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(storage.read(subscriptionId).blockOptional().map(Checkpoint::asString)).contains(markerCheckpoint));
    }

    // Booted with SpringApplication.run rather than @SpringBootTest, so there is no @ServiceConnection and the url is
    // passed as an argument.
    private String[] bootArgs(String databaseName, String beforeSubscribe, String afterStart) {
        return new String[]{
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=" + capabilities,
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName,
                "--test.before-subscribe=" + beforeSubscribe,
                "--test.after-start=" + afterStart
        };
    }

    static CloudEventConverter<TestEvent> converter(CloudEventTypeMapper<TestEvent> typeMapper, String source) {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create(source))
                .typeMapper(typeMapper)
                .idMapper(TestEvent::eventId)
                .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .build();
    }

    static class EventWriter implements ApplicationRunner {
        private final Consumer<String> write;
        private final List<String> beforeSubscribe;
        private final List<String> afterStart;

        EventWriter(Consumer<String> write, String beforeSubscribe, String afterStart) {
            this.write = write;
            this.beforeSubscribe = List.of(beforeSubscribe.split(","));
            this.afterStart = afterStart.isEmpty() ? List.of() : List.of(afterStart.split(","));
        }

        // Subscriptions register once every singleton exists, so these are written before any of them starts. A
        // subscription starting at the present can still receive what the same MongoClient wrote up to a second
        // earlier, so the pause keeps them out of reach of the NOW subscribers.
        @PostConstruct
        void writeBeforeSubscribe() throws InterruptedException {
            beforeSubscribe.forEach(write);
            Thread.sleep(1500);
        }

        @Override
        public void run(ApplicationArguments args) {
            afterStart.forEach(write);
        }
    }

    abstract static class RecordingSubscriber implements Recorder {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();
        private final ConcurrentHashMap<String, String> checkpoints = new ConcurrentHashMap<>();

        Mono<Void> record(TestEvent event, EventMetadata metadata) {
            metadata.getCheckpoint().ifPresent(checkpoint -> checkpoints.put(event.name(), checkpoint));
            received.add(event);
            return Mono.empty();
        }

        @Override
        public @Nullable String checkpointOf(String name) {
            return checkpoints.get(name);
        }

        @Override
        public List<String> names() {
            return received.stream().map(TestEvent::name).toList();
        }
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
