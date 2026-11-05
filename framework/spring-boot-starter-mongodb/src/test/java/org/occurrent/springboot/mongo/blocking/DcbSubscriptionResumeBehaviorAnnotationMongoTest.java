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

package org.occurrent.springboot.mongo.blocking;

import jakarta.annotation.PostConstruct;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.DcbSnapshotView;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.blocking.SnapshotStore;
import org.occurrent.dsl.snapshot.mongodb.spring.blocking.SpringMongoSnapshotStore;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.subscription.CatchupTimeCheckpoint;
import org.occurrent.subscription.Checkpoint;
import org.occurrent.subscription.GlobalCheckpoint;
import org.occurrent.subscription.StringBasedCheckpoint;
import org.occurrent.subscription.api.blocking.ReplayAwareSubscriptions;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.api.blocking.CheckpointStorage;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that {@code resumeBehavior} on {@link DcbSubscription} behaves differently across an actual application
 * restart, not just a pause. The annotation processor computes the {@code DcbStartAt} once, when the subscriber bean
 * is post-processed at startup, and that computation is a {@code DcbStartAt.dynamic(...)} supplier evaluated exactly
 * once inside the subscription model's {@code subscribe(...)} call. Pausing and resuming the same running model
 * (stop/start) never re-invokes that supplier, so {@code resumeBehavior} can only be observed by closing the Spring
 * context and booting a fresh one against the same durable MongoDB backing data, exactly like a real process restart.
 * <p>
 * Both boots share one MongoDB replica set but use isolated collections per scenario (see {@code bootArgs}), so the
 * scenarios below can run against the same container without interfering with each other despite the fixed,
 * compile-time-constant annotation attributes forcing one subscriber class per scenario.
 * <p>
 * The {@code NOW} scenarios and the {@link Snapshot} scenarios boot a separate application,
 * {@link NowRestartApplication}. It holds {@link DcbSubscription}s, DCB {@link Projection}s and {@link Snapshot}s
 * returning a {@code DcbSnapshotView}, which share the DCB start-position computation. Each boot
 * writes the events named by {@code test.before-subscribe} before any subscription registers and, from an
 * {@link ApplicationRunner}, the events named by {@code test.after-start}. The last of those is a marker: it is
 * written after everything else, so once an annotation has handled it, whatever it was going to handle for the events
 * before it has arrived and the whole handled list can be asserted.
 * <p>
 * A DCB snapshot folds every matching event it reads from the event store, so its saved state is the same whichever
 * events its subscription is delivered. The {@link Snapshot} scenarios therefore check two things per boot. One is how
 * many times the registrar read the stored snapshot, which it does once per delivered event. The other is the versions
 * it saved, which are the positions of the delivered events that were not already folded. A redelivery is read but not
 * saved.
 */
@DisplayName("DcbSubscription resumeBehavior across an application restart")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(120)
class DcbSubscriptionResumeBehaviorAnnotationMongoTest {

    // Booted directly with SpringApplication.run (not @SpringBootTest), so there is no @ServiceConnection to resolve
    // the container for the context. The url is passed as a boot argument instead, and getReplicaSetUrl() reports the
    // mapped host port, so nothing has to be pinned.
    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    @Test
    void default_resume_behavior_resumes_from_the_stored_position_after_restart_while_same_as_start_at_replays_from_the_beginning_again() {
        String[] args = bootArgs("dcb-resume-behavior");

        ConfigurableApplicationContext ctx1 = SpringApplication.run(FirstBootApplication.class, args);
        try {
            DefaultResumeSubscriber defaultSubscriber1 = ctx1.getBean(DefaultResumeSubscriber.class);
            SameAsStartAtBeginningSubscriber sameAsStartAtSubscriber1 = ctx1.getBean(SameAsStartAtBeginningSubscriber.class);
            SameAsStartAtPositionSubscriber positionSubscriber1 = ctx1.getBean(SameAsStartAtPositionSubscriber.class);

            // First boot: every scenario replays the same pre-existing history, each per its own start position.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(defaultSubscriber1.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2"));
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(sameAsStartAtSubscriber1.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2"));
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(positionSubscriber1.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2"));
        } finally {
            ctx1.close();
        }

        // The application is "down": a new event is appended while nothing is subscribed.
        ConfigurableApplicationContext ctx2 = SpringApplication.run(SecondBootApplication.class, args);
        try {
            DefaultResumeSubscriber defaultSubscriber2 = ctx2.getBean(DefaultResumeSubscriber.class);
            SameAsStartAtBeginningSubscriber sameAsStartAtSubscriber2 = ctx2.getBean(SameAsStartAtBeginningSubscriber.class);
            SameAsStartAtPositionSubscriber positionSubscriber2 = ctx2.getBean(SameAsStartAtPositionSubscriber.class);

            // DEFAULT: the fresh subscriber instance resumes from the durably stored position, it never sees the
            // historic events again, only the one event appended while the application was down.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(defaultSubscriber2.received()).extracting(TestEvent::name).containsExactly("while-down-1"));
            await().during(ofSeconds(2)).atMost(ofSeconds(5)).untilAsserted(() ->
                    assertThat(defaultSubscriber2.received()).extracting(TestEvent::name).containsExactly("while-down-1"));

            // SAME_AS_START_AT (BEGINNING): the fresh subscriber instance replays the entire history again, from
            // the beginning, exactly as it did on the first boot, plus the new event.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(sameAsStartAtSubscriber2.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2", "while-down-1"));

            // SAME_AS_START_AT (explicit startAtDcbPosition 0): replays again from that same position, not from
            // stored progress, so it sees the same history plus the new event.
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(positionSubscriber2.received()).extracting(TestEvent::name).containsExactly("historic-1", "historic-2", "while-down-1"));
        } finally {
            ctx2.close();
        }
    }

    enum NowAnnotated {
        DCB_SUBSCRIPTION(DcbSubscriptionNowDefault.class, DcbSubscriptionNowSameAsStartAt.class),
        DCB_PROJECTION(DcbProjectionNowDefault.class, DcbProjectionNowSameAsStartAt.class);

        final Class<? extends Handled> withDefault;
        final Class<? extends Handled> withSameAsStartAt;

        NowAnnotated(Class<? extends Handled> withDefault, Class<? extends Handled> withSameAsStartAt) {
            this.withDefault = withDefault;
            this.withSameAsStartAt = withSameAsStartAt;
        }
    }

    @ParameterizedTest
    @EnumSource(NowAnnotated.class)
    void now_with_default_resume_behavior_ignores_history_on_first_boot_and_handles_events_written_while_down_after_restart(NowAnnotated annotated) {
        restartNow("dcb-now-default-" + annotated.name().toLowerCase(), annotated.withDefault,
                List.of("live-1"),
                List.of("while-down-1", "live-2"));
    }

    @ParameterizedTest
    @EnumSource(NowAnnotated.class)
    void now_with_same_as_start_at_starts_at_the_present_again_after_restart_and_misses_events_written_while_down(NowAnnotated annotated) {
        restartNow("dcb-now-same-as-start-at-" + annotated.name().toLowerCase(), annotated.withSameAsStartAt,
                List.of("live-1"),
                List.of("live-2"));
    }

    // Pausing and resuming is not a restart. The subscription is NOW with SAME_AS_START_AT and a catch-up checkpoint is
    // planted while it is paused. A resume does not start over from storage, so historic-2 is not replayed from the
    // planted checkpoint before live-2.
    @Test
    void a_dcb_subscription_with_now_and_same_as_start_at_does_not_replay_from_a_catch_up_checkpoint_planted_while_paused_when_it_is_resumed() {
        try (ConfigurableApplicationContext context = SpringApplication.run(NowRestartApplication.class,
                nowBootArgs("dcb-now-same-as-start-at-paused", "historic-1,historic-2", "live-1"))) {
            DcbSubscriptionNowSameAsStartAt handler = context.getBean(DcbSubscriptionNowSameAsStartAt.class);
            awaitHandled(handler, "live-1");
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                    assertThat(handler.checkpointOf("live-1")).isNotNull());
            Checkpoint live = new StringBasedCheckpoint(handler.checkpointOf("live-1"));
            CheckpointStorage storage = context.getBean(CheckpointStorage.class);
            SubscriptionModel subscriptionModel = context.getBean(SubscriptionModel.class);

            subscriptionModel.pauseSubscription("dcb-now-sub-same");
            await().atMost(ofSeconds(10)).pollInterval(ofMillis(50)).until(() -> subscriptionModel.isPaused("dcb-now-sub-same"));
            storage.save("dcb-now-sub-same", GlobalCheckpoint.of(1, live, 0, 2));
            assertThat(GlobalCheckpoint.isGlobalCheckpoint(storage.read("dcb-now-sub-same"))).as("catch-up checkpoint").isTrue();
            subscriptionModel.resumeSubscription("dcb-now-sub-same").waitUntilStarted(ofSeconds(30));
            append(context.getBean(DcbEventStore.class), context.getBean(CloudEventConverter.class), new TestEvent("live-2"));
            awaitHandled(handler, "live-2");

            assertThat(handler.names()).containsExactly("live-1", "live-2");
        }
    }

    // A BEGINNING start that has caught up leaves a live checkpoint in storage. A catch-up that was interrupted leaves a
    // catch-up checkpoint, and a test cannot stop a catch-up halfway, so the one a catch-up stores after historic-1 is
    // written here with the live start the first boot left in storage. A start with DEFAULT continues from it, so
    // historic-2 is replayed and historic-1 is not.
    @Test
    void a_dcb_subscription_with_the_default_start_replays_from_the_catch_up_checkpoint_an_earlier_beginning_start_left_in_storage() {
        String id = "dcb-left-over";
        List<String> handledBySecondBoot;
        try (ConfigurableApplicationContext firstBoot = SpringApplication.run(NowRestartApplication.class,
                leftOverBootArgs("dcb-left-over-catch-up", "beginning", "historic-1,historic-2", ""))) {
            awaitHandled(firstBoot.getBean(DcbLeftOverBeginning.class), "historic-2");
            CheckpointStorage storage = firstBoot.getBean(CheckpointStorage.class);
            // The catch-up stores a checkpoint of its own while it replays, so one existing says nothing about the handover.
            AtomicReference<Checkpoint> live = new AtomicReference<>();
            await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
                Checkpoint stored = storage.read(id);
                assertThat(stored).isNotNull();
                assertThat(GlobalCheckpoint.isGlobalCheckpoint(stored) || CatchupTimeCheckpoint.isCatchupTimeCheckpoint(stored)).as("catch-up checkpoint %s", stored.asString()).isFalse();
                live.set(stored);
            });
            storage.save(id, GlobalCheckpoint.of(1, live.get(), 0, 2));
            assertThat(GlobalCheckpoint.isGlobalCheckpoint(storage.read(id))).as("catch-up checkpoint").isTrue();
        }
        try (ConfigurableApplicationContext secondBoot = SpringApplication.run(NowRestartApplication.class,
                leftOverBootArgs("dcb-left-over-catch-up", "default", "while-down-1", "live-2"))) {
            DcbLeftOverDefault handler = secondBoot.getBean(DcbLeftOverDefault.class);
            awaitHandled(handler, "live-2");
            handledBySecondBoot = handler.names();
        }

        assertThat(handledBySecondBoot).containsSubsequence("historic-2", "while-down-1", "live-2");
        assertThat(handledBySecondBoot).filteredOn(name -> name.startsWith("historic")).containsExactly("historic-2");
    }

    private static String[] leftOverBootArgs(String databaseName, String startsAt, String beforeSubscribe, String afterStart) {
        return Stream.concat(Arrays.stream(nowBootArgs(databaseName, beforeSubscribe, afterStart)), Stream.of("--test.left-over=" + startsAt)).toArray(String[]::new);
    }

    private static void restartNow(String databaseName, Class<? extends Handled> handlerType,
                                   List<String> expectedFirstBoot, List<String> expectedSecondBoot) {
        ConfigurableApplicationContext firstBoot = SpringApplication.run(NowRestartApplication.class,
                nowBootArgs(databaseName, "historic-1,historic-2", "live-1"));
        try {
            Handled handler = firstBoot.getBean(handlerType);
            awaitHandled(handler, "live-1");
            assertThat(handler.names()).containsExactlyElementsOf(expectedFirstBoot);
        } finally {
            firstBoot.close();
        }

        ConfigurableApplicationContext secondBoot = SpringApplication.run(NowRestartApplication.class,
                nowBootArgs(databaseName, "while-down-1", "live-2"));
        try {
            Handled handler = secondBoot.getBean(handlerType);
            awaitHandled(handler, "live-2");
            assertThat(handler.names()).containsExactlyElementsOf(expectedSecondBoot);
        } finally {
            secondBoot.close();
        }
    }

    // Positions run historic-1=1, historic-2=2, live-1=3, while-down-1=4, live-2=5. Reads count every delivery and saved
    // versions show which of them were new, so a redelivered live-1 cannot pass for while-down-1.
    @Test
    void a_dcb_snapshot_with_now_and_default_resume_behavior_is_not_delivered_history_on_first_boot_and_is_delivered_events_written_while_down_after_restart() {
        restartSnapshot("dcb-now-default-snapshot", DcbSnapshotNowDefault.class, "dcb-now-snapshot-default", 1, List.of(3L), 2, List.of(4L, 5L));
    }

    @Test
    void a_dcb_snapshot_with_now_and_same_as_start_at_is_delivered_only_the_present_again_after_restart_but_still_folds_events_written_while_down() {
        restartSnapshot("dcb-now-same-as-start-at-snapshot", DcbSnapshotNowSameAsStartAt.class, null, 1, List.of(3L), 1, List.of(5L));
    }

    @Test
    void a_dcb_snapshot_with_beginning_and_default_resume_behavior_is_delivered_history_once_and_only_what_is_new_after_restart() {
        restartSnapshot("dcb-beginning-default-snapshot", DcbSnapshotBeginningDefault.class, "dcb-beginning-snapshot-default", 3, List.of(1L, 2L, 3L), 2, List.of(4L, 5L));
    }

    @Test
    void a_dcb_snapshot_with_beginning_and_same_as_start_at_is_delivered_the_whole_history_again_after_restart() {
        restartSnapshot("dcb-beginning-same-as-start-at-snapshot", DcbSnapshotBeginningSameAsStartAt.class, null, 3, List.of(1L, 2L, 3L), 5, List.of(4L, 5L));
    }

    private static void restartSnapshot(String databaseName, Class<? extends DcbSnapshotHandled> handlerType,
                                        @Nullable String subscriptionIdThatStoresCheckpoint, int expectedReadsFirstBoot, List<Long> expectedSavedFirstBoot,
                                        int expectedReadsSecondBoot, List<Long> expectedSavedSecondBoot) {
        ConfigurableApplicationContext firstBoot = SpringApplication.run(NowRestartApplication.class,
                nowBootArgs(databaseName, "historic-1,historic-2", "live-1"));
        try {
            DcbSnapshotHandled handler = firstBoot.getBean(handlerType);
            awaitFolded(handler, "live-1");
            if (subscriptionIdThatStoresCheckpoint != null) {
                awaitCheckpointed(firstBoot, subscriptionIdThatStoresCheckpoint, "live-1");
            }
            assertDelivered(handler, expectedReadsFirstBoot, expectedSavedFirstBoot);
        } finally {
            firstBoot.close();
        }

        ConfigurableApplicationContext secondBoot = SpringApplication.run(NowRestartApplication.class,
                nowBootArgs(databaseName, "while-down-1", "live-2"));
        try {
            DcbSnapshotHandled handler = secondBoot.getBean(handlerType);
            awaitFolded(handler, "live-2");
            assertDelivered(handler, expectedReadsSecondBoot, expectedSavedSecondBoot);
            assertThat(handler.folded()).containsExactly("historic-1", "historic-2", "live-1", "while-down-1", "live-2");
        } finally {
            secondBoot.close();
        }
    }

    // The snapshot's checkpoint is stored after its handler returned, so live-1 being folded does not mean a restart
    // resumes after it. The SAME_AS_START_AT subscription beside it is delivered live-1 with the checkpoint of that event,
    // which is what the snapshot's storage holds once live-1 is stored.
    private static void awaitCheckpointed(ConfigurableApplicationContext context, String subscriptionId, String marker) {
        DcbSubscriptionNowSameAsStartAt delivery = context.getBean(DcbSubscriptionNowSameAsStartAt.class);
        CheckpointStorage storage = context.getBean(CheckpointStorage.class);
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(delivery.checkpointOf(marker)).as("checkpoint %s was delivered with", marker).isNotNull();
            Checkpoint stored = storage.read(subscriptionId);
            assertThat(stored).as("checkpoint of %s", subscriptionId).isNotNull();
            assertThat(stored.asString()).isEqualTo(delivery.checkpointOf(marker));
        });
    }

    // Held for a second so a delivery that arrives after the marker would fail the assertion.
    private static void assertDelivered(DcbSnapshotHandled handler, int expectedReads, List<Long> expectedSaved) {
        await().during(ofSeconds(1)).atMost(ofSeconds(5)).untilAsserted(() -> {
            assertThat(handler.reads()).isEqualTo(expectedReads);
            assertThat(handler.savedVersions()).containsExactlyElementsOf(expectedSaved);
        });
    }

    private static void awaitFolded(DcbSnapshotHandled handler, String marker) {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(handler.folded()).contains(marker));
    }

    private static void awaitHandled(Handled handler, String marker) {
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(handler.names()).contains(marker));
    }

    private static String[] nowBootArgs(String databaseName, String beforeSubscribe, String afterStart) {
        return Stream.concat(Arrays.stream(bootArgs(databaseName)), Stream.of(
                "--test.before-subscribe=" + beforeSubscribe,
                "--test.after-start=" + afterStart)).toArray(String[]::new);
    }

    private static String[] bootArgs(String databaseName) {
        return new String[]{
                "--spring.mongodb.uri=" + mongoDBContainer.getReplicaSetUrl(databaseName),
                "--spring.main.web-application-type=none",
                "--occurrent.event-store.capabilities=dcb",
                "--occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:" + databaseName
        };
    }

    // --- application boot classes ---

    @SpringBootApplication
    @EnableOccurrent
    static class FirstBootApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        // Appends the two historic events before any subscriber starts, so BEGINNING-based subscriptions replay them.
        @Bean
        HistoryAppender historyAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new HistoryAppender(dcbEventStore, cloudEventConverter);
        }

        @Bean
        @DependsOn("historyAppender")
        DefaultResumeSubscriber defaultResumeSubscriber() {
            return new DefaultResumeSubscriber();
        }

        @Bean
        @DependsOn("historyAppender")
        SameAsStartAtBeginningSubscriber sameAsStartAtBeginningSubscriber() {
            return new SameAsStartAtBeginningSubscriber();
        }

        @Bean
        @DependsOn("historyAppender")
        SameAsStartAtPositionSubscriber sameAsStartAtPositionSubscriber() {
            return new SameAsStartAtPositionSubscriber();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class SecondBootApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        // Appends the "while the application was down" event before any subscriber (re)starts.
        @Bean
        OfflineAppender offlineAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            return new OfflineAppender(dcbEventStore, cloudEventConverter);
        }

        @Bean
        @DependsOn("offlineAppender")
        DefaultResumeSubscriber defaultResumeSubscriber() {
            return new DefaultResumeSubscriber();
        }

        @Bean
        @DependsOn("offlineAppender")
        SameAsStartAtBeginningSubscriber sameAsStartAtBeginningSubscriber() {
            return new SameAsStartAtBeginningSubscriber();
        }

        @Bean
        @DependsOn("offlineAppender")
        SameAsStartAtPositionSubscriber sameAsStartAtPositionSubscriber() {
            return new SameAsStartAtPositionSubscriber();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class NowRestartApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return newConverter(typeMapper);
        }

        @Bean
        NowEventWriter nowEventWriter(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter,
                                      @Value("${test.before-subscribe}") String beforeSubscribe,
                                      @Value("${test.after-start}") String afterStart,
                                      MongoTemplate mongoOperations, ObjectProvider<SubscriptionModel> subscriptionModel,
                                      ObjectProvider<DcbSnapshotBeginningDefault> beginningDefault,
                                      ObjectProvider<DcbSnapshotBeginningSameAsStartAt> beginningSameAsStartAt) {
            // A BEGINNING snapshot replays the history. An event written while it replays is delivered by the replay and can
            // be delivered by the live feed too, so the events written after start wait until the last history event is folded.
            Runnable replayIsOver = () -> {
                String lastHistory = List.of(beforeSubscribe.split(",")).getLast();
                ReplayAwareSubscriptions replayAware = ReplayAwareSubscriptions.findIn(subscriptionModel.getObject()).orElseThrow();
                Map<String, DcbSnapshotHandled> replaying = Map.of(
                        "dcb-beginning-snapshot-default", beginningDefault.getObject(),
                        "dcb-beginning-snapshot-same", beginningSameAsStartAt.getObject());
                replaying.forEach((id, handler) -> await().atMost(ofSeconds(30)).pollInterval(ofMillis(50)).untilAsserted(() -> {
                    assertThat(handler.folded()).contains(lastHistory);
                    assertThat(replayAware.isCatchingUp(id)).isFalse();
                }));
            };
            return new NowEventWriter(dcbEventStore, cloudEventConverter, beforeSubscribe, afterStart, mongoOperations, replayIsOver);
        }

        @Bean
        DcbSubscriptionNowDefault dcbSubscriptionNowDefault() {
            return new DcbSubscriptionNowDefault();
        }

        @Bean
        @ConditionalOnProperty(name = "test.left-over", havingValue = "beginning")
        DcbLeftOverBeginning dcbLeftOverBeginning() {
            return new DcbLeftOverBeginning();
        }

        @Bean
        @ConditionalOnProperty(name = "test.left-over", havingValue = "default")
        DcbLeftOverDefault dcbLeftOverDefault() {
            return new DcbLeftOverDefault();
        }

        @Bean
        DcbSubscriptionNowSameAsStartAt dcbSubscriptionNowSameAsStartAt() {
            return new DcbSubscriptionNowSameAsStartAt();
        }

        @Bean
        DcbProjectionNowDefault dcbProjectionNowDefault() {
            return new DcbProjectionNowDefault();
        }

        @Bean
        DcbProjectionNowSameAsStartAt dcbProjectionNowSameAsStartAt() {
            return new DcbProjectionNowSameAsStartAt();
        }

        @Bean
        RecordingSnapshotStore dcbSnapshotNowDefaultStore(MongoOperations mongoOperations) {
            return new RecordingSnapshotStore(mongoOperations, "dcb-now-snapshot-default");
        }

        @Bean
        DcbSnapshotNowDefault dcbSnapshotNowDefault(@Qualifier("dcbSnapshotNowDefaultStore") RecordingSnapshotStore store) {
            return new DcbSnapshotNowDefault(store);
        }

        @Bean
        RecordingSnapshotStore dcbSnapshotNowSameAsStartAtStore(MongoOperations mongoOperations) {
            return new RecordingSnapshotStore(mongoOperations, "dcb-now-snapshot-same");
        }

        @Bean
        DcbSnapshotNowSameAsStartAt dcbSnapshotNowSameAsStartAt(@Qualifier("dcbSnapshotNowSameAsStartAtStore") RecordingSnapshotStore store) {
            return new DcbSnapshotNowSameAsStartAt(store);
        }

        @Bean
        RecordingSnapshotStore dcbSnapshotBeginningDefaultStore(MongoOperations mongoOperations) {
            return new RecordingSnapshotStore(mongoOperations, "dcb-beginning-snapshot-default");
        }

        @Bean
        DcbSnapshotBeginningDefault dcbSnapshotBeginningDefault(@Qualifier("dcbSnapshotBeginningDefaultStore") RecordingSnapshotStore store) {
            return new DcbSnapshotBeginningDefault(store);
        }

        @Bean
        RecordingSnapshotStore dcbSnapshotBeginningSameAsStartAtStore(MongoOperations mongoOperations) {
            return new RecordingSnapshotStore(mongoOperations, "dcb-beginning-snapshot-same");
        }

        @Bean
        DcbSnapshotBeginningSameAsStartAt dcbSnapshotBeginningSameAsStartAt(@Qualifier("dcbSnapshotBeginningSameAsStartAtStore") RecordingSnapshotStore store) {
            return new DcbSnapshotBeginningSameAsStartAt(store);
        }
    }

    private static CloudEventConverter<TestEvent> newConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
        return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), URI.create("urn:occurrent:dcb-resume-behavior"))
                .typeMapper(typeMapper)
                .idMapper(TestEvent::eventId)
                .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                .build();
    }

    // --- appenders ---

    static class HistoryAppender {
        private final DcbEventStore dcbEventStore;
        private final CloudEventConverter<TestEvent> cloudEventConverter;

        HistoryAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            this.dcbEventStore = dcbEventStore;
            this.cloudEventConverter = cloudEventConverter;
        }

        @PostConstruct
        void appendHistory() {
            append(dcbEventStore, cloudEventConverter, new TestEvent("historic-1"), new TestEvent("historic-2"));
        }
    }

    static class OfflineAppender {
        private final DcbEventStore dcbEventStore;
        private final CloudEventConverter<TestEvent> cloudEventConverter;

        OfflineAppender(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter) {
            this.dcbEventStore = dcbEventStore;
            this.cloudEventConverter = cloudEventConverter;
        }

        @PostConstruct
        void appendWhileDown() {
            append(dcbEventStore, cloudEventConverter, new TestEvent("while-down-1"));
        }
    }

    static class NowEventWriter implements ApplicationRunner {
        private final DcbEventStore dcbEventStore;
        private final CloudEventConverter<TestEvent> cloudEventConverter;
        private final List<String> beforeSubscribe;
        private final List<String> afterStart;
        private final MongoTemplate mongoOperations;
        private final Runnable beforeAfterStart;

        NowEventWriter(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> cloudEventConverter, String beforeSubscribe, String afterStart, MongoTemplate mongoOperations, Runnable beforeAfterStart) {
            this.beforeAfterStart = beforeAfterStart;
            this.dcbEventStore = dcbEventStore;
            this.cloudEventConverter = cloudEventConverter;
            this.beforeSubscribe = List.of(beforeSubscribe.split(","));
            this.afterStart = List.of(afterStart.split(","));
            this.mongoOperations = mongoOperations;
        }

        // Subscriptions register once every singleton exists, so these are written before any of them starts. The check
        // confirms that a NOW subscription would open after them.
        @PostConstruct
        void writeBeforeSubscribe() {
            beforeSubscribe.forEach(this::write);
            HistoryBeforeThePresent.awaitedWith(mongoOperations);
        }

        @Override
        public void run(ApplicationArguments args) {
            beforeAfterStart.run();
            afterStart.forEach(this::write);
        }

        private void write(String name) {
            append(dcbEventStore, cloudEventConverter, new TestEvent(name));
        }
    }

    private static void append(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter, TestEvent... events) {
        List<io.cloudevents.CloudEvent> cloudEvents = converter.toCloudEvents(List.of(events))
                .stream()
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse("test:resume-behavior"))))
                .toList();
        dcbEventStore.append(cloudEvents);
    }

    // --- subscribers (one per scenario, since the annotation attributes are compile-time constants) ---

    static class DefaultResumeSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = "dcb-resume-default", startAt = StartPosition.BEGINNING, resumeBehavior = ResumeBehavior.DEFAULT)
        void onEvent(TestEvent event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class SameAsStartAtBeginningSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = "dcb-resume-same-as-start-at-beginning", startAt = StartPosition.BEGINNING, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void onEvent(TestEvent event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    static class SameAsStartAtPositionSubscriber {
        private final CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        @DcbSubscription(id = "dcb-resume-same-as-start-at-position", startAtDcbPosition = 0, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void onEvent(TestEvent event) {
            received.add(event);
        }

        List<TestEvent> received() {
            return received;
        }
    }

    interface Handled {
        List<String> names();
    }

    abstract static class Recording implements Handled {
        private final CopyOnWriteArrayList<String> handled = new CopyOnWriteArrayList<>();

        void record(String name) {
            handled.add(name);
        }

        @Override
        public List<String> names() {
            return List.copyOf(handled);
        }
    }

    static class DcbSubscriptionNowDefault extends Recording {
        @DcbSubscription(id = "dcb-now-sub-default", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        void onEvent(TestEvent event) {
            record(event.name());
        }
    }

    // Only the one named by test.left-over exists in a boot, so the same subscription id is started at BEGINNING by one
    // boot and with no start position by the next, as when the annotation is edited between two deployments.
    static class DcbLeftOverBeginning extends Recording {
        @DcbSubscription(id = "dcb-left-over", startAt = StartPosition.BEGINNING, resumeBehavior = ResumeBehavior.DEFAULT)
        void onEvent(TestEvent event) {
            record(event.name());
        }
    }

    static class DcbLeftOverDefault extends Recording {
        @DcbSubscription(id = "dcb-left-over")
        void onEvent(TestEvent event) {
            record(event.name());
        }
    }

    static class DcbSubscriptionNowSameAsStartAt extends Recording {
        private final ConcurrentHashMap<String, String> checkpoints = new ConcurrentHashMap<>();

        @DcbSubscription(id = "dcb-now-sub-same", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        void onEvent(TestEvent event, EventMetadata metadata) {
            metadata.getCheckpoint().ifPresent(checkpoint -> checkpoints.put(event.name(), checkpoint));
            record(event.name());
        }

        @Nullable String checkpointOf(String name) {
            return checkpoints.get(name);
        }
    }

    static class DcbProjectionNowDefault extends Recording {
        @Projection(id = "dcb-now-projection-default", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        DcbProjection<Seen, TestEvent, String> projection() {
            return recordingProjection(this::record);
        }
    }

    static class DcbProjectionNowSameAsStartAt extends Recording {
        @Projection(id = "dcb-now-projection-same", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        DcbProjection<Seen, TestEvent, String> projection() {
            return recordingProjection(this::record);
        }
    }

    // --- DCB @Snapshot ---

    static class DcbSnapshotNowDefault extends DcbSnapshotHandled {
        DcbSnapshotNowDefault(RecordingSnapshotStore store) {
            super(store);
        }

        @Snapshot(id = "dcb-now-snapshot-default", storeName = "dcbSnapshotNowDefaultStore", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        DcbSnapshotView<Folded, TestEvent> snapshot() {
            return foldingSnapshot();
        }
    }

    static class DcbSnapshotNowSameAsStartAt extends DcbSnapshotHandled {
        DcbSnapshotNowSameAsStartAt(RecordingSnapshotStore store) {
            super(store);
        }

        @Snapshot(id = "dcb-now-snapshot-same", storeName = "dcbSnapshotNowSameAsStartAtStore", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        DcbSnapshotView<Folded, TestEvent> snapshot() {
            return foldingSnapshot();
        }
    }

    static class DcbSnapshotBeginningDefault extends DcbSnapshotHandled {
        DcbSnapshotBeginningDefault(RecordingSnapshotStore store) {
            super(store);
        }

        @Snapshot(id = "dcb-beginning-snapshot-default", storeName = "dcbSnapshotBeginningDefaultStore", startAt = StartPosition.BEGINNING, resumeBehavior = ResumeBehavior.DEFAULT)
        DcbSnapshotView<Folded, TestEvent> snapshot() {
            return foldingSnapshot();
        }
    }

    static class DcbSnapshotBeginningSameAsStartAt extends DcbSnapshotHandled {
        DcbSnapshotBeginningSameAsStartAt(RecordingSnapshotStore store) {
            super(store);
        }

        @Snapshot(id = "dcb-beginning-snapshot-same", storeName = "dcbSnapshotBeginningSameAsStartAtStore", startAt = StartPosition.BEGINNING, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        DcbSnapshotView<Folded, TestEvent> snapshot() {
            return foldingSnapshot();
        }
    }

    abstract static class DcbSnapshotHandled {
        private final RecordingSnapshotStore store;

        DcbSnapshotHandled(RecordingSnapshotStore store) {
            this.store = store;
        }

        int reads() {
            return store.reads();
        }

        List<Long> savedVersions() {
            return store.savedVersions();
        }

        List<String> folded() {
            return store.folded();
        }
    }

    // The snapshot is stored in MongoDB, so what the first boot folded is still there on the second. The reads and the
    // saved versions are per boot, since each boot has its own store bean.
    static class RecordingSnapshotStore implements SnapshotStore<Folded> {
        private static final String KEY = DcbSnapshotKeys.canonicalKey(DcbCriteria.tags(Tag.parse("test:resume-behavior")));

        private final SnapshotStore<Folded> delegate;
        private final AtomicInteger reads = new AtomicInteger();
        private final CopyOnWriteArrayList<Long> savedVersions = new CopyOnWriteArrayList<>();

        RecordingSnapshotStore(MongoOperations mongoOperations, String snapshotId) {
            this.delegate = new SpringMongoSnapshotStore<>(mongoOperations, Folded.class, "occurrent-snapshot-" + snapshotId);
        }

        @Override
        public Optional<org.occurrent.dsl.snapshot.Snapshot<Folded>> findLatest(String key) {
            reads.incrementAndGet();
            return delegate.findLatest(key);
        }

        @Override
        public void save(String key, org.occurrent.dsl.snapshot.Snapshot<Folded> snapshot) {
            delegate.save(key, snapshot);
            savedVersions.add(snapshot.version());
        }

        int reads() {
            return reads.get();
        }

        List<Long> savedVersions() {
            return List.copyOf(savedVersions);
        }

        // Read from the delegate so that looking at the state is not counted as a read.
        List<String> folded() {
            return delegate.findLatest(KEY).map(snapshot -> snapshot.state().names()).orElse(List.of());
        }
    }

    private static DcbSnapshotView<Folded, TestEvent> foldingSnapshot() {
        return new DcbSnapshotView<>(
                SnapshotView.<Folded, TestEvent>builder(new Folded(List.of()))
                        .on(TestEvent.class, (state, event) -> new Folded(Stream.concat(state.names().stream(), Stream.of(event.name())).toList()))
                        .build(),
                DcbCriteria.tags(Tag.parse("test:resume-behavior")));
    }

    record Folded(List<String> names) {
    }

    private static DcbProjection<Seen, TestEvent, String> recordingProjection(Consumer<String> onEvent) {
        var projection = org.occurrent.dsl.projection.Projection.<Seen, TestEvent, String>builder(new Seen("none"))
                .id(TestEvent::name)
                .on(TestEvent.class, (state, event) -> {
                    onEvent.accept(event.name());
                    return new Seen(event.name());
                })
                .build();
        return new DcbProjection<>(projection, DcbCriteria.tags(Tag.parse("test:resume-behavior")));
    }

    record Seen(@Id String id) {
    }

    record TestEvent(String eventId, Date timestamp, String name) {
        TestEvent(String name) {
            this(UUID.randomUUID().toString(), new Date(), name);
        }
    }
}
