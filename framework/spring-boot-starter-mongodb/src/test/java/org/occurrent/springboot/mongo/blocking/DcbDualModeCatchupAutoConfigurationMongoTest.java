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

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson3.JacksonCloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.dcb.TagGenerator;
import org.occurrent.condition.Condition;
import org.occurrent.dsl.dcb.blocking.DcbSubscriptions;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStore;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.*;
import org.occurrent.subscription.api.blocking.Subscription;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.durable.catchup.StartAtTime;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves that in a STREAM-and-DCB combined-capability context both a stream subscription and a DCB
 * subscription can replay history written before they subscribe, from the single auto-configured
 * {@link SubscriptionModel}. Prior to the dual-mode {@code CatchupSubscriptionModel} constructor a
 * combined-capability app only got stream catch-up; DCB subscriptions started live.
 */
@DisplayName("Dual-mode catch-up auto-configuration (STREAM + DCB)")
@DisplayNameGeneration(ReplaceUnderscores.class)
@SpringBootTest(
        classes = DcbDualModeCatchupAutoConfigurationMongoTest.CombinedModeApplication.class,
        properties = {
                "occurrent.event-store.capabilities=stream,dcb",
                "occurrent.cloud-event-converter.cloud-event-source=urn:occurrent:dual-mode-catchup-test"
        }
)
@Import(DcbDualModeCatchupAutoConfigurationMongoTest.MongoDbContainerConfiguration.class)
@Testcontainers
@Timeout(60)
class DcbDualModeCatchupAutoConfigurationMongoTest {

    private static final String STREAM_TAG = "kind:stream";
    private static final String DCB_TAG = "kind:dcb";
    private static final URI SOURCE = URI.create("urn:occurrent:dual-mode-catchup-test");

    @Autowired
    private ApplicationService<TestEvent> applicationService;

    @Autowired
    private DcbEventStore dcbEventStore;

    @Autowired
    private DcbSubscriptions<TestEvent> dcbSubscriptions;

    @Autowired
    private SubscriptionModel subscriptionModel;

    @Autowired
    private CloudEventConverter<TestEvent> cloudEventConverter;

    @Autowired
    private EventStoreQueries eventStoreQueries;

    @Test
    void stream_subscription_replays_historic_stream_events_appended_before_subscribe() {
        // Given - stream events written before the subscription starts
        TestEvent historic1 = streamEvent("s-historic-1");
        TestEvent historic2 = streamEvent("s-historic-2");
        appendStream(historic1, historic2);

        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        // When - stream subscription catches up from the beginning of time
        subscriptionModel.subscribe(
                        "stream-catchup-" + UUID.randomUUID(),
                        StartAtTime.beginningOfTime(),
                        ce -> {
                            TestEvent event = cloudEventConverter.toDomainEvent(ce);
                            if (STREAM_TAG.equals(ce.getSubject())) {
                                received.add(event);
                            }
                        })
                .waitUntilStarted();

        // Then - historic stream events are delivered via catch-up
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(historic1, historic2));
    }

    @Test
    void dcb_subscription_replays_historic_dcb_events_appended_before_subscribe() {
        // Given - DCB events written before the subscription starts
        TestEvent historic1 = dcbEvent("d-historic-1");
        TestEvent historic2 = dcbEvent("d-historic-2");
        TestEvent historic3 = dcbEvent("d-historic-3");
        appendDcb(historic1, historic2, historic3);

        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        // When - DCB subscription catches up from position 0
        dcbSubscriptions
                .subscribe(
                        "dcb-catchup-" + UUID.randomUUID(),
                        DcbCriteria.tags(Tag.parse(DCB_TAG)),
                        DcbStartAt.beginning(),
                        received::add)
                .waitUntilStarted();

        // Then - historic DCB events are delivered via DCB catch-up
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(historic1, historic2, historic3));
    }

    @Test
    void both_stream_and_dcb_subscriptions_replay_their_respective_histories_from_one_wired_model() {
        // Given - events of both kinds written before any subscription starts
        TestEvent streamHistoric = streamEvent("both-stream-historic");
        TestEvent dcbHistoric = dcbEvent("both-dcb-historic");
        appendStream(streamHistoric);
        appendDcb(dcbHistoric);

        CopyOnWriteArrayList<TestEvent> streamReceived = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<TestEvent> dcbReceived = new CopyOnWriteArrayList<>();

        // When - both subscriptions start after the events are already written
        subscriptionModel.subscribe(
                        "both-stream-" + UUID.randomUUID(),
                        StartAtTime.beginningOfTime(),
                        ce -> {
                            if (STREAM_TAG.equals(ce.getSubject())) {
                                streamReceived.add(cloudEventConverter.toDomainEvent(ce));
                            }
                        })
                .waitUntilStarted();

        dcbSubscriptions
                .subscribe(
                        "both-dcb-" + UUID.randomUUID(),
                        DcbCriteria.tags(Tag.parse(DCB_TAG)),
                        DcbStartAt.beginning(),
                        dcbReceived::add)
                .waitUntilStarted();

        // Then - each subscription receives its own historical events, not the other kind
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(streamReceived).contains(streamHistoric);
            assertThat(dcbReceived).contains(dcbHistoric);
        });
    }

    @Test
    void dcb_subscription_does_not_receive_stream_only_events() {
        // Given
        TestEvent streamOnly = streamEvent("noise-stream");
        TestEvent dcbOnly = dcbEvent("signal-dcb");
        appendStream(streamOnly);
        appendDcb(dcbOnly);

        CopyOnWriteArrayList<TestEvent> dcbReceived = new CopyOnWriteArrayList<>();

        // When
        dcbSubscriptions
                .subscribe(
                        "isolation-dcb-" + UUID.randomUUID(),
                        DcbCriteria.tags(Tag.parse(DCB_TAG)),
                        DcbStartAt.beginning(),
                        dcbReceived::add)
                .waitUntilStarted();

        // Then - the DCB subscription receives only the DCB event, not the stream-only one
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(dcbReceived).contains(dcbOnly).doesNotContain(streamOnly));
    }

    @Test
    void stream_subscription_does_not_receive_dcb_only_events_during_catchup() {
        // Given - one stream-only event and one DCB-tagged event written before the subscription starts
        TestEvent streamOnly = streamEvent("signal-stream");
        TestEvent dcbOnly = dcbEvent("noise-dcb");
        appendStream(streamOnly);
        appendDcb(dcbOnly);

        // Every delivered event is collected without any test-side filter, so a leaked DCB event would show up here.
        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        // When - a stream subscription with no filter catches up from the beginning of time
        subscriptionModel.subscribe(
                        "stream-no-filter-catchup-" + UUID.randomUUID(),
                        StartAtTime.beginningOfTime(),
                        ce -> received.add(cloudEventConverter.toDomainEvent(ce)))
                .waitUntilStarted();

        // Then - only the stream event is replayed, the DCB event is excluded by the capability guard
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(streamOnly).doesNotContain(dcbOnly));
    }

    @Test
    void stream_subscription_with_explicit_filter_does_not_receive_dcb_only_events_during_catchup() {
        // Given - a stream and a DCB event that both match the caller's explicit source filter
        TestEvent streamOnly = streamEvent("filtered-stream");
        TestEvent dcbOnly = dcbEvent("filtered-dcb");
        appendStream(streamOnly);
        appendDcb(dcbOnly);

        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        // When - a stream subscription with an explicit StreamSubscriptionFilter (that also matches the DCB event)
        // catches up from the beginning of time
        StreamSubscriptionFilter filter = StreamSubscriptionFilter.filter(Filter.source(SOURCE));
        subscriptionModel.subscribe(
                        "stream-explicit-filter-catchup-" + UUID.randomUUID(),
                        filter,
                        StartAtTime.beginningOfTime(),
                        ce -> received.add(cloudEventConverter.toDomainEvent(ce)))
                .waitUntilStarted();

        // Then - the DCB event is still excluded even though the caller's own filter would have matched it
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(streamOnly).doesNotContain(dcbOnly));
    }

    @Test
    void stream_subscription_does_not_receive_dcb_only_events_during_live_delivery() {
        // Given - one historic stream event so the subscription catches up and then goes live
        TestEvent historic = streamEvent("live-stream-historic");
        appendStream(historic);

        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        subscriptionModel.subscribe(
                        "stream-live-guard-" + UUID.randomUUID(),
                        StartAtTime.beginningOfTime(),
                        ce -> received.add(cloudEventConverter.toDomainEvent(ce)))
                .waitUntilStarted();

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(historic));

        // When - after handover to live, a DCB event and then a stream event arrive
        TestEvent liveDcb = dcbEvent("live-dcb-should-be-excluded");
        TestEvent liveStream = streamEvent("live-stream-should-be-delivered");
        appendDcb(liveDcb);
        appendStream(liveStream);

        // Then - the live stream event is delivered but the live DCB event never is, proving the guard covers the
        // live handover phase and not just the replay. Asserting on the stream event first gives the DCB event ample
        // time to have leaked through if the guard were missing.
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(historic, liveStream));
        assertThat(received).doesNotContain(liveDcb);
    }

    @Test
    void plain_event_store_queries_on_the_same_dual_capability_store_still_return_dcb_events() {
        // Given - a stream event and a DCB event on the dual-capability store
        TestEvent streamOnly = streamEvent("neutral-stream");
        TestEvent dcbOnly = dcbEvent("neutral-dcb");
        appendStream(streamOnly);
        appendDcb(dcbOnly);

        // When - querying the neutral EventStoreQueries layer directly with Filter.all(), bypassing
        // StreamCatchupSubscriptionModel entirely
        List<TestEvent> queried = eventStoreQueries.query(Filter.all())
                .map(cloudEventConverter::toDomainEvent)
                .toList();

        // Then - the neutral layer is unaffected by the stream-capability guard and returns both events, proving the
        // fix is scoped to StreamCatchupSubscriptionModel and did not leak into the generic query layer
        assertThat(queried).contains(streamOnly, dcbOnly);
    }

    @Test
    void dcb_subscription_delivers_live_event_after_catch_up_completes() {
        // Given - one historic event before subscribe, one live event after
        TestEvent historic = dcbEvent("live-test-historic");
        appendDcb(historic);

        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        dcbSubscriptions
                .subscribe(
                        "live-dcb-" + UUID.randomUUID(),
                        DcbCriteria.tags(Tag.parse(DCB_TAG)),
                        DcbStartAt.beginning(),
                        received::add)
                .waitUntilStarted();

        await().atMost(ofSeconds(20)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(historic));

        // When - a live event arrives after catch-up
        TestEvent live = dcbEvent("live-test-live");
        appendDcb(live);

        // Then - the live event is delivered without duplication
        await().atMost(ofSeconds(20)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(received).contains(historic, live);
            assertThat(received).doesNotHaveDuplicates();
        });
    }

    @Test
    void neutral_subscription_receives_both_stream_and_dcb_historic_events_during_catchup() {
        // Given - one stream event and one DCB event written before the neutral subscription starts
        TestEvent streamHistoric = streamEvent("neutral-catchup-stream-historic");
        TestEvent dcbHistoric = dcbEvent("neutral-catchup-dcb-historic");
        appendStream(streamHistoric);
        appendDcb(dcbHistoric);

        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        // When - a capability-agnostic subscription (AgnosticSubscriptionFilter, no capability scope) catches up from
        // the unified global position 0
        subscriptionModel.subscribe(
                        "neutral-catchup-" + UUID.randomUUID(),
                        AgnosticSubscriptionFilter.filter(Filter.type(cloudEventConverter.getCloudEventType(TestEvent.class))),
                        StartAt.checkpoint(GlobalCheckpoint.of(0)),
                        ce -> received.add(cloudEventConverter.toDomainEvent(ce)))
                .waitUntilStarted();

        // Then - both the stream-written and the DCB-appended event are delivered, unlike a plain stream subscription
        // (see stream_subscription_does_not_receive_dcb_only_events_during_catchup) which would exclude the DCB one
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(streamHistoric, dcbHistoric));
    }

    @Test
    void neutral_subscription_receives_both_stream_and_dcb_events_live_after_handover() {
        // Given - one historic stream event so the subscription has something to catch up on before going live
        TestEvent historic = streamEvent("neutral-live-historic");
        appendStream(historic);

        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        Subscription subscription = subscriptionModel.subscribe(
                        "neutral-live-" + UUID.randomUUID(),
                        AgnosticSubscriptionFilter.filter(Filter.type(cloudEventConverter.getCloudEventType(TestEvent.class))),
                        StartAt.checkpoint(GlobalCheckpoint.of(0)),
                        ce -> received.add(cloudEventConverter.toDomainEvent(ce)));
        subscription.waitUntilStarted();

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(historic));

        // When - after catch-up has handed over to live delivery, a stream event and a DCB event are both appended
        TestEvent liveStream = streamEvent("neutral-live-stream");
        TestEvent liveDcb = dcbEvent("neutral-live-dcb");
        appendStream(liveStream);
        appendDcb(liveDcb);

        // Then - both live events are delivered to the same neutral subscription
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(historic, liveStream, liveDcb));
    }

    @Test
    void neutral_subscription_resumes_from_global_checkpoint_across_restart_without_missing_events() {
        // Given - a first-generation event of each capability, delivered to a neutral subscription that is then
        // paused (not cancelled: cancelSubscription deletes the durable checkpoint, which would defeat this test).
        // Pausing and resuming the live delegate while keeping the checkpoint in storage is what a subscription with
        // the same id resuming after a genuine application restart also does, since the checkpoint lives in Mongo
        // independent of the in-process subscription object.
        String subscriptionId = "neutral-resume-" + UUID.randomUUID();
        TestEvent firstStream = streamEvent("neutral-resume-stream-1");
        TestEvent firstDcb = dcbEvent("neutral-resume-dcb-1");
        appendStream(firstStream);
        appendDcb(firstDcb);

        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();
        subscriptionModel.subscribe(
                        subscriptionId,
                        AgnosticSubscriptionFilter.filter(Filter.type(cloudEventConverter.getCloudEventType(TestEvent.class))),
                        StartAt.checkpoint(GlobalCheckpoint.of(0)),
                        ce -> received.add(cloudEventConverter.toDomainEvent(ce)))
                .waitUntilStarted();

        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(firstStream, firstDcb));

        subscriptionModel.pauseSubscription(subscriptionId);
        await().atMost(ofSeconds(10)).pollInterval(ofMillis(50)).until(() -> subscriptionModel.isPaused(subscriptionId));

        // When - more events of both capabilities are written while the subscription is paused, then it is resumed
        // with the same id, which continues from the stored GlobalCheckpoint rather than replaying from the beginning
        TestEvent secondStream = streamEvent("neutral-resume-stream-2");
        TestEvent secondDcb = dcbEvent("neutral-resume-dcb-2");
        appendStream(secondStream);
        appendDcb(secondDcb);

        subscriptionModel.resumeSubscription(subscriptionId);

        // Then - the events written while paused are delivered after resume, without a gap. Delivery is at-least-once,
        // so the assertion only requires the new events to show up, not that the first-generation ones stop appearing.
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(secondStream, secondDcb));
    }

    @Test
    void neutral_subscription_with_type_filter_receives_only_matching_type_across_both_capabilities() {
        // Given - a stream event and a DCB event of the shared TestEvent cloud event type (the only domain type this
        // test fixture's CloudEventConverter can decode), distinguished into "signal" and "noise" by subject the same
        // way the STREAM_TAG/DCB_TAG split already distinguishes capability elsewhere in this class. Filter.type is
        // exercised directly against the real, single cloud event type this fixture has; Filter.subject narrows further
        // to isolate signal from noise, mirroring how a real app narrows a type filter with an additional predicate.
        TestEvent streamNoise = streamEvent("type-filter-stream-noise");
        TestEvent dcbNoise = dcbEvent("type-filter-dcb-noise");
        TestEvent streamSignal = new TestEvent(UUID.randomUUID().toString(), new Date(), "type-filter-signal-stream", "type-filter-stream-signal");
        TestEvent dcbSignal = new TestEvent(UUID.randomUUID().toString(), new Date(), "type-filter-signal-dcb", "type-filter-dcb-signal");
        appendStream(streamNoise, streamSignal);
        appendDcb(dcbNoise, dcbSignal);

        CopyOnWriteArrayList<TestEvent> received = new CopyOnWriteArrayList<>();

        // When - a neutral subscription is filtered to the TestEvent cloud event type and further narrowed to the
        // "signal" subjects only
        String testEventCloudEventType = cloudEventConverter.getCloudEventType(TestEvent.class);
        Filter signalFilter = new Filter.CompositionFilter(Filter.CompositionOperator.AND, List.of(
                Filter.type(testEventCloudEventType),
                Filter.subject(Condition.in("type-filter-signal-stream", "type-filter-signal-dcb"))));
        subscriptionModel.subscribe(
                        "neutral-type-filter-" + UUID.randomUUID(),
                        AgnosticSubscriptionFilter.filter(signalFilter),
                        StartAt.checkpoint(GlobalCheckpoint.of(0)),
                        ce -> received.add(cloudEventConverter.toDomainEvent(ce)))
                .waitUntilStarted();

        // Then - both the stream-written and the DCB-appended signal events are delivered (proving the filter applies
        // across both capabilities), while the noise events are excluded
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() ->
                assertThat(received).contains(streamSignal, dcbSignal).doesNotContain(streamNoise, dcbNoise));
    }

    @Test
    void stream_and_dcb_annotated_style_subscriptions_stay_scoped_to_their_own_capability_alongside_a_neutral_one() {
        // Given - one event of each capability
        TestEvent streamOnly = streamEvent("regression-stream");
        TestEvent dcbOnly = dcbEvent("regression-dcb");
        appendStream(streamOnly);
        appendDcb(dcbOnly);

        CopyOnWriteArrayList<TestEvent> streamReceived = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<TestEvent> dcbReceived = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<TestEvent> neutralReceived = new CopyOnWriteArrayList<>();

        // When - a stream-scoped, a DCB-scoped and a neutral subscription all catch up from the beginning at once
        subscriptionModel.subscribe(
                        "regression-stream-" + UUID.randomUUID(),
                        StreamSubscriptionFilter.filter(Filter.all()),
                        StartAtTime.beginningOfTime(),
                        ce -> streamReceived.add(cloudEventConverter.toDomainEvent(ce)))
                .waitUntilStarted();

        dcbSubscriptions
                .subscribe(
                        "regression-dcb-" + UUID.randomUUID(),
                        DcbCriteria.all(),
                        DcbStartAt.beginning(),
                        dcbReceived::add)
                .waitUntilStarted();

        subscriptionModel.subscribe(
                        "regression-neutral-" + UUID.randomUUID(),
                        AgnosticSubscriptionFilter.filter(Filter.type(cloudEventConverter.getCloudEventType(TestEvent.class))),
                        StartAt.checkpoint(GlobalCheckpoint.of(0)),
                        ce -> neutralReceived.add(cloudEventConverter.toDomainEvent(ce)))
                .waitUntilStarted();

        // Then - the stream subscription sees only the stream event, the DCB subscription sees only the DCB event, and
        // only the neutral subscription sees both, confirming the #282 capability guard is intact alongside the new
        // neutral behavior
        await().atMost(ofSeconds(30)).pollInterval(ofMillis(100)).untilAsserted(() -> {
            assertThat(streamReceived).contains(streamOnly).doesNotContain(dcbOnly);
            assertThat(dcbReceived).contains(dcbOnly).doesNotContain(streamOnly);
            assertThat(neutralReceived).contains(streamOnly, dcbOnly);
        });
    }

    // --- helpers ---

    private TestEvent streamEvent(String name) {
        return new TestEvent(UUID.randomUUID().toString(), new Date(), STREAM_TAG, name);
    }

    private TestEvent dcbEvent(String name) {
        return new TestEvent(UUID.randomUUID().toString(), new Date(), DCB_TAG, name);
    }

    private void appendStream(TestEvent... events) {
        String streamId = UUID.randomUUID().toString();
        applicationService.execute(streamId, __ -> List.of(events));
    }

    private void appendDcb(TestEvent... events) {
        List<CloudEvent> cloudEvents = cloudEventConverter.toCloudEvents(List.of(events))
                .stream()
                .map(ce -> DcbCloudEvents.withTags(ce, List.of(Tag.parse(DCB_TAG))))
                .toList();
        dcbEventStore.append(cloudEvents);
    }

    // --- inner application and configuration classes ---

    @TestConfiguration(proxyBeanMethods = false)
    static class MongoDbContainerConfiguration {

        @Bean
        @ServiceConnection
        MongoDBContainer mongoDbContainer() {
            return ReplicaSetReadyMongoDBContainer.withDefaultVersion();
        }
    }

    @SpringBootApplication
    @EnableOccurrent
    static class CombinedModeApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return new JacksonCloudEventConverter.Builder<TestEvent>(new ObjectMapper(), SOURCE)
                    .typeMapper(typeMapper)
                    .idMapper(TestEvent::eventId)
                    .subjectMapper(TestEvent::kind)
                    .timeMapper(event -> event.timestamp().toInstant().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS))
                    .build();
        }

        @Bean
        TagGenerator<TestEvent> testEventTagGenerator() {
            return event -> Set.of(Tag.parse(event.kind()));
        }
    }

    record TestEvent(String eventId, Date timestamp, String kind, String name) {
    }
}
