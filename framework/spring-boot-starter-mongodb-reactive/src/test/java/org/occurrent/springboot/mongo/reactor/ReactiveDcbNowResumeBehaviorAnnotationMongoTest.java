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

import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.DcbSubscription;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.DcbProjection;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.EventWriter;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.RecordingSubscriber;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.TestEvent;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.occurrent.springboot.mongo.reactor.ApplicationRestart.FirstBootEnds.WHEN_THE_MARKER_IS_CHECKPOINTED;

/**
 * Proves that {@code startAt = NOW} combined with {@code resumeBehavior} on the reactive {@link DcbSubscription} and on
 * a {@code Projection} returning a {@link DcbProjection} behaves as documented across an actual application restart.
 * Under {@code DEFAULT} the first start is the present and a restart continues from the stored position, so an event
 * written while the application was down is delivered. Under {@code SAME_AS_START_AT} every start is the present, so
 * that event is missed.
 */
@DisplayName("Reactive DCB startAt NOW and resumeBehavior across an application restart")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveDcbNowResumeBehaviorAnnotationMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final ApplicationRestart APPLICATION = new ApplicationRestart(mongoDBContainer, RestartApplication.class, "dcb");

    private static final String TAG = "test:reactive-dcb-now-resume-behavior";
    private static final String VIEW_ID = "seen";

    @Test
    void dcb_subscription_with_now_and_default_resume_behavior_receives_events_written_while_down_after_restart() {
        APPLICATION.run("reactive-dcb-subscription-now-default", DcbSubscriptionNowDefault.class, WHEN_THE_MARKER_IS_CHECKPOINTED,
                List.of("live-1"),
                List.of("while-down-1", "live-2"));
    }

    @Test
    void dcb_subscription_with_now_and_same_as_start_at_starts_at_the_present_again_after_restart_and_misses_events_written_while_down() {
        APPLICATION.run("reactive-dcb-subscription-now-same", DcbSubscriptionNowSameAsStartAt.class, WHEN_THE_MARKER_IS_CHECKPOINTED,
                List.of("live-1"),
                List.of("live-2"));
    }

    @Test
    void dcb_projection_with_now_and_default_resume_behavior_receives_events_written_while_down_after_restart() {
        APPLICATION.run("reactive-dcb-projection-now-default", DcbProjectionNowDefault.class, WHEN_THE_MARKER_IS_CHECKPOINTED,
                List.of("live-1"),
                List.of("while-down-1", "live-2"));
    }

    @Test
    void dcb_projection_with_now_and_same_as_start_at_starts_at_the_present_again_after_restart_and_misses_events_written_while_down() {
        APPLICATION.run("reactive-dcb-projection-now-same", DcbProjectionNowSameAsStartAt.class, WHEN_THE_MARKER_IS_CHECKPOINTED,
                List.of("live-1"),
                List.of("live-2"));
    }

    @SpringBootApplication
    @EnableOccurrentReactive
    static class RestartApplication {

        @Bean
        CloudEventTypeMapper<TestEvent> testEventCloudEventTypeMapper() {
            return ReflectionCloudEventTypeMapper.qualified();
        }

        @Bean
        CloudEventConverter<TestEvent> testEventCloudEventConverter(CloudEventTypeMapper<TestEvent> typeMapper) {
            return ApplicationRestart.converter(typeMapper, "urn:occurrent:reactive-dcb-now-resume-behavior");
        }

        @Bean
        ViewStateRepository<Seen, String> seenViewStateRepository() {
            ConcurrentHashMap<String, Seen> views = new ConcurrentHashMap<>();
            return ViewStateRepository.create(views::get, views::put);
        }

        @Bean
        EventWriter eventWriter(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter,
                                @Value("${test.before-subscribe}") String beforeSubscribe,
                                @Value("${test.after-start}") String afterStart) {
            return new EventWriter(name -> append(dcbEventStore, converter, new TestEvent(name)), beforeSubscribe, afterStart);
        }

        @Bean
        DcbSubscriptionNowDefault dcbSubscriptionNowDefault() {
            return new DcbSubscriptionNowDefault();
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
    }

    private static void append(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter, TestEvent event) {
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(event))
                .stream()
                .map(cloudEvent -> DcbCloudEvents.withTags(cloudEvent, List.of(Tag.parse(TAG))))
                .toList();
        dcbEventStore.append(cloudEvents).block();
    }

    static class DcbSubscriptionNowDefault extends RecordingSubscriber {
        @DcbSubscription(id = "reactive-dcb-subscription-now-default", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    static class DcbSubscriptionNowSameAsStartAt extends RecordingSubscriber {
        @DcbSubscription(id = "reactive-dcb-subscription-now-same", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    static class DcbProjectionNowDefault extends RecordingSubscriber {
        @org.occurrent.annotation.Projection(id = "reactive-dcb-projection-now-default", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        DcbProjection<Seen, TestEvent, String> seen() {
            return seenProjection(this);
        }
    }

    static class DcbProjectionNowSameAsStartAt extends RecordingSubscriber {
        @org.occurrent.annotation.Projection(id = "reactive-dcb-projection-now-same", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        DcbProjection<Seen, TestEvent, String> seen() {
            return seenProjection(this);
        }
    }

    // The fold hands the event and its metadata to the recorder, which is how the test sees what the projection folded
    // and the checkpoint it was delivered with.
    private static DcbProjection<Seen, TestEvent, String> seenProjection(RecordingSubscriber recorder) {
        Projection<Seen, TestEvent, String> projection = Projection.<Seen, TestEvent, String>builder(new Seen(0))
                .id(event -> VIEW_ID)
                .on(TestEvent.class, (state, metadata, event) -> {
                    recorder.record(event, metadata);
                    return new Seen(state.count() + 1);
                })
                .build();
        return new DcbProjection<>(projection, DcbCriteria.tags(Tag.parse(TAG)));
    }

    record Seen(int count) {
    }
}
