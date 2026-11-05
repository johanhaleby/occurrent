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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.StartPosition;
import org.occurrent.annotation.Subscription;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.application.service.reactor.ApplicationService;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.view.ViewStateRepository;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.occurrent.springboot.mongo.reactor.ApplicationRestart.FirstBootEnds.WHEN_THE_MARKER_IS_CHECKPOINTED;

/**
 * Proves that {@code startAt = NOW} combined with {@code resumeBehavior} on the reactive capability-agnostic
 * {@link Subscription} and {@code Projection} behaves as documented across an actual application restart. Under
 * {@code DEFAULT} the first start is the present and a restart continues from the stored position, so an event written
 * while the application was down is delivered. Under {@code SAME_AS_START_AT} every start is the present, so that
 * event is missed.
 */
@DisplayName("Reactive agnostic startAt NOW and resumeBehavior across an application restart")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveAgnosticNowResumeBehaviorAnnotationMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final ApplicationRestart APPLICATION = new ApplicationRestart(mongoDBContainer, RestartApplication.class, "stream");

    private static final String VIEW_ID = "seen";

    @Test
    void subscription_with_now_and_default_resume_behavior_receives_events_written_while_down_after_restart() {
        APPLICATION.run("reactive-agnostic-subscription-now-default", SubscriptionNowDefault.class, WHEN_THE_MARKER_IS_CHECKPOINTED,
                List.of("live-1"),
                List.of("while-down-1", "live-2"));
    }

    @Test
    void subscription_with_now_and_same_as_start_at_starts_at_the_present_again_after_restart_and_misses_events_written_while_down() {
        APPLICATION.run("reactive-agnostic-subscription-now-same", SubscriptionNowSameAsStartAt.class, WHEN_THE_MARKER_IS_CHECKPOINTED,
                List.of("live-1"),
                List.of("live-2"));
    }

    @Test
    void projection_with_now_and_default_resume_behavior_receives_events_written_while_down_after_restart() {
        APPLICATION.run("reactive-agnostic-projection-now-default", ProjectionNowDefault.class, WHEN_THE_MARKER_IS_CHECKPOINTED,
                List.of("live-1"),
                List.of("while-down-1", "live-2"));
    }

    @Test
    void projection_with_now_and_same_as_start_at_starts_at_the_present_again_after_restart_and_misses_events_written_while_down() {
        APPLICATION.run("reactive-agnostic-projection-now-same", ProjectionNowSameAsStartAt.class, WHEN_THE_MARKER_IS_CHECKPOINTED,
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
            return ApplicationRestart.converter(typeMapper, "urn:occurrent:reactive-agnostic-now-resume-behavior");
        }

        @Bean
        ViewStateRepository<Seen, String> seenViewStateRepository() {
            ConcurrentHashMap<String, Seen> views = new ConcurrentHashMap<>();
            return ViewStateRepository.create(views::get, views::put);
        }

        @Bean
        EventWriter eventWriter(ApplicationService<TestEvent> applicationService,
                                @Value("${test.before-subscribe}") String beforeSubscribe,
                                @Value("${test.after-start}") String afterStart) {
            return new EventWriter(name -> applicationService.execute(UUID.randomUUID().toString(), __ -> List.of(new TestEvent(name))).block(), beforeSubscribe, afterStart);
        }

        @Bean
        SubscriptionNowDefault subscriptionNowDefault() {
            return new SubscriptionNowDefault();
        }

        @Bean
        SubscriptionNowSameAsStartAt subscriptionNowSameAsStartAt() {
            return new SubscriptionNowSameAsStartAt();
        }

        @Bean
        ProjectionNowDefault projectionNowDefault() {
            return new ProjectionNowDefault();
        }

        @Bean
        ProjectionNowSameAsStartAt projectionNowSameAsStartAt() {
            return new ProjectionNowSameAsStartAt();
        }
    }

    static class SubscriptionNowDefault extends RecordingSubscriber {
        @Subscription(id = "reactive-agnostic-subscription-now-default", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    static class SubscriptionNowSameAsStartAt extends RecordingSubscriber {
        @Subscription(id = "reactive-agnostic-subscription-now-same", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        Mono<Void> on(TestEvent event, EventMetadata metadata) {
            return record(event, metadata);
        }
    }

    static class ProjectionNowDefault extends RecordingSubscriber {
        @org.occurrent.annotation.Projection(id = "reactive-agnostic-projection-now-default", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        Projection<Seen, TestEvent, String> seen() {
            return seenProjection(this);
        }
    }

    static class ProjectionNowSameAsStartAt extends RecordingSubscriber {
        @org.occurrent.annotation.Projection(id = "reactive-agnostic-projection-now-same", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        Projection<Seen, TestEvent, String> seen() {
            return seenProjection(this);
        }
    }

    // The fold hands the event and its metadata to the recorder, which is how the test sees what the projection folded
    // and the checkpoint it was delivered with.
    private static Projection<Seen, TestEvent, String> seenProjection(RecordingSubscriber recorder) {
        return Projection.<Seen, TestEvent, String>builder(new Seen(0))
                .id(event -> VIEW_ID)
                .on(TestEvent.class, (state, metadata, event) -> {
                    recorder.record(event, metadata);
                    return new Seen(state.count() + 1);
                })
                .build();
    }

    record Seen(int count) {
    }
}
