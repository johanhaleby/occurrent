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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.annotation.ResumeBehavior;
import org.occurrent.annotation.Snapshot;
import org.occurrent.annotation.StartPosition;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.typemapper.CloudEventTypeMapper;
import org.occurrent.application.converter.typemapper.ReflectionCloudEventTypeMapper;
import org.occurrent.dsl.snapshot.DcbSnapshotKeys;
import org.occurrent.dsl.snapshot.DcbSnapshotView;
import org.occurrent.dsl.snapshot.SnapshotView;
import org.occurrent.dsl.snapshot.mongodb.spring.reactor.ReactiveSpringMongoSnapshotStore;
import org.occurrent.dsl.snapshot.reactor.ReactiveSnapshotStore;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.api.dcb.reactor.DcbEventStore;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.EventWriter;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.Recorder;
import org.occurrent.springboot.mongo.reactor.ApplicationRestart.TestEvent;
import org.occurrent.testsupport.mongodb.ReplicaSetReadyMongoDBContainer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.occurrent.springboot.mongo.reactor.ApplicationRestart.FirstBootEnds.WHEN_THE_MARKER_IS_DELIVERED;

/**
 * Checks which events a reactive {@link Snapshot} that returns a {@link DcbSnapshotView} is delivered across an
 * application restart, for {@code NOW} with each {@code resumeBehavior} and for {@code BEGINNING} with
 * {@code SAME_AS_START_AT}.
 * <p>
 * A DCB snapshot folds every matching event it reads from the event store, so its saved state is the same whichever
 * events its subscription is delivered. The tests therefore check two things per boot. One is how many times the
 * registrar read the stored snapshot, which it does once per delivered event. The other is the versions it saved, which
 * are the positions of the delivered events that were not already folded. A redelivery is read but not saved.
 * <p>
 * {@code BEGINNING} with {@code DEFAULT} is left out because after a restart it is delivered the first boot's last live
 * event again.
 */
@DisplayName("Reactive DCB Snapshot startAt and resumeBehavior across an application restart")
@DisplayNameGeneration(ReplaceUnderscores.class)
@Testcontainers
@Timeout(180)
class ReactiveDcbSnapshotResumeBehaviorAnnotationMongoTest {

    @Container
    static final MongoDBContainer mongoDBContainer = ReplicaSetReadyMongoDBContainer.withDefaultVersion();

    private static final ApplicationRestart APPLICATION = new ApplicationRestart(mongoDBContainer, RestartApplication.class, "dcb");

    private static final Tag TAG = Tag.parse("test:reactive-dcb-snapshot-resume-behavior");
    private static final List<String> EVERYTHING_WRITTEN = List.of("historic-1", "historic-2", "live-1", "while-down-1", "live-2");

    // Positions run historic-1=1, historic-2=2, live-1=3, while-down-1=4, live-2=5. Reads count every delivery and saved
    // versions show which of them were new, so a redelivered live-1 cannot pass for while-down-1.
    @Test
    void a_dcb_snapshot_with_now_and_default_resume_behavior_is_not_delivered_history_on_first_boot_and_is_delivered_events_written_while_down_after_restart() {
        restart("reactive-dcb-snapshot-now-default", DcbSnapshotNowDefault.class, 1, List.of(3L), 2, List.of(4L, 5L));
    }

    @Test
    void a_dcb_snapshot_with_now_and_same_as_start_at_is_delivered_only_the_present_again_after_restart_but_still_folds_events_written_while_down() {
        restart("reactive-dcb-snapshot-now-same", DcbSnapshotNowSameAsStartAt.class, 1, List.of(3L), 1, List.of(5L));
    }

    @Test
    void a_dcb_snapshot_with_beginning_and_same_as_start_at_is_delivered_the_whole_history_again_after_restart() {
        restart("reactive-dcb-snapshot-beginning-same", DcbSnapshotBeginningSameAsStartAt.class, 3, List.of(1L, 2L, 3L), 5, List.of(4L, 5L));
    }

    private static void restart(String snapshotId, Class<? extends SnapshotRecorder> recorderType,
                                int expectedReadsFirstBoot, List<Long> expectedSavedFirstBoot,
                                int expectedReadsSecondBoot, List<Long> expectedSavedSecondBoot) {
        APPLICATION.run(snapshotId, recorderType, WHEN_THE_MARKER_IS_DELIVERED,
                List.of("historic-1", "historic-2", "live-1"),
                EVERYTHING_WRITTEN,
                (context, marker) -> {
                    boolean firstBoot = marker.equals("live-1");
                    CountingSnapshotStore store = context.getBean(recorderType).store;
                    // Held for a second so a delivery that arrives after the marker would fail the assertion.
                    await().during(ofSeconds(1)).atMost(ofSeconds(5)).untilAsserted(() -> {
                        assertThat(store.reads()).isEqualTo(firstBoot ? expectedReadsFirstBoot : expectedReadsSecondBoot);
                        assertThat(store.savedVersions()).containsExactlyElementsOf(firstBoot ? expectedSavedFirstBoot : expectedSavedSecondBoot);
                    });
                });
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
            return ApplicationRestart.converter(typeMapper, "urn:occurrent:reactive-dcb-snapshot-resume-behavior");
        }

        @Bean
        EventWriter eventWriter(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter,
                                @Value("${test.before-subscribe}") String beforeSubscribe,
                                @Value("${test.after-start}") String afterStart) {
            return new EventWriter(name -> append(dcbEventStore, converter, new TestEvent(name)), beforeSubscribe, afterStart);
        }

        @Bean
        CountingSnapshotStore dcbSnapshotNowDefaultStore(ReactiveMongoOperations mongoOperations) {
            return new CountingSnapshotStore(mongoOperations, "reactive-dcb-snapshot-now-default");
        }

        @Bean
        DcbSnapshotNowDefault dcbSnapshotNowDefault(@Qualifier("dcbSnapshotNowDefaultStore") CountingSnapshotStore store) {
            return new DcbSnapshotNowDefault(store);
        }

        @Bean
        CountingSnapshotStore dcbSnapshotNowSameAsStartAtStore(ReactiveMongoOperations mongoOperations) {
            return new CountingSnapshotStore(mongoOperations, "reactive-dcb-snapshot-now-same");
        }

        @Bean
        DcbSnapshotNowSameAsStartAt dcbSnapshotNowSameAsStartAt(@Qualifier("dcbSnapshotNowSameAsStartAtStore") CountingSnapshotStore store) {
            return new DcbSnapshotNowSameAsStartAt(store);
        }

        @Bean
        CountingSnapshotStore dcbSnapshotBeginningSameAsStartAtStore(ReactiveMongoOperations mongoOperations) {
            return new CountingSnapshotStore(mongoOperations, "reactive-dcb-snapshot-beginning-same");
        }

        @Bean
        DcbSnapshotBeginningSameAsStartAt dcbSnapshotBeginningSameAsStartAt(@Qualifier("dcbSnapshotBeginningSameAsStartAtStore") CountingSnapshotStore store) {
            return new DcbSnapshotBeginningSameAsStartAt(store);
        }
    }

    private static void append(DcbEventStore dcbEventStore, CloudEventConverter<TestEvent> converter, TestEvent event) {
        List<CloudEvent> cloudEvents = converter.toCloudEvents(List.of(event))
                .stream()
                .map(cloudEvent -> DcbCloudEvents.withTags(cloudEvent, List.of(TAG)))
                .toList();
        dcbEventStore.append(cloudEvents).block();
    }

    static class DcbSnapshotNowDefault extends SnapshotRecorder {
        DcbSnapshotNowDefault(CountingSnapshotStore store) {
            super(store);
        }

        @Snapshot(id = "reactive-dcb-snapshot-now-default", storeName = "dcbSnapshotNowDefaultStore", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.DEFAULT)
        DcbSnapshotView<Folded, TestEvent> snapshot() {
            return foldingSnapshot();
        }
    }

    static class DcbSnapshotNowSameAsStartAt extends SnapshotRecorder {
        DcbSnapshotNowSameAsStartAt(CountingSnapshotStore store) {
            super(store);
        }

        @Snapshot(id = "reactive-dcb-snapshot-now-same", storeName = "dcbSnapshotNowSameAsStartAtStore", startAt = StartPosition.NOW, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        DcbSnapshotView<Folded, TestEvent> snapshot() {
            return foldingSnapshot();
        }
    }

    static class DcbSnapshotBeginningSameAsStartAt extends SnapshotRecorder {
        DcbSnapshotBeginningSameAsStartAt(CountingSnapshotStore store) {
            super(store);
        }

        @Snapshot(id = "reactive-dcb-snapshot-beginning-same", storeName = "dcbSnapshotBeginningSameAsStartAtStore", startAt = StartPosition.BEGINNING, resumeBehavior = ResumeBehavior.SAME_AS_START_AT)
        DcbSnapshotView<Folded, TestEvent> snapshot() {
            return foldingSnapshot();
        }
    }

    // The snapshot is stored in MongoDB, so the names it folded on the first boot are still there on the second.
    abstract static class SnapshotRecorder implements Recorder {
        final CountingSnapshotStore store;

        SnapshotRecorder(CountingSnapshotStore store) {
            this.store = store;
        }

        @Override
        public List<String> names() {
            return store.folded();
        }

        @Override
        public @Nullable String checkpointOf(String name) {
            return null;
        }
    }

    static class CountingSnapshotStore implements ReactiveSnapshotStore<Folded> {
        private static final String KEY = DcbSnapshotKeys.canonicalKey(DcbCriteria.tags(TAG));

        private final ReactiveSnapshotStore<Folded> delegate;
        private final AtomicInteger reads = new AtomicInteger();
        private final CopyOnWriteArrayList<Long> savedVersions = new CopyOnWriteArrayList<>();

        CountingSnapshotStore(ReactiveMongoOperations mongoOperations, String snapshotId) {
            this.delegate = new ReactiveSpringMongoSnapshotStore<>(mongoOperations, Folded.class, "occurrent-snapshot-" + snapshotId);
        }

        @Override
        public Mono<org.occurrent.dsl.snapshot.Snapshot<Folded>> findLatest(String key) {
            reads.incrementAndGet();
            return delegate.findLatest(key);
        }

        @Override
        public Mono<Void> save(String key, org.occurrent.dsl.snapshot.Snapshot<Folded> snapshot) {
            return delegate.save(key, snapshot).doOnSuccess(ignored -> savedVersions.add(snapshot.version()));
        }

        int reads() {
            return reads.get();
        }

        List<Long> savedVersions() {
            return List.copyOf(savedVersions);
        }

        // Read from the delegate so that looking at the state is not counted as a read.
        List<String> folded() {
            return delegate.findLatest(KEY).blockOptional().map(snapshot -> snapshot.state().names()).orElse(List.of());
        }
    }

    private static DcbSnapshotView<Folded, TestEvent> foldingSnapshot() {
        return new DcbSnapshotView<>(
                SnapshotView.<Folded, TestEvent>builder(new Folded(List.of()))
                        .on(TestEvent.class, (state, event) -> new Folded(Stream.concat(state.names().stream(), Stream.of(event.name())).toList()))
                        .build(),
                DcbCriteria.tags(TAG));
    }

    record Folded(List<String> names) {
    }
}
