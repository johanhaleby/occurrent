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

package org.occurrent.dsl.projection.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.eventstore.api.PositionRange;
import org.occurrent.eventstore.api.reactor.PositionOrderedReader;
import org.occurrent.filter.Filter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Shared by the tests of what a catch-up marker means (ADR 179).
 */
final class ResumePositionFixtures {

    private ResumePositionFixtures() {
    }

    record Counted(String eventId) {
    }

    static CloudEventConverter<Counted> converter() {
        return new CloudEventConverter<>() {
            @Override
            public CloudEvent toCloudEvent(Counted domainEvent) {
                return cloudEvent(domainEvent.eventId());
            }

            @Override
            public Counted toDomainEvent(CloudEvent cloudEvent) {
                return new Counted(cloudEvent.getId());
            }

            @Override
            public String getCloudEventType(Class<? extends Counted> type) {
                return "Counted";
            }
        };
    }

    static CloudEvent cloudEvent(String id) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("urn:occurrent:test")).withType("Counted").build();
    }

    // An event's position is its one-based place in the append order, and a range excludes its lower bound, which is
    // what the real stores do.
    static final class InMemoryLog implements PositionOrderedReader {
        private final List<CloudEvent> events = new CopyOnWriteArrayList<>();

        void append(String... ids) {
            events.addAll(Arrays.stream(ids).map(ResumePositionFixtures::cloudEvent).toList());
        }

        long head() {
            return events.size();
        }

        @Override
        public Flux<CloudEvent> readInPositionOrder(Filter filter, PositionRange range) {
            return Flux.defer(() -> {
                List<CloudEvent> snapshot = List.copyOf(events);
                long after = range.afterPosition().orElse(0L);
                long upTo = Math.min(range.upToPosition().orElse((long) snapshot.size()), snapshot.size());
                return Flux.fromIterable(snapshot.subList((int) Math.min(after, upTo), (int) upTo));
            });
        }

        @Override
        public Mono<Long> currentPosition() {
            return Mono.fromSupplier(this::head);
        }

        @Override
        public boolean writesPosition() {
            return true;
        }
    }

    // Records each call the way a view sees it, so a test can say which callbacks reached it.
    static final class RecordingReplayAwareView implements BiFunction<EventMetadata, Counted, Mono<Void>>, ReactiveReplayAware {
        final List<String> calls = new CopyOnWriteArrayList<>();
        private final Consumer<Counted> onUpdate;

        RecordingReplayAwareView() {
            this(event -> {
            });
        }

        RecordingReplayAwareView(Consumer<Counted> onUpdate) {
            this.onUpdate = onUpdate;
        }

        @Override
        public Mono<Void> apply(EventMetadata metadata, Counted event) {
            return Mono.fromRunnable(() -> {
                calls.add("update:" + event.eventId());
                onUpdate.accept(event);
            });
        }

        @Override
        public void replayStarted() {
            calls.add("replayStarted");
        }

        @Override
        public Mono<Void> replayCompleted() {
            return Mono.fromRunnable(() -> calls.add("replayCompleted"));
        }

        @Override
        public void replayAbandoned() {
            calls.add("replayAbandoned");
        }

        @Override
        public Mono<Void> alreadyDeliveredByReplay(EventMetadata metadata) {
            return Mono.fromRunnable(() -> calls.add("alreadyDeliveredByReplay"));
        }

        List<String> updates() {
            return calls.stream().filter(c -> c.startsWith("update:")).map(c -> c.substring("update:".length())).toList();
        }
    }
}
