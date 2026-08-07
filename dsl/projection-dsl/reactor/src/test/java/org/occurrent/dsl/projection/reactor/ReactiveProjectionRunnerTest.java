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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.subscription.synchronous.reactor.SynchronousSubscriptionModel;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Docker-free reactor test proving the metadata-carrying {@link ReactiveProjectionRunner#project(String, Projection,
 * java.util.function.BiFunction) BiFunction overload} threads a delivered event's real {@link
 * org.occurrent.cloudevents.EventMetadata} into the caller-supplied update, mirroring how
 * {@link ReactiveDcbProjectionRunnerTest} proves the same for its DCB sibling. Uses the in-process
 * {@link SynchronousSubscriptionModel} rather than a real store, feeding it a {@link CloudEvent} carrying the
 * {@code streamid}/{@code streamversion} extensions by hand, since that is all {@link
 * org.occurrent.cloudevents.EventMetadata#from(CloudEvent)} reads from.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class ReactiveProjectionRunnerTest {

    private final CloudEventConverter<DomainEvent> converter =
            new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:test")).idMapper(DomainEvent::eventId).build();

    @Test
    void the_bifunction_overload_exposes_the_delivered_events_real_stream_metadata_to_the_update_function() {
        SynchronousSubscriptionModel sync = new SynchronousSubscriptionModel();
        ConcurrentHashMap<String, Long> repo = new ConcurrentHashMap<>();
        Projection<Long, DomainEvent, String> projection = Projection.<Long, DomainEvent, String>builder(0L).id(event -> "alice").build();
        ReactiveProjectionRunner<DomainEvent> runner = ReactiveProjectionRunner.agnostic(sync, converter);

        // getStreamId()/getStreamVersion() throw on EventMetadata.empty(), so this only passes if real metadata
        // reaches the BiFunction instead of the event-only Function overload's path.
        runner.project("alice-projection", projection, (metadata, event) -> {
            repo.put(metadata.getStreamId(), metadata.getStreamVersion());
            return Mono.empty();
        });

        NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), LocalDateTime.now(), "alice", "Alice");
        CloudEvent cloudEvent = withStreamMetadata(converter.toCloudEvent(nameDefined), "alice-stream", 1);
        sync.dispatch(List.of(cloudEvent)).block();

        assertThat(repo).containsExactly(Map.entry("alice-stream", 1L));
    }

    private static CloudEvent withStreamMetadata(CloudEvent cloudEvent, String streamId, long streamVersion) {
        return CloudEventBuilder.v1(cloudEvent).withExtension(OccurrentCloudEventExtension.occurrent(streamId, streamVersion)).build();
    }
}
