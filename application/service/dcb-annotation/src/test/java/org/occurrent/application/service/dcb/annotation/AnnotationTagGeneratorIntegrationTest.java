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

package org.occurrent.application.service.dcb.annotation;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.DcbTag;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.eventstore.api.dcb.DcbAppendResult;
import org.occurrent.eventstore.api.dcb.DcbCloudEvents;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.DcbEventStream;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end round-trip through the real DCB append/query pipeline: an {@link AnnotationTagGenerator}
 * derives {@link Tag}s from {@code @DcbTag}-annotated event components, {@link GenericDcbApplicationService}
 * appends the resulting event to a real {@link InMemoryEventStore} via a real {@link JacksonCloudEventConverter},
 * and the event is read back by querying the store for the derived tags. No mocks are involved anywhere
 * in the chain.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class AnnotationTagGeneratorIntegrationTest {

    private record SubscribedToCourse(String eventId, long occurredAt, @DcbTag(key = "email") String email,
                                       @DcbTag(key = "courseId") String courseId) {
    }

    private final CloudEventConverter<SubscribedToCourse> converter =
            new JacksonCloudEventConverter<>(new ObjectMapper(), URI.create("urn:test"));

    @Test
    void event_appended_through_the_dcb_application_service_can_be_read_back_by_its_annotation_derived_tags() {
        InMemoryEventStore eventStore = new InMemoryEventStore();
        AnnotationTagGenerator<SubscribedToCourse> tagGenerator = new AnnotationTagGenerator<>();
        GenericDcbApplicationService<SubscribedToCourse> applicationService =
                new GenericDcbApplicationService<>(eventStore, converter, tagGenerator);

        SubscribedToCourse event = new SubscribedToCourse(UUID.randomUUID().toString(), System.currentTimeMillis(), "alice@example.com", "course-42");

        Optional<DcbAppendResult> result = applicationService.execute(DcbCriteria.all(), events -> Stream.of(event));

        assertThat(result).isPresent();

        DcbEventStream byEmail = eventStore.read(DcbCriteria.tags(Tag.of("email", "alice@example.com")));
        assertThat(byEmail.events()).extracting(converter::toDomainEvent).containsExactly(event);

        CloudEvent storedCloudEvent = byEmail.events().get(0);
        assertThat(DcbCloudEvents.getTags(storedCloudEvent)).containsExactlyInAnyOrder(
                Tag.of("email", "alice@example.com"),
                Tag.of("courseId", "course-42")
        );

        DcbEventStream byNonMatchingTag = eventStore.read(DcbCriteria.tags(Tag.of("email", "nobody@example.com")));
        assertThat(byNonMatchingTag.events()).isEmpty();
    }
}
