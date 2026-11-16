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

package org.occurrent.subscription.synchronous.blocking;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.service.WriteRefusedException;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.application.service.blocking.dcb.GenericDcbApplicationService;
import org.occurrent.eventstore.api.dcb.DcbCriteria;
import org.occurrent.eventstore.api.dcb.Tag;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.filter.Filter;
import org.occurrent.subscription.StreamSubscriptionFilter;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.eventstore.api.dcb.DcbCriteria.tags;

/**
 * A synchronous handler has no replay, so a write it does not see is lost to it. The application service therefore
 * asks the synchronous model before it writes, and the model refuses the write while it is stopped or when a paused
 * subscription matches the events. These tests drive the real application service over a real model, since the
 * refusal is the two working together. The model is never given to the test extension that turns the refusals off.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class SynchronousSubscriptionModelWriteRefusalTest {

    private static final Tag TAG = Tag.parse("name:1");

    private InMemoryEventStore eventStore;
    private SynchronousSubscriptionModel model;
    private List<String> handled;
    private GenericApplicationService<TestEvent> applicationService;

    @BeforeEach
    void initialize() {
        eventStore = new InMemoryEventStore();
        model = new SynchronousSubscriptionModel();
        handled = new ArrayList<>();
        applicationService = GenericApplicationService.builder(eventStore, new TestEventConverter())
                .synchronousSubscriptions(model)
                .build();
    }

    @Test
    void a_write_after_the_model_is_stopped_is_refused_when_a_handler_is_registered() {
        model.subscribe("handler", event -> handled.add(event.getType()));
        model.stop();
        String streamId = UUID.randomUUID().toString();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isInstanceOf(WriteRefusedException.class).hasMessageContaining("stopped");
        assertThat(eventStore.exists(streamId)).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_after_the_model_is_stopped_goes_through_when_no_handler_is_registered() {
        model.stop();
        String streamId = UUID.randomUUID().toString();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isNull();
        assertThat(eventStore.exists(streamId)).as("the write was stored").isTrue();
    }

    @Test
    void a_write_after_the_model_is_started_again_goes_through_and_reaches_the_handler() {
        model.subscribe("handler", event -> handled.add(event.getType()));
        model.stop();
        model.start();
        String streamId = UUID.randomUUID().toString();

        applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined")));

        assertThat(eventStore.exists(streamId)).isTrue();
        assertThat(handled).containsExactly("NameDefined");
    }

    @Test
    void a_write_that_a_paused_subscription_matches_is_refused_and_the_message_names_the_subscription() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection");
        assertThat(eventStore.exists(streamId)).as("the refused write was stored").isFalse();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_write_that_a_paused_subscription_does_not_match_goes_through() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add("orders-projection"));
        model.subscribe("other-projection", StreamSubscriptionFilter.filter(Filter.type("NameWasChanged")), event -> handled.add("other-projection"));
        model.pauseSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        Throwable thrown = catchThrowable(() -> applicationService.execute(streamId, events -> List.of(new TestEvent("NameWasChanged"))));

        assertThat(thrown).isNull();
        assertThat(eventStore.exists(streamId)).as("the write was stored").isTrue();
        assertThat(handled).containsExactly("other-projection");
    }

    @Test
    void a_write_goes_through_once_the_paused_subscription_is_resumed() {
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        model.pauseSubscription("orders-projection");
        model.resumeSubscription("orders-projection");
        String streamId = UUID.randomUUID().toString();

        applicationService.execute(streamId, events -> List.of(new TestEvent("NameDefined")));

        assertThat(eventStore.exists(streamId)).isTrue();
        assertThat(handled).containsExactly("NameDefined");
    }

    @Test
    void a_dcb_write_that_a_paused_subscription_matches_is_refused_and_the_message_names_the_subscription() {
        var dcbApplicationService = dcbApplicationService();
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add(event.getType()));
        model.pauseSubscription("orders-projection");

        Throwable thrown = catchThrowable(() -> dcbApplicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isInstanceOf(WriteRefusedException.class).hasMessageContaining("orders-projection");
        assertThat(eventStore.read(DcbCriteria.all()).events()).as("events stored by the refused write").isEmpty();
        assertThat(handled).isEmpty();
    }

    @Test
    void a_dcb_write_that_a_paused_subscription_does_not_match_goes_through() {
        var dcbApplicationService = dcbApplicationService();
        model.subscribe("orders-projection", StreamSubscriptionFilter.filter(Filter.type("NameDefined")), event -> handled.add("orders-projection"));
        model.pauseSubscription("orders-projection");

        Throwable thrown = catchThrowable(() -> dcbApplicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameWasChanged"))));

        assertThat(thrown).isNull();
        assertThat(eventStore.read(DcbCriteria.all()).events()).as("events stored by the write").hasSize(1);
    }

    @Test
    void a_dcb_write_after_the_model_is_stopped_is_refused_when_a_handler_is_registered() {
        var dcbApplicationService = dcbApplicationService();
        model.subscribe("handler", event -> handled.add(event.getType()));
        model.stop();

        Throwable thrown = catchThrowable(() -> dcbApplicationService.execute(tags(TAG), events -> List.of(new TestEvent("NameDefined"))));

        assertThat(thrown).isInstanceOf(WriteRefusedException.class).hasMessageContaining("stopped");
        assertThat(eventStore.read(DcbCriteria.all()).events()).as("events stored by the refused write").isEmpty();
    }

    private GenericDcbApplicationService<TestEvent> dcbApplicationService() {
        return GenericDcbApplicationService.builder(eventStore, new TestEventConverter())
                .tagGenerator(event -> Set.of(TAG))
                .synchronousSubscriptions(model)
                .build();
    }

    private record TestEvent(String type) {
    }

    private static final class TestEventConverter implements CloudEventConverter<TestEvent> {
        @Override
        public CloudEvent toCloudEvent(TestEvent domainEvent) {
            return CloudEventBuilder.v1()
                    .withId(UUID.randomUUID().toString())
                    .withSource(URI.create("urn:occurrent:test"))
                    .withType(domainEvent.type())
                    .build();
        }

        @Override
        public TestEvent toDomainEvent(CloudEvent cloudEvent) {
            return new TestEvent(cloudEvent.getType());
        }

        @Override
        public String getCloudEventType(Class<? extends TestEvent> type) {
            throw new UnsupportedOperationException();
        }
    }
}
