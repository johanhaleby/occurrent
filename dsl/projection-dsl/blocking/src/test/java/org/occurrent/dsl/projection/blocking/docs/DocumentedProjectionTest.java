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

package org.occurrent.dsl.projection.blocking.docs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.application.converter.CloudEventConverter;
import org.occurrent.application.converter.jackson.JacksonCloudEventConverter;
import org.occurrent.application.service.blocking.ApplicationService;
import org.occurrent.application.service.blocking.TransactionExecutor;
import org.occurrent.application.service.blocking.generic.GenericApplicationService;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.dsl.projection.Projection;
import org.occurrent.dsl.projection.blocking.ProjectionExtensionsKt;
import org.occurrent.dsl.projection.blocking.ProjectionRunner;
import org.occurrent.dsl.query.blocking.DomainEventQueries;
import org.occurrent.dsl.view.View;
import org.occurrent.dsl.view.ViewStateRepository;
import org.occurrent.eventstore.inmemory.InMemoryEventStore;
import org.occurrent.subscription.inmemory.InMemorySubscriptionModel;
import org.occurrent.subscription.synchronous.blocking.SynchronousSubscriptionModel;

import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The projections the documentation's Testing chapter shows, kept compiling and passing here so a published snippet
 * cannot drift from the API. Covers the pure fold with no store, the asynchronous subscription-fed store, the
 * read-your-writes synchronous model, and the agreement between the push-fed store and the on-demand pull query.
 */
@DisplayName("DocumentedProjection")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DocumentedProjectionTest {

    @Nested
    @DisplayName("when folding purely, with no store or subscription")
    class When_folding_purely_with_no_store_or_subscription {

        @Test
        void folds_the_initial_state_through_the_registered_handlers() {
            // Given
            View<String, DomainEvent> view = currentNameProjection().view();

            // When
            String afterDefine = view.evolve(view.initialState(), new NameDefined(UUID.randomUUID().toString(), new Date(), "johan", "Johan"));
            String afterChange = view.evolve(afterDefine, new NameWasChanged(UUID.randomUUID().toString(), new Date(), "johan", "Johan Haleby"));

            // Then
            assertThat(afterChange).isEqualTo("Johan Haleby");
        }

        @Test
        void an_event_type_with_no_registered_handler_leaves_the_state_unchanged() {
            // Given
            Projection<String, DomainEvent, String> onlyDefinedProjection = Projection.<String, DomainEvent, String>builder("unset")
                    .id(DomainEvent::userId)
                    .on(NameDefined.class, (state, event) -> event.name())
                    .build();
            View<String, DomainEvent> view = onlyDefinedProjection.view();

            // When
            String state = view.evolve(view.initialState(), new NameWasChanged(UUID.randomUUID().toString(), new Date(), "johan", "Johan Haleby"));

            // Then
            assertThat(state).isEqualTo("unset");
        }
    }

    @Nested
    @DisplayName("when projected into a store through a subscription")
    class When_projected_into_a_store_through_a_subscription {

        private InMemorySubscriptionModel subscriptionModel;
        private InMemoryEventStore eventStore;
        private CloudEventConverter<DomainEvent> converter;

        @BeforeEach
        void setup() {
            subscriptionModel = new InMemorySubscriptionModel();
            eventStore = new InMemoryEventStore(subscriptionModel);
            converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:occurrent:projection-docs"))
                    .idMapper(DomainEvent::eventId)
                    .build();
        }

        @AfterEach
        void shutdown() {
            subscriptionModel.shutdown();
        }

        @Test
        void the_store_eventually_holds_the_folded_state() {
            // Given
            ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
            ViewStateRepository<String, String> repository = ViewStateRepository.create(store::get, store::put);
            ProjectionRunner.agnostic(subscriptionModel, converter).project("current-name", currentNameProjection(), repository);

            // When
            write("johan",
                    new NameDefined(UUID.randomUUID().toString(), new Date(), "johan", "Johan"),
                    new NameWasChanged(UUID.randomUUID().toString(), new Date(), "johan", "Johan Haleby"));

            // Then a deterministic drain, so the assertion is plain rather than polled
            assertThat(subscriptionModel.waitUntilAllEventsProcessed()).isTrue();
            assertThat(store.get("johan")).isEqualTo("Johan Haleby");
        }

        private void write(String streamId, DomainEvent... events) {
            eventStore.write(streamId, asList(events).stream().map(converter::toCloudEvent).toList());
        }
    }

    @Nested
    @DisplayName("when read after write on the synchronous subscription model")
    class When_read_after_write_is_synchronous {

        @Test
        void the_projection_is_visible_immediately_after_execute_returns() {
            // Given
            CloudEventConverter<DomainEvent> converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:occurrent:projection-docs"))
                    .idMapper(DomainEvent::eventId)
                    .build();
            InMemoryEventStore eventStore = new InMemoryEventStore();
            SynchronousSubscriptionModel synchronousSubscriptions = new SynchronousSubscriptionModel();

            ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
            ViewStateRepository<String, String> repository = ViewStateRepository.create(store::get, store::put);

            ProjectionRunner.agnostic(synchronousSubscriptions, converter).project("current-name", currentNameProjection(), repository);

            ApplicationService<DomainEvent> applicationService = GenericApplicationService.builder(eventStore, converter)
                    .synchronousSubscriptions(synchronousSubscriptions)
                    .transactionExecutor(TransactionExecutor.noTransaction())
                    .build();

            // When
            applicationService.execute("johan", events -> List.of(new NameDefined(UUID.randomUUID().toString(), new Date(), "johan", "Johan Haleby")));

            // Then
            // No await: the projection was updated synchronously, within execute(...), so it must already be visible.
            // An await here would pass whether the update was synchronous or merely fast, and would not prove the point.
            assertThat(store.get("johan")).isEqualTo("Johan Haleby");
        }
    }

    @Nested
    @DisplayName("when push and pull are compared for the same projection")
    class When_push_and_pull_agree_on_the_same_projection {

        private InMemorySubscriptionModel subscriptionModel;
        private InMemoryEventStore eventStore;
        private CloudEventConverter<DomainEvent> converter;

        @BeforeEach
        void setup() {
            subscriptionModel = new InMemorySubscriptionModel();
            eventStore = new InMemoryEventStore(subscriptionModel);
            converter = new JacksonCloudEventConverter.Builder<DomainEvent>(new ObjectMapper(), URI.create("urn:occurrent:projection-docs"))
                    .idMapper(DomainEvent::eventId)
                    .build();
        }

        @AfterEach
        void shutdown() {
            subscriptionModel.shutdown();
        }

        @Test
        void the_pushed_store_state_equals_the_pulled_query_state_for_the_same_instance() {
            // Given
            ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
            ViewStateRepository<String, String> repository = ViewStateRepository.create(store::get, store::put);
            ProjectionRunner.agnostic(subscriptionModel, converter).project("current-name", currentNameProjection(), repository);

            // When
            write("johan",
                    new NameDefined(UUID.randomUUID().toString(), new Date(), "johan", "Johan"),
                    new NameWasChanged(UUID.randomUUID().toString(), new Date(), "johan", "Johan Haleby"));
            // A second instance, so the pull side has to scope to one of them. With a single instance the scoping is a
            // no-op and this test cannot tell a correctly scoped fold from one that folds everything.
            write("eve", new NameDefined(UUID.randomUUID().toString(), new Date(), "eve", "Eve"));
            assertThat(subscriptionModel.waitUntilAllEventsProcessed()).isTrue();
            assertThat(store.get("johan")).isEqualTo("Johan Haleby");

            DomainEventQueries<DomainEvent> queries = new DomainEventQueries<>(eventStore, converter);
            String pulled = ProjectionExtensionsKt.project(queries, currentNameProjection(), "johan");

            // Then
            // The pull folds the same events on demand; it must agree with what the push side already materialized,
            // not merely equal a hardcoded expectation.
            assertThat(pulled).isEqualTo(store.get("johan"));
        }

        private void write(String streamId, DomainEvent... events) {
            eventStore.write(streamId, asList(events).stream().map(converter::toCloudEvent).toList());
        }
    }

    private static Projection<String, DomainEvent, String> currentNameProjection() {
        return Projection.<String, DomainEvent, String>builder("")
                .id(DomainEvent::userId)
                .on(NameDefined.class, (state, event) -> event.name())
                .on(NameWasChanged.class, (state, event) -> event.name())
                .build();
    }
}
