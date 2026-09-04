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

package org.occurrent.dsl.projection;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.dsl.view.View;
import org.occurrent.filter.Filter;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProjectionTest {

    sealed interface AccountEvent permits AccountRegistered, AccountClosed, UsernameChanged {
        String accountId();
    }

    record AccountRegistered(String accountId, String username) implements AccountEvent {
    }

    record AccountClosed(String accountId) implements AccountEvent {
    }

    record UsernameChanged(String accountId, String newUsername) implements AccountEvent {
    }

    private static Projection<Boolean, AccountEvent, String> isUsernameClaimed(String username) {
        return Projection.<Boolean, AccountEvent, String>builder(false)
                .id(AccountEvent::accountId)
                .on(AccountRegistered.class, (state, event) -> event.username().equals(username))
                .on(AccountClosed.class, (state, event) -> false)
                .on(UsernameChanged.class, (state, event) -> event.newUsername().equals(username))
                .build();
    }

    @Nested
    class Dispatch {

        @Test
        void folds_events_through_the_handler_registered_for_each_type() {
            Projection<Boolean, AccountEvent, String> projection = isUsernameClaimed("bob");
            View<Boolean, AccountEvent> view = projection.view();

            Boolean state = view.evolve(view.initialState(), new AccountRegistered("1", "bob"));
            assertThat(state).isTrue();

            state = view.evolve(state, new UsernameChanged("1", "alice"));
            assertThat(state).isFalse();
        }

        @Test
        void returns_state_unchanged_for_an_event_type_without_a_handler() {
            // Only AccountRegistered has a handler; AccountClosed must be a no-op.
            Projection<Boolean, AccountEvent, String> projection = Projection.<Boolean, AccountEvent, String>builder(false)
                    .id(AccountEvent::accountId)
                    .on(AccountRegistered.class, (state, event) -> true)
                    .build();

            Boolean state = projection.view().evolve(true, new AccountClosed("1"));

            assertThat(state).isTrue();
        }

        @Test
        void falls_back_to_a_handler_registered_for_a_supertype_when_no_exact_handler_matches() {
            // Handler keyed on the sealed parent interface; fed a concrete implementer.
            Projection<Integer, AccountEvent, String> countingProjection = Projection.<Integer, AccountEvent, String>builder(0)
                    .id(AccountEvent::accountId)
                    .on(AccountEvent.class, (state, event) -> state + 1)
                    .build();

            Integer count = countingProjection.view().evolve(
                    new AccountRegistered("1", "bob"),
                    new UsernameChanged("1", "alice"),
                    new AccountClosed("1"));

            assertThat(count).isEqualTo(3);
        }

        @Test
        void prefers_the_exact_type_handler_over_a_supertype_handler() {
            Projection<String, AccountEvent, String> projection = Projection.<String, AccountEvent, String>builder("")
                    .id(AccountEvent::accountId)
                    .on(AccountEvent.class, (state, event) -> "supertype")
                    .on(AccountRegistered.class, (state, event) -> "exact")
                    .build();

            assertThat(projection.view().evolve("", new AccountRegistered("1", "bob"))).isEqualTo("exact");
            assertThat(projection.view().evolve("", new AccountClosed("1"))).isEqualTo("supertype");
        }
    }

    @Nested
    class EventTypes {

        @Test
        void are_exactly_the_registered_handler_types() {
            Projection<Boolean, AccountEvent, String> projection = isUsernameClaimed("bob");

            assertThat(projection.eventTypes())
                    .containsExactlyInAnyOrder(AccountRegistered.class, AccountClosed.class, UsernameChanged.class);
        }

        @Test
        void are_empty_when_no_handler_is_registered() {
            Projection<Boolean, AccountEvent, String> projection = Projection.<Boolean, AccountEvent, String>builder(false)
                    .id(AccountEvent::accountId)
                    .build();

            assertThat(projection.eventTypes()).isEmpty();
        }
    }

    @Nested
    class Id {

        @Test
        void derives_the_view_instance_id_from_the_event() {
            Projection<Boolean, AccountEvent, String> projection = isUsernameClaimed("bob");

            assertThat(projection.id().apply(new AccountRegistered("acc-1", "bob"))).isEqualTo("acc-1");
        }

        @Test
        void may_return_null_to_skip_an_event() {
            Projection<Boolean, AccountEvent, String> projection = Projection.<Boolean, AccountEvent, String>builder(false)
                    .id(event -> event instanceof AccountClosed ? null : event.accountId())
                    .on(AccountRegistered.class, (state, event) -> true)
                    .build();

            assertThat(projection.id().apply(new AccountRegistered("acc-1", "bob"))).isEqualTo("acc-1");
            assertThat(projection.id().apply(new AccountClosed("acc-1"))).isNull();
        }
    }

    @Nested
    class MetadataAware {

        private static EventMetadata metadata(String streamId, long position) {
            Map<String, Object> data = new HashMap<>();
            data.put(OccurrentCloudEventExtension.STREAM_ID, streamId);
            data.put(OccurrentCloudEventExtension.POSITION, position);
            return new EventMetadata(data);
        }

        // A projection keyed on the stream id (metadata) rather than a domain field, folding the global position.
        private static Projection<Long, AccountEvent, String> lastPositionPerStream() {
            return Projection.<Long, AccountEvent, String>builder(0L)
                    .id((m, event) -> m.getStreamId())
                    .on(AccountRegistered.class, (state, m, event) -> m.getPosition())
                    .build();
        }

        @Test
        void keys_the_view_instance_by_stream_id_from_metadata() {
            Projection<Long, AccountEvent, String> projection = lastPositionPerStream();

            String instanceId = projection.idWithMetadata().apply(metadata("stream-1", 42L), new AccountRegistered("acc-1", "bob"));

            assertThat(instanceId).isEqualTo("stream-1");
        }

        @Test
        void folds_using_the_position_from_metadata() {
            Projection<Long, AccountEvent, String> projection = lastPositionPerStream();

            Long state = projection.view().evolve(projection.view().initialState(), metadata("stream-1", 42L), new AccountRegistered("acc-1", "bob"));

            assertThat(state).isEqualTo(42L);
        }

        @Test
        void metadata_less_evolve_folds_with_empty_metadata() {
            Projection<Long, AccountEvent, String> projection = lastPositionPerStream();

            // The query/replay path has no CloudEvent, so it folds with EventMetadata.empty() whose position is null.
            Long state = projection.view().evolve(0L, new AccountRegistered("acc-1", "bob"));

            assertThat(state).isNull();
        }

        @Test
        void event_only_id_still_works_alongside_metadata_folds() {
            // Keyed by a domain field via the event-only id(...), but folding with metadata via the 3-arg on(...).
            Projection<Long, AccountEvent, String> projection = Projection.<Long, AccountEvent, String>builder(0L)
                    .id(AccountEvent::accountId)
                    .on(AccountRegistered.class, (state, m, event) -> m.getPosition())
                    .build();

            assertThat(projection.id().apply(new AccountRegistered("acc-1", "bob"))).isEqualTo("acc-1");
            assertThat(projection.view().evolve(0L, metadata("stream-1", 7L), new AccountRegistered("acc-1", "bob"))).isEqualTo(7L);
        }
    }

    @Nested
    class MetadataKeyedFlag {

        @Test
        void is_true_after_id_biFunction() {
            Projection<Long, AccountEvent, String> projection = Projection.<Long, AccountEvent, String>builder(0L)
                    .id((metadata, event) -> metadata.getStreamId())
                    .on(AccountRegistered.class, (state, metadata, event) -> metadata.getPosition())
                    .build();

            assertThat(projection.metadataKeyed()).isTrue();
        }

        @Test
        void is_false_after_id_function_even_though_it_delegates_to_a_biFunction_internally() {
            Projection<Boolean, AccountEvent, String> projection = isUsernameClaimed("bob");

            assertThat(projection.metadataKeyed()).isFalse();
        }

        @Test
        void is_false_for_a_singleton_projection() {
            Projection<Boolean, AccountEvent, String> projection = Projection.<Boolean, AccountEvent>singletonBuilder(false)
                    .on(AccountRegistered.class, (state, event) -> true)
                    .build();

            assertThat(projection.metadataKeyed()).isFalse();
        }

        @Test
        void is_preserved_across_adapt_when_declared_metadata_keyed() {
            Projection<Integer, AccountRegistered, String> narrow = Projection.<Integer, AccountRegistered, String>builder(0)
                    .id((metadata, event) -> metadata.getStreamId())
                    .on(AccountRegistered.class, (state, event) -> state + 1)
                    .build();

            Projection<Integer, AccountEvent, String> widened = Projection.adapt(narrow, AccountRegistered.class);

            assertThat(widened.metadataKeyed()).isTrue();
        }

        @Test
        void is_preserved_across_adapt_when_declared_event_only_keyed() {
            Projection<Integer, AccountRegistered, String> narrow = Projection.<Integer, AccountRegistered, String>builder(0)
                    .id(AccountRegistered::accountId)
                    .on(AccountRegistered.class, (state, event) -> state + 1)
                    .build();

            Projection<Integer, AccountEvent, String> widened = Projection.adapt(narrow, AccountRegistered.class);

            assertThat(widened.metadataKeyed()).isFalse();
        }
    }

    @Nested
    class ExplicitFilter {

        @Test
        void is_null_by_default() {
            assertThat(isUsernameClaimed("bob").filter()).isNull();
        }

        @Test
        void is_kept_when_set() {
            Filter filter = Filter.subject("account-1");
            Projection<Boolean, AccountEvent, String> projection = Projection.<Boolean, AccountEvent, String>builder(false)
                    .id(AccountEvent::accountId)
                    .on(AccountRegistered.class, (state, event) -> true)
                    .filter(filter)
                    .build();

            assertThat(projection.filter()).isSameAs(filter);
        }
    }

    @Nested
    class Validation {

        @Test
        void build_requires_an_id_function() {
            Projection.Builder<Boolean, AccountEvent, String> builder = Projection.<Boolean, AccountEvent, String>builder(false)
                    .on(AccountRegistered.class, (state, event) -> true);

            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("id");
        }

        @Test
        void event_types_are_an_immutable_copy() {
            Projection<Boolean, AccountEvent, String> projection = isUsernameClaimed("bob");

            assertThatThrownBy(() -> projection.eventTypes().clear())
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void id_cannot_be_set_twice() {
            Projection.Builder<Boolean, AccountEvent, String> builder = Projection.<Boolean, AccountEvent, String>builder(false)
                    .id(AccountEvent::accountId);

            assertThatThrownBy(() -> builder.id(event -> "other"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("id");
        }

        @Test
        void filter_cannot_be_set_twice() {
            Projection.Builder<Boolean, AccountEvent, String> builder = Projection.<Boolean, AccountEvent, String>builder(false)
                    .id(AccountEvent::accountId)
                    .filter(Filter.all());

            assertThatThrownBy(() -> builder.filter(Filter.all()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("filter");
        }

        @Test
        void singleton_builds_without_an_id() {
            Projection<Boolean, AccountEvent, String> projection = Projection.<Boolean, AccountEvent>singletonBuilder(false)
                    .on(AccountRegistered.class, (state, event) -> true)
                    .build();

            assertThat(projection.id()).isNull();
        }

        @Test
        void id_then_singleton_throws() {
            Projection.Builder<Boolean, AccountEvent, String> builder = Projection.<Boolean, AccountEvent, String>builder(false)
                    .id(AccountEvent::accountId);

            assertThatThrownBy(builder::singleton)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("singleton");
        }

        @Test
        void singleton_then_id_throws() {
            Projection.Builder<Boolean, AccountEvent, String> builder = Projection.<Boolean, AccountEvent>singletonBuilder(false);

            assertThatThrownBy(() -> builder.id(AccountEvent::accountId))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("singleton");
        }
    }

    @Nested
    class Adapt {

        // A projection defined narrowly over AccountRegistered, later widened to the whole AccountEvent stream.
        @Test
        void widens_a_projection_to_a_broader_event_type_and_skips_foreign_events() {
            Projection<Integer, AccountRegistered, String> narrow = Projection.<Integer, AccountRegistered, String>builder(0)
                    .id(AccountRegistered::accountId)
                    .on(AccountRegistered.class, (state, event) -> state + 1)
                    .build();

            Projection<Integer, AccountEvent, String> widened = Projection.adapt(narrow, AccountRegistered.class);

            // The widened fold counts registrations and ignores other AccountEvents.
            Integer count = widened.view().evolve(
                    new AccountRegistered("1", "bob"),
                    new AccountClosed("1"),
                    new AccountRegistered("2", "alice"));
            assertThat(count).isEqualTo(2);

            // The widened id skips foreign events.
            assertThat(widened.id().apply(new AccountRegistered("1", "bob"))).isEqualTo("1");
            assertThat(widened.id().apply(new AccountClosed("1"))).isNull();

            assertThat(widened.eventTypes()).containsExactly(AccountRegistered.class);
        }
    }

    @Nested
    class NoArgBuilder {

        @Test
        void builder_with_no_argument_starts_from_null_like_builder_of_null() {
            Projection<Boolean, AccountEvent, String> projection = Projection.<Boolean, AccountEvent, String>builder()
                    .id(AccountEvent::accountId)
                    .build();

            assertThat(projection.view().initialState()).isNull();
        }

        @Test
        void singleton_builder_with_no_argument_starts_from_null_like_singleton_builder_of_null() {
            Projection<Boolean, AccountEvent, String> projection = Projection.<Boolean, AccountEvent>singletonBuilder()
                    .build();

            assertThat(projection.view().initialState()).isNull();
        }
    }
}
