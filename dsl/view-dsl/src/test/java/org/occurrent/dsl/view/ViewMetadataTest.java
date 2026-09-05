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

package org.occurrent.dsl.view;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.occurrent.cloudevents.EventMetadata;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ViewMetadataTest {

    record Registered(String id) {
    }

    private static EventMetadata metadata(String streamId, long position) {
        Map<String, Object> data = new HashMap<>();
        data.put(OccurrentCloudEventExtension.STREAM_ID, streamId);
        data.put(OccurrentCloudEventExtension.POSITION, position);
        return new EventMetadata(data);
    }

    @Nested
    class MetadataAwareView {

        // A view that records the global position of the last folded event, so we can observe which metadata reached the fold.
        private final View<Long, Registered> view = View.create(0L, (Long state, EventMetadata m, Registered event) -> m.getPosition());

        @Test
        void three_arg_evolve_folds_with_the_supplied_metadata() {
            Long state = view.evolve(view.initialState(), metadata("stream-1", 42L), new Registered("a"));

            assertThat(state).isEqualTo(42L);
        }

        @Test
        void two_arg_evolve_folds_with_empty_metadata() {
            // The metadata-less form delegates with EventMetadata.empty(), whose position is null.
            Long state = view.evolve(view.initialState(), new Registered("a"));

            assertThat(state).isNull();
        }

        @Test
        void list_replay_folds_with_empty_metadata() {
            Long state = view.evolve(List.of(new Registered("a"), new Registered("b")));

            assertThat(state).isNull();
        }
    }

    @Nested
    class PlainViewIgnoresMetadata {

        // A plain 2-arg view has no metadata-aware fold; the default 3-arg evolve must transparently delegate to it.
        private final View<Integer, Registered> counting = View.create(0, (state, event) -> state + 1);

        @Test
        void default_three_arg_evolve_delegates_to_the_two_arg_fold() {
            Integer state = counting.evolve(0, metadata("stream-1", 7L), new Registered("a"));

            assertThat(state).isEqualTo(1);
        }
    }

    @Nested
    class MetadataAwareMaterializedView {

        @Test
        void keys_the_instance_by_metadata_and_folds_with_it() {
            Map<String, Long> store = new HashMap<>();
            ViewStateRepository<Long, String> repository = ViewStateRepository.create(store::get, store::put);
            // Keyed by the stream id from the metadata, folding the position into the state.
            View<Long, Registered> view = View.create(0L, (Long state, EventMetadata m, Registered event) -> m.getPosition());
            MaterializedView<Registered> materializedView = MaterializedView.create(
                    (EventMetadata m, Registered event) -> m.getStreamId(), view, repository);

            materializedView.update(metadata("stream-1", 100L), new Registered("a"));

            assertThat(store).containsEntry("stream-1", 100L);
        }

        @Test
        void event_only_update_folds_with_empty_metadata() {
            Map<String, Integer> store = new HashMap<>();
            ViewStateRepository<Integer, String> repository = ViewStateRepository.create(store::get, store::put);
            View<Integer, Registered> counting = View.create(0, (state, event) -> state + 1);
            MaterializedView<Registered> materializedView = MaterializedView.create(Registered::id, counting, repository);

            // The event-only overload is still keyed by the event, and the fold sees empty metadata via the default path.
            materializedView.update(new Registered("a"));
            materializedView.update(new Registered("a"));

            assertThat(store).containsEntry("a", 2);
        }
    }

    @Nested
    class NoArgCreate {

        @Test
        void create_with_no_argument_starts_from_null_like_create_of_null() {
            View<Boolean, Registered> view = View.create((state, event) -> !state);

            assertThat(view.initialState()).isNull();
        }

        @Test
        void metadata_aware_create_with_no_argument_starts_from_null_like_create_of_null() {
            View<Long, Registered> view = View.create((Long state, EventMetadata m, Registered event) -> m.getPosition());

            assertThat(view.initialState()).isNull();
        }
    }
}
