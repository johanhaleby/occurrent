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

package org.occurrent.tck.eventstore.reactor;

import io.cloudevents.CloudEvent;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.occurrent.eventstore.api.*;
import org.occurrent.eventstore.api.reactor.*;
import org.occurrent.filter.Filter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.occurrent.tck.ConformanceEvents.SOURCE;
import static org.occurrent.tck.ConformanceEvents.event;

/**
 * The part of a reactive event store's contract that survives only until something blocks on a result.
 * <p>
 * Everything about what a store reads and writes is asserted once, by the blocking suites running over
 * {@link BlockingEventStoreOverReactive}, rather than described a second time in terms of {@code Mono} and {@code Flux}.
 * What that cannot reach is the shape of the publishers themselves: whether the work waits for a subscriber, whether a
 * failure travels through the publisher or is thrown while assembling it, whether a {@code Mono} documented to always
 * emit ever completes empty, and what cancelling a read does. A store can get every one of those wrong and still pass
 * every blocking suite.
 * <p>
 * Why each of them matters to somebody using the store:
 * <ul>
 *   <li>A publisher that writes at assembly time writes even when nobody subscribes, so a {@code Mono} built and
 *       discarded inside a {@code switchIfEmpty} or a cancelled request has already changed the store.</li>
 *   <li>A failure thrown while assembling escapes the reactive chain instead of reaching {@code onErrorResume}, so
 *       error handling written the reactive way never runs.</li>
 *   <li>A {@code Mono} that completes empty where a value was promised turns into whatever default the caller's
 *       operator supplies. {@code count()} silently reads zero rather than failing.</li>
 * </ul>
 * <p>
 * Every wait here is bounded. The event-store CI shards have no rerun backstop, so a store that never completes has to
 * fail the test rather than hang the build.
 * <p>
 * STREAM only, and with no capability declaration. Every reactive store shipping with Occurrent supports STREAM, and
 * these are properties of how a publisher is built rather than of a capability, so asserting them once on the stream
 * side says what there is to say. A DCB-only reactive store declines this suite the way a store declines any suite,
 * by not extending it.
 */
@NullMarked
@DisplayNameGeneration(ReplaceUnderscores.class)
@DisplayName("the reactive contract a blocking bridge cannot see")
public abstract class ReactiveEventStoreConformance {

    /**
     * How long any single publisher here is given. Generous, because it only has to exceed a working store's latency,
     * and a store that has genuinely stalled fails either way.
     */
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    private static final String DEFINED = "NameDefined";
    private static final String STREAM_1 = "stream:1";
    private static final String ABSENT = "stream:absent";

    private @Nullable ReactiveEventStoreFixture fixture;

    /**
     * Creates a fixture whose store contains no events. Called before every test method.
     */
    protected abstract ReactiveEventStoreFixture createFixture();

    @BeforeEach
    final void createTheFixture() {
        this.fixture = requireNonNull(createFixture(), "createFixture() returned null");
    }

    @AfterEach
    final void closeTheFixture() {
        ReactiveEventStoreFixture current = this.fixture;
        this.fixture = null;
        if (current != null) {
            current.close();
        }
    }

    @Nested
    @DisplayName("nothing happens until something subscribes")
    class Laziness {

        @Test
        void a_write_writes_nothing_until_its_mono_is_subscribed() {
            Mono<WriteResult> write = eventStore().write(STREAM_1, Flux.just(event("A", DEFINED)));

            assertThat(await(queries().count(Filter.all())))
                    .as("assembling a write must not write, or a Mono that is built and never subscribed still changes the store")
                    .isZero();

            await(write);

            assertThat(await(queries().count(Filter.all())))
                    .as("subscribing must then write, or the assertion above passes for a store that writes nothing at all")
                    .isOne();
        }

        @Test
        void a_stream_delete_deletes_nothing_until_its_mono_is_subscribed() {
            await(eventStore().write(STREAM_1, Flux.just(event("A", DEFINED))));

            Mono<Void> delete = operations().deleteEventStream(STREAM_1);

            assertThat(await(queries().count(Filter.all()))).isOne();

            awaitCompletion(delete);

            assertThat(await(queries().count(Filter.all()))).isZero();
        }

        @Test
        void an_event_delete_deletes_nothing_until_its_mono_is_subscribed() {
            await(eventStore().write(STREAM_1, Flux.just(event("A", DEFINED))));

            Mono<Void> delete = operations().deleteEvent("A", SOURCE);

            assertThat(await(queries().count(Filter.all()))).isOne();

            awaitCompletion(delete);

            assertThat(await(queries().count(Filter.all()))).isZero();
        }

        @Test
        void a_delete_by_filter_deletes_nothing_until_its_mono_is_subscribed() {
            await(eventStore().write(STREAM_1, Flux.just(event("A", DEFINED))));

            Mono<Void> delete = operations().delete(Filter.all());

            assertThat(await(queries().count(Filter.all()))).isOne();

            awaitCompletion(delete);

            assertThat(await(queries().count(Filter.all()))).isZero();
        }

        @Test
        void an_event_update_updates_nothing_until_its_mono_is_subscribed() {
            await(eventStore().write(STREAM_1, Flux.just(event("A", DEFINED))));

            Mono<CloudEvent> update = operations().updateEvent("A", SOURCE,
                    cloudEvent -> event("A", "NameCorrected"));

            assertThat(typeOfTheOnlyEvent())
                    .as("assembling an update must not update")
                    .isEqualTo(DEFINED);

            await(update);

            assertThat(typeOfTheOnlyEvent()).isEqualTo("NameCorrected");
        }

        private String typeOfTheOnlyEvent() {
            List<CloudEvent> events = awaitAll(queries().query(Filter.all(), 0, 10, SortBy.unsorted()));
            assertThat(events).hasSize(1);
            return events.getFirst().getType();
        }
    }

    @Nested
    @DisplayName("a failure travels through the publisher")
    class Failures {

        @Test
        void a_violated_write_condition_reaches_the_subscriber_rather_than_the_assembling_call() {
            await(eventStore().write(STREAM_1, Flux.just(event("A", DEFINED))));

            Mono<WriteResult> write = assertDoesNotThrow(
                    () -> eventStore().write(STREAM_1, WriteCondition.streamVersionEq(0), Flux.just(event("B", DEFINED))),
                    "assembling a write that is going to fail must not throw, or the failure never reaches onErrorResume");

            assertThatThrownBy(() -> await(write))
                    .isExactlyInstanceOf(WriteConditionNotFulfilledException.class);
        }

        @Test
        void a_duplicate_cloud_event_reaches_the_subscriber_rather_than_the_assembling_call() {
            await(eventStore().write(STREAM_1, Flux.just(event("A", DEFINED))));

            Mono<WriteResult> write = assertDoesNotThrow(
                    () -> eventStore().write("stream:2", Flux.just(event("A", DEFINED))),
                    "assembling a write that is going to fail must not throw, or the failure never reaches onErrorResume");

            assertThatThrownBy(() -> await(write))
                    .isExactlyInstanceOf(DuplicateCloudEventException.class);
        }
    }

    @Nested
    @DisplayName("a publisher promised to emit does emit")
    class Emptiness {

        @Test
        void reading_a_stream_that_does_not_exist_emits_an_empty_stream_rather_than_nothing() {
            EventStream<CloudEvent> stream = await(eventStore().read(ABSENT));

            assertThat(stream)
                    .as("read(..) must emit exactly one EventStream, an empty one for a stream that does not exist, "
                            + "because a caller cannot tell an empty completion apart from a store that lost the read")
                    .isNotNull();
            assertThat(awaitAll(stream.events()))
                    .as("the events inside it must complete empty rather than never completing")
                    .isEmpty();
        }

        @Test
        void every_mono_that_always_answers_answers_on_an_empty_store() {
            assertAll(
                    () -> assertEmits("count(Filter)", queries().count(Filter.all())),
                    () -> assertEmits("exists(Filter)", queries().exists(Filter.all())),
                    () -> assertEmits("exists(String)", eventStore().exists(ABSENT)),
                    () -> assertEmits("currentPosition()", positionOrderedReader().currentPosition())
            );
        }

        /**
         * {@code updateEvent} is deliberately absent from the list above. It is documented to complete empty when there
         * is no such event, so emptiness is its answer rather than a missing one.
         */
        private void assertEmits(String method, Mono<?> mono) {
            assertThat(awaitOptional(mono))
                    .as(method + " is documented to always emit, so an empty completion becomes whatever default the "
                            + "caller's operator supplies rather than an error")
                    .isPresent();
        }
    }

    @Nested
    @DisplayName("cancelling a read")
    class Cancellation {

        @Test
        void taking_only_the_first_event_of_a_query_completes_and_leaves_the_store_readable() {
            await(eventStore().write(STREAM_1, Flux.just(event("A", DEFINED), event("B", DEFINED), event("C", DEFINED))));

            List<CloudEvent> firstOnly = awaitAll(queries().query(Filter.all(), 0, 10, SortBy.unsorted()).take(1));

            assertThat(firstOnly)
                    .as("a query that is cancelled after one event must complete rather than hang, since a caller "
                            + "taking the first match is ordinary use")
                    .hasSize(1);
            assertThat(await(queries().count(Filter.all())))
                    .as("cancelling one read must leave the store readable, so whatever backs the query is released "
                            + "rather than left open")
                    .isEqualTo(3);
        }
    }

    private static <T> T await(Mono<T> mono) {
        T value = mono.block(TIMEOUT);
        if (value == null) {
            throw new AssertionError("The Mono completed empty instead of emitting a value.");
        }
        return value;
    }

    private static <T> Optional<T> awaitOptional(Mono<T> mono) {
        return mono.blockOptional(TIMEOUT);
    }

    private static void awaitCompletion(Mono<Void> mono) {
        mono.block(TIMEOUT);
    }

    private static <T> List<T> awaitAll(Flux<T> flux) {
        List<T> values = flux.collectList().block(TIMEOUT);
        return values == null ? List.of() : values;
    }

    private ReactiveEventStoreFixture fixture() {
        ReactiveEventStoreFixture current = this.fixture;
        if (current == null) {
            throw new IllegalStateException("No fixture. One is created per test method, so this is only reachable "
                    + "from a constructor or a @BeforeAll, neither of which a suite should use.");
        }
        return current;
    }

    protected final EventStore eventStore() {
        return fixture().eventStore();
    }

    protected final EventStoreQueries queries() {
        return fixture().queries();
    }

    protected final EventStoreOperations operations() {
        return fixture().operations();
    }

    protected final PositionOrderedReader positionOrderedReader() {
        return fixture().positionOrderedReader();
    }
}
