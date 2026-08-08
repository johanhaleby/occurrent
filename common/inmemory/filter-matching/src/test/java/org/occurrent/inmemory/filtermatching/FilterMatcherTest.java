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

package org.occurrent.inmemory.filtermatching;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;
import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;
import org.occurrent.filter.Filter.All;
import org.occurrent.filter.Filter.CapabilityFilter;
import org.occurrent.filter.Filter.CompositionFilter;
import org.occurrent.filter.Filter.SingleConditionFilter;
import org.occurrent.filtermatching.DataFieldReader;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.condition.Condition.gt;

/**
 * Covers the two-arg {@code matchesFilter} overload staying exactly as before (three shared subscription call
 * sites rely on it not needing a {@link DataFieldReader}) and the new three-arg overload threading a reader
 * through composition filters.
 */
class FilterMatcherTest {

    private static final CloudEvent EVENT = CloudEventBuilder.v1()
            .withId("id")
            .withSource(URI.create("urn:test"))
            .withType("test")
            .build();

    private static DataFieldReader readerOver(Map<String, Object> payload) {
        return (event, path) -> Optional.ofNullable(payload.get(path));
    }

    @Test
    void the_two_arg_overload_refuses_a_data_filter_because_it_has_no_reader() {
        Filter dataFilter = Filter.data("amount", eq(42));

        assertThatThrownBy(() -> FilterMatcher.matchesFilter(EVENT, dataFilter))
                .as("the overload that takes no reader can never answer a payload condition, so it refuses the way "
                        + "anything refuses a capability it was not built with")
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void the_three_arg_overload_answers_a_data_filter_using_the_supplied_reader() {
        Filter dataFilter = Filter.data("amount", eq(42));
        DataFieldReader reader = readerOver(Map.of("amount", 42));

        assertThat(FilterMatcher.matchesFilter(EVENT, dataFilter, reader)).isTrue();
    }

    @Test
    void a_reader_is_threaded_through_a_composition_filter() {
        Filter both = Filter.type("test").and(Filter.data("amount", eq(42)));
        DataFieldReader reader = readerOver(Map.of("amount", 42));

        assertThat(FilterMatcher.matchesFilter(EVENT, both, reader)).isTrue();
        assertThat(FilterMatcher.matchesFilter(EVENT, both, readerOver(Map.of("amount", 1)))).isFalse();
    }

    @Test
    void a_non_data_filter_behaves_identically_regardless_of_which_overload_is_used() {
        Filter byType = Filter.type("test");

        assertThat(FilterMatcher.matchesFilter(EVENT, byType)).isTrue();
        assertThat(FilterMatcher.matchesFilter(EVENT, byType, DataFieldReader.refusing())).isTrue();
    }

    // Covers the AND-flattening and readAll-batching #623 adds. matchesFilter still answers exactly what it
    // answered before (proven below by the property test), but a composed AND with several data-field leaves now
    // resolves them with one readAll call instead of one read() per leaf.

    @Test
    void an_and_filter_with_several_data_leaves_resolves_them_in_one_readAll_call() {
        Map<String, Object> payload = Map.of("a", 1, "b", 2, "c", 3);
        CountingReader reader = new CountingReader(payload);
        // Filter.and(...) chains left-deep (AND(AND(a,b),c)), the shape #623's benchmark uses.
        Filter filter = Filter.data("a", eq(1)).and(Filter.data("b", eq(2))).and(Filter.data("c", eq(3)));

        assertThat(FilterMatcher.matchesFilter(EVENT, filter, reader)).isTrue();

        assertThat(reader.readAllCalls).hasValue(1);
        assertThat(reader.readCalls).hasValue(0);
        assertThat(reader.lastReadAllPaths).containsExactlyInAnyOrder("a", "b", "c");
    }

    @Test
    void a_deeply_nested_and_chain_still_batches_into_a_single_readAll_call() {
        Filter filter = Filter.data("a", eq(0));
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("a", 0);
        for (String path : List.of("b", "c", "d", "e")) {
            filter = filter.and(Filter.data(path, eq(0)));
            payload.put(path, 0);
        }
        CountingReader reader = new CountingReader(payload);

        assertThat(FilterMatcher.matchesFilter(EVENT, filter, reader)).isTrue();

        assertThat(reader.readAllCalls).hasValue(1);
        assertThat(reader.lastReadAllPaths).containsExactlyInAnyOrder("a", "b", "c", "d", "e");
    }

    @Test
    void an_and_filter_with_no_data_leaves_never_calls_readAll() {
        CountingReader reader = new CountingReader(Map.of());
        Filter filter = Filter.type("test").and(Filter.subject("s"));

        FilterMatcher.matchesFilter(EVENT, filter, reader);

        assertThat(reader.readAllCalls).hasValue(0);
        assertThat(reader.readCalls).hasValue(0);
    }

    @Test
    void a_data_leaf_nested_inside_an_or_falls_back_to_read_since_batching_only_covers_the_and_directly() {
        // AND(data("a"), data("d"), OR(data("b"), data("c"))): flattening the AND collects "a" and "d" (direct
        // SingleConditionFilter operands) but leaves the OR as one opaque operand, so "b" and "c" are resolved by
        // the OR's own recursive evaluation, through the fallback path of the memoizing reader rather than the
        // readAll batch.
        Map<String, Object> payload = Map.of("a", 1, "b", 2, "c", 3, "d", 4);
        CountingReader reader = new CountingReader(payload);
        Filter filter = Filter.data("a", eq(1)).and(Filter.data("d", eq(4))).and(Filter.data("b", eq(99)).or(Filter.data("c", eq(3))));

        assertThat(FilterMatcher.matchesFilter(EVENT, filter, reader)).isTrue();

        assertThat(reader.lastReadAllPaths).containsExactlyInAnyOrder("a", "d");
        assertThat(reader.readCalls.get()).isGreaterThan(0);
    }

    @Test
    void an_and_filter_with_exactly_one_data_leaf_never_calls_readAll_since_there_is_nothing_to_batch() {
        // A single data leaf mixed into an AND with attribute leaves has nothing to share a parse with, so batching
        // it would only add a Map allocation and a lookup around the one read it was always going to be.
        Map<String, Object> payload = Map.of("a", 1);
        CountingReader reader = new CountingReader(payload);
        Filter filter = Filter.type("test").and(Filter.data("a", eq(1)));

        assertThat(FilterMatcher.matchesFilter(EVENT, filter, reader)).isTrue();

        assertThat(reader.readAllCalls).hasValue(0);
        assertThat(reader.readCalls.get()).isGreaterThan(0);
    }

    @Test
    void an_and_filter_with_a_data_leaf_still_refuses_when_given_no_reader() {
        Filter filter = Filter.type("test").and(Filter.data("amount", eq(42)));

        assertThatThrownBy(() -> FilterMatcher.matchesFilter(EVENT, filter))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @RepeatedTest(300)
    void batching_an_and_filter_answers_exactly_what_the_unbatched_evaluation_answers(RepetitionInfo repetitionInfo) {
        Random random = new Random(repetitionInfo.getCurrentRepetition());
        Map<String, Object> payload = RandomPayload.random(random, 3);
        DataFieldReader reader = dottedPathReaderOver(payload);
        Filter filter = RandomFilterTree.random(random, payload, 3);

        boolean viaUnbatchedReference = referenceMatchesFilter(EVENT, filter, reader);
        boolean viaBatchedFilterMatcher = FilterMatcher.matchesFilter(EVENT, filter, reader);

        assertThat(viaBatchedFilterMatcher)
                .as("payload %s, filter %s", payload, filter)
                .isEqualTo(viaUnbatchedReference);
    }

    /**
     * The pre-#623 shape of {@link FilterMatcher#matchesFilter(CloudEvent, Filter, DataFieldReader)}: no AND
     * flattening, no batching, one {@link DataFieldReader#read} per leaf. The property test above compares this
     * against the current, batched implementation across many random filter trees, which is what proves the
     * rewrite answers exactly what the unbatched evaluation would have answered.
     */
    private static boolean referenceMatchesFilter(CloudEvent cloudEvent, Filter filter, DataFieldReader dataFieldReader) {
        return switch (filter) {
            case All ignored -> true;
            case SingleConditionFilter scf -> ConditionMatcher.matchesCondition(cloudEvent, scf.fieldName(), scf.condition(), dataFieldReader);
            case CapabilityFilter cpf -> throw new UnsupportedOperationException("Not used by the random filter trees below");
            case CompositionFilter cf -> {
                Predicate<Filter> matchingPredicate = f -> referenceMatchesFilter(cloudEvent, f, dataFieldReader);
                yield switch (cf.operator()) {
                    case AND -> cf.filters().stream().allMatch(matchingPredicate);
                    case OR -> cf.filters().stream().anyMatch(matchingPredicate);
                };
            }
        };
    }

    // A DataFieldReader that also counts calls and remembers the paths passed to the last readAll, so the batching
    // behaviour itself (not just the boolean outcome) can be asserted on directly.
    private static final class CountingReader implements DataFieldReader {
        private final Map<String, Object> payload;
        private final AtomicInteger readCalls = new AtomicInteger();
        private final AtomicInteger readAllCalls = new AtomicInteger();
        private volatile Collection<String> lastReadAllPaths = List.of();

        private CountingReader(Map<String, Object> payload) {
            this.payload = payload;
        }

        @Override
        public Optional<Object> read(CloudEvent cloudEvent, String path) {
            readCalls.incrementAndGet();
            return Optional.ofNullable(payload.get(path));
        }

        @Override
        public Map<String, Object> readAll(CloudEvent cloudEvent, Collection<String> paths) {
            readAllCalls.incrementAndGet();
            lastReadAllPaths = paths;
            Map<String, Object> result = new LinkedHashMap<>();
            for (String path : paths) {
                Optional.ofNullable(payload.get(path)).ifPresent(value -> result.put(path, value));
            }
            return result;
        }
    }

    // A dotted-path reader over a plain nested Map, the same contract JacksonDataFieldReader honours for a
    // Map-backed event, kept here so this module's tests do not need a dependency on the Jackson module to exercise
    // multi-segment paths.
    private static DataFieldReader dottedPathReaderOver(Object root) {
        return (event, path) -> {
            Object current = root;
            for (String segment : path.split("\\.")) {
                if (!(current instanceof Map<?, ?> map) || !map.containsKey(segment)) {
                    return Optional.empty();
                }
                current = map.get(segment);
            }
            return Optional.ofNullable(current);
        };
    }

    /**
     * Random nested payloads for the property test above. Deliberately no arrays, since array-of-objects traversal
     * is a {@link org.occurrent.filtermatching.DataFieldReader} concern this module's simple map-walking test reader
     * does not even implement the same way a real reader would, and it is irrelevant to what is being proven here,
     * which is that AND-flattening and batching do not change a filter's match outcome.
     */
    private static final class RandomPayload {

        private static final String[] FIELD_NAMES = {"a", "b", "c", "d"};

        private RandomPayload() {
        }

        static Map<String, Object> random(Random random, int depth) {
            Map<String, Object> object = new LinkedHashMap<>();
            int fieldCount = 1 + random.nextInt(FIELD_NAMES.length);
            for (int i = 0; i < fieldCount; i++) {
                object.put(FIELD_NAMES[i], randomValue(random, depth));
            }
            return object;
        }

        private static Object randomValue(Random random, int depth) {
            int choice = depth <= 0 ? random.nextInt(2) : random.nextInt(3);
            return switch (choice) {
                case 0 -> "value" + random.nextInt(5);
                case 1 -> random.nextInt(100);
                default -> random(random, depth - 1);
            };
        }
    }

    /**
     * Random AND/OR filter trees mixing data-field leaves (paths drawn from {@code payload}, real or fabricated,
     * conditions that sometimes match and sometimes do not) with plain attribute leaves, so both the flattening
     * logic and the fallback-to-{@code read} path for a data leaf that batching did not cover get exercised.
     */
    private static final class RandomFilterTree {

        private RandomFilterTree() {
        }

        static Filter random(Random random, Map<String, Object> payload, int depth) {
            if (depth <= 0 || random.nextInt(4) == 0) {
                return randomLeaf(random, payload);
            }
            int operandCount = 2 + random.nextInt(3);
            Filter filter = random(random, payload, depth - 1);
            for (int i = 1; i < operandCount; i++) {
                Filter operand = random(random, payload, depth - 1);
                filter = random.nextBoolean() ? filter.and(operand) : filter.or(operand);
            }
            return filter;
        }

        private static Filter randomLeaf(Random random, Map<String, Object> payload) {
            if (random.nextBoolean()) {
                return Filter.type("test");
            }
            List<String> paths = new ArrayList<>(collectPaths(payload, ""));
            String path = paths.isEmpty() || random.nextInt(4) == 0
                    ? randomPath(random)
                    : paths.get(random.nextInt(paths.size()));
            Object actual = valueAt(payload, path);
            Condition<?> condition = randomCondition(random, actual);
            return Filter.data(path, condition);
        }

        private static Condition<?> randomCondition(Random random, Object actual) {
            if (actual instanceof Number number) {
                int value = number.intValue();
                return switch (random.nextInt(3)) {
                    case 0 -> eq(value); // matches
                    case 1 -> gt(value - 1); // value > value - 1: matches
                    default -> gt(value + 1); // value > value + 1: does not match
                };
            }
            if (actual instanceof String text && random.nextBoolean()) {
                return eq(text);
            }
            return eq("nomatch" + random.nextInt(3));
        }

        private static String randomPath(Random random) {
            int segments = 1 + random.nextInt(2);
            StringBuilder path = new StringBuilder();
            for (int i = 0; i < segments; i++) {
                if (i > 0) {
                    path.append('.');
                }
                path.append(RandomPayload.FIELD_NAMES[random.nextInt(RandomPayload.FIELD_NAMES.length)]);
            }
            return path.toString();
        }

        private static Set<String> collectPaths(Object value, String prefix) {
            Set<String> paths = new java.util.LinkedHashSet<>();
            if (value instanceof Map<?, ?> map) {
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    String path = prefix.isEmpty() ? entry.getKey().toString() : prefix + "." + entry.getKey();
                    paths.add(path);
                    paths.addAll(collectPaths(entry.getValue(), path));
                }
            }
            return paths;
        }

        private static Object valueAt(Object root, String path) {
            Object current = root;
            for (String segment : path.split("\\.")) {
                if (!(current instanceof Map<?, ?> map) || !map.containsKey(segment)) {
                    return null;
                }
                current = map.get(segment);
            }
            return current;
        }
    }
}
