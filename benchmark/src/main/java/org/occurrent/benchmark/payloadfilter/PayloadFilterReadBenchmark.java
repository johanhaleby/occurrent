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

package org.occurrent.benchmark.payloadfilter;

import io.cloudevents.CloudEvent;
import org.occurrent.filter.Filter;
import org.occurrent.filtermatching.jackson.JacksonDataFieldReader;
import org.occurrent.inmemory.filtermatching.FilterMatcher;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the cost {@code #623} is about: a composed payload filter with several data-field leaves, each reading
 * the payload independently with no memoization ({@code ConditionMatcher} calls
 * {@link org.occurrent.filtermatching.DataFieldReader#read(CloudEvent, String)} once per leaf,
 * {@code FilterMatcher.java:46-57} and {@code ConditionMatcher.java:180-191}).
 * <p>
 * {@code JacksonDataFieldReader} now overrides
 * {@link org.occurrent.filtermatching.DataFieldReader#readAll(CloudEvent, java.util.Collection)} (added by PR #626)
 * to resolve every path in one parse instead of one per path. That makes both {@link #matchesFilter} and
 * {@link #readAllPaths} exercise the batched path. {@code FilterMatcher.matchesAndFilter} calls {@code readAll} for
 * a composed filter with two or more data-field leaves, and {@link #readAllPaths} calls it directly. Neither one
 * measures the old, unmemoized baseline any more, but {@link #readEachPathIndependently} does. It calls
 * {@link org.occurrent.filtermatching.DataFieldReader#read(CloudEvent, String)} once per path, the same per-leaf
 * loop {@code ConditionMatcher} ran before {@code readAll} existed, so comparing it against the other two methods is
 * what shows whether the batched path is actually faster.
 * <p>
 * Follows the protocol #623 asks for: 1, 5 and 20 leaves, 1 KB and 256 KB payloads, the needle fields placed early
 * or late in the payload, against both a byte-backed event (streaming re-parse per leaf) and a Map-backed event
 * (already-decoded, as {@code DocumentCloudEventReader} hands it to the reader). Every condition is built to match,
 * so {@code FilterMatcher}'s AND does not short-circuit and every leaf is actually read; a filter that fails fast on
 * its first leaf costs one read regardless of leafCount, which is not the case this benchmark is measuring.
 * <p>
 * {@code FilterMatcher.matchesAndFilter} briefly regressed the fail-fast claim in the previous paragraph. Between
 * PR #647 and its fix, every data path in an AND was resolved through {@code readAll} before any operand was
 * evaluated, so a leading metadata leaf that already decided the result no longer saved a payload read, and a
 * store with no {@link org.occurrent.filtermatching.DataFieldReader} (which answers a read with
 * {@code UnsupportedOperationException}) could throw on a filter that 0.32.0 evaluated to {@code false} without
 * ever touching the payload. Ordered, left-to-right evaluation is restored, and this suite still does not prove it,
 * because a throughput benchmark cannot distinguish "zero reads" from "one very cheap read" at these leaf counts.
 * {@code FilterMatcherTest} asserts the call counts directly instead, once for the no-throw outcome with a refusing
 * reader and once for the zero-{@code read}/zero-{@code readAll} count with a counting reader, both on a type
 * mismatch ahead of two data leaves.
 * <p>
 * Run with, for example:
 * <pre>{@code
 * java -jar benchmark/target/benchmarks.jar PayloadFilterReadBenchmark -wi 3 -i 5 -f 1
 * }</pre>
 * No external service or container is required.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class PayloadFilterReadBenchmark {

    @State(Scope.Benchmark)
    public static class ReadState {

        @Param({"1", "5", "20"})
        public int leafCount;

        @Param({"1024", "262144"})
        public int payloadSizeBytes;

        @Param({"EARLY", "LATE"})
        public PayloadFixtures.FieldPosition fieldPosition;

        @Param({"BYTES", "MAP"})
        public PayloadFixtures.Backing backing;

        private CloudEvent event;
        private Filter filter;
        private List<String> paths;
        private final JacksonDataFieldReader reader = new JacksonDataFieldReader();

        @Setup(Level.Trial)
        public void setUp() throws NoSuchMethodException {
            requireReadAllOverride();
            Map<String, Object> payload = PayloadFixtures.payload(leafCount, payloadSizeBytes, fieldPosition);
            event = backing == PayloadFixtures.Backing.MAP
                    ? PayloadFixtures.eventWithMap(payload)
                    : PayloadFixtures.eventWithBytes(payload);
            paths = PayloadFixtures.needlePaths(leafCount);
            filter = PayloadFixtures.matchAllFilter(paths, payload);
        }

        /**
         * {@link #matchesFilter} and {@link #readAllPaths} are only distinct from {@link #readEachPathIndependently}
         * as long as {@code JacksonDataFieldReader} overrides {@code readAll} with a real batched implementation. If
         * that override is ever removed, {@code readAll} silently falls back to
         * {@link org.occurrent.filtermatching.DataFieldReader}'s default, which calls {@code read} once per path,
         * the exact loop {@link #readEachPathIndependently} already measures, and this benchmark would keep running
         * without ever telling anyone it stopped comparing two different implementations.
         */
        private static void requireReadAllOverride() throws NoSuchMethodException {
            Method readAll = JacksonDataFieldReader.class.getMethod("readAll", CloudEvent.class, Collection.class);
            if (readAll.getDeclaringClass() != JacksonDataFieldReader.class) {
                throw new IllegalStateException(JacksonDataFieldReader.class.getSimpleName() + " no longer overrides readAll(..), "
                        + "so matchesFilter and readAllPaths would silently measure the same per-path loop as "
                        + "readEachPathIndependently instead of the batched path this benchmark exists to compare it against.");
            }
        }
    }

    @Benchmark
    public boolean matchesFilter(ReadState state) {
        return FilterMatcher.matchesFilter(state.event, state.filter, state.reader);
    }

    @Benchmark
    public Map<String, Object> readAllPaths(ReadState state) {
        return state.reader.readAll(state.event, state.paths);
    }

    /**
     * The pre-{@code readAll} baseline, resolving every path in {@code state.paths} with its own
     * {@link org.occurrent.filtermatching.DataFieldReader#read(CloudEvent, String)} call, reparsing a byte-backed
     * payload from the start each time. This is what {@link #matchesFilter} and {@link #readAllPaths} used to
     * measure before {@code JacksonDataFieldReader} started overriding {@code readAll}.
     */
    @Benchmark
    public Map<String, Object> readEachPathIndependently(ReadState state) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (String path : state.paths) {
            state.reader.read(state.event, path).ifPresent(value -> result.put(path, value));
        }
        return result;
    }
}
