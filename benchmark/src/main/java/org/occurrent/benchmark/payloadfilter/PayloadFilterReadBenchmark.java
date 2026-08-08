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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the cost {@code #623} is about: a composed payload filter with several data-field leaves, each reading
 * the payload independently with no memoization ({@code ConditionMatcher} calls
 * {@link org.occurrent.filtermatching.DataFieldReader#read(CloudEvent, String)} once per leaf,
 * {@code FilterMatcher.java:46-57} and {@code ConditionMatcher.java:180-191}).
 * <p>
 * Also benchmarks {@link org.occurrent.filtermatching.DataFieldReader#readAll(CloudEvent, java.util.Collection)},
 * the bulk-read entry point PR #626 added: its default implementation is the same per-leaf loop
 * {@code FilterMatcher} already does, so it is not expected to differ from the composed-filter numbers here.
 * {@code JacksonDataFieldReader} does not override it, so this measures the current, unmemoized baseline both
 * paths share, not a hypothetical faster implementation.
 * <p>
 * Follows the protocol #623 asks for: 1, 5 and 20 leaves, 1 KB and 256 KB payloads, the needle fields placed early
 * or late in the payload, against both a byte-backed event (streaming re-parse per leaf) and a Map-backed event
 * (already-decoded, as {@code DocumentCloudEventReader} hands it to the reader). Every condition is built to match,
 * so {@code FilterMatcher}'s AND does not short-circuit and every leaf is actually read; a filter that fails fast on
 * its first leaf costs one read regardless of leafCount, which is not the case this benchmark is measuring.
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
        public void setUp() {
            Map<String, Object> payload = PayloadFixtures.payload(leafCount, payloadSizeBytes, fieldPosition);
            event = backing == PayloadFixtures.Backing.MAP
                    ? PayloadFixtures.eventWithMap(payload)
                    : PayloadFixtures.eventWithBytes(payload);
            paths = PayloadFixtures.needlePaths(leafCount);
            filter = PayloadFixtures.matchAllFilter(paths, payload);
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
}
