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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Builds the payload/filter fixtures {@link PayloadFilterReadBenchmark} measures: a JSON object with N "needle"
 * fields the filter reads and one filler field padding the payload to a target size, with the needles placed either
 * right after the filler (late) or right before it (early), so a streaming, byte-backed read pays a different
 * skip cost depending on where in the payload it has to look.
 */
// Public, and so are the two nested enums below: JMH's generated benchmark harness lives in a jmh_generated
// sub-package and references PayloadFixtures.FieldPosition/Backing as @Param field types, so both the enums and
// their enclosing class need to be reachable from outside this package even though PayloadFilterReadBenchmark
// itself sits in the same package as this class. Everything else here stays package-private; only the two enums
// are part of that accidental cross-package surface.
public final class PayloadFixtures {

    private PayloadFixtures() {
    }

    public enum FieldPosition {EARLY, LATE}

    public enum Backing {BYTES, MAP}

    static List<String> needlePaths(int leafCount) {
        List<String> paths = new ArrayList<>(leafCount);
        for (int i = 0; i < leafCount; i++) {
            paths.add("needle" + i);
        }
        return paths;
    }

    /**
     * A payload with {@code leafCount} needle fields and a filler field, ordered by {@code position}, sized so the
     * resulting JSON document is approximately {@code targetSizeBytes}.
     */
    static Map<String, Object> payload(int leafCount, int targetSizeBytes, FieldPosition position) {
        List<String> needles = needlePaths(leafCount);
        Map<String, Object> needleFields = new LinkedHashMap<>();
        for (String needle : needles) {
            needleFields.put(needle, needle + "-value");
        }
        int needleFieldsOverheadEstimate = leafCount * 24;
        int fillerLength = Math.max(0, targetSizeBytes - needleFieldsOverheadEstimate);
        String filler = "x".repeat(fillerLength);

        Map<String, Object> result = new LinkedHashMap<>();
        if (position == FieldPosition.EARLY) {
            result.putAll(needleFields);
            result.put("filler", filler);
        } else {
            result.put("filler", filler);
            result.putAll(needleFields);
        }
        return result;
    }

    /**
     * A composed AND filter over every path in {@code paths}, each condition matching the value already in
     * {@code payload}, so evaluating the filter reads every leaf rather than short-circuiting after the first: the
     * worst case for repeated, unmemoized reads that #623 is about.
     */
    static Filter matchAllFilter(List<String> paths, Map<String, Object> payload) {
        Filter filter = null;
        for (String path : paths) {
            Filter leaf = Filter.data(path, Condition.eq(payload.get(path)));
            filter = filter == null ? leaf : filter.and(leaf);
        }
        return filter;
    }

    static CloudEvent eventWithBytes(Map<String, Object> payload) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("urn:occurrent:benchmark"))
                .withType("BenchmarkPayload")
                .withDataContentType("application/json")
                .withData(toJsonBytes(payload))
                .build();
    }

    /**
     * Wraps the payload the way {@code DocumentCloudEventReader} wraps a Mongo-decoded document: already a
     * {@code Map}, reachable through {@link PojoCloudEventData} without a byte round trip.
     */
    static CloudEvent eventWithMap(Map<String, Object> payload) {
        PojoCloudEventData<Map<String, Object>> wrapped = PojoCloudEventData.wrap(payload, PayloadFixtures::toJsonBytes);
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("urn:occurrent:benchmark"))
                .withType("BenchmarkPayload")
                .withDataContentType("application/json")
                .withData(wrapped)
                .build();
    }

    private static byte[] toJsonBytes(Map<String, Object> map) {
        try {
            return new ObjectMapper().writeValueAsBytes(map);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
