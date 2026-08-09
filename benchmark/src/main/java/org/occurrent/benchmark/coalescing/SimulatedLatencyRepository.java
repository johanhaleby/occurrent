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

package org.occurrent.benchmark.coalescing;

import org.occurrent.benchmark.handover.BusySpin;
import org.occurrent.dsl.view.ViewStateRepository;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * A {@link ViewStateRepository} standing in for a real store, charging a fixed round-trip cost once per call plus a
 * small per-key cost for each key that call touches. {@code findAllById} and {@code saveAll} are overridden the way
 * a real bulk-capable repository would override them, so one call handles every key in a batch instead of paying the
 * round trip once per key the way the unoverridden default (and {@code findById}/{@code save}) would. That is the
 * whole reason {@link CoalescingFlushBenchmark} exists. A flush's cost depends on how many of these calls it makes,
 * not how many events it buffered.
 */
final class SimulatedLatencyRepository implements ViewStateRepository<Long, String> {

    private final Map<String, Long> store = new ConcurrentHashMap<>();
    private final long roundTripMicros;
    private final long perKeyMicros;
    private final LongAdder sink;

    SimulatedLatencyRepository(long roundTripMicros, long perKeyMicros, LongAdder sink) {
        this.roundTripMicros = roundTripMicros;
        this.perKeyMicros = perKeyMicros;
        this.sink = sink;
    }

    @Override
    public Optional<Long> findById(String id) {
        BusySpin.spinMicros(roundTripMicros, sink);
        return Optional.ofNullable(store.get(id));
    }

    @Override
    public void save(String id, Long state) {
        BusySpin.spinMicros(roundTripMicros, sink);
        store.put(id, state);
    }

    @Override
    public Map<String, Long> findAllById(Collection<String> ids) {
        BusySpin.spinMicros(roundTripMicros, sink);
        Map<String, Long> result = new LinkedHashMap<>();
        for (String id : ids) {
            BusySpin.spinMicros(perKeyMicros, sink);
            Long value = store.get(id);
            if (value != null) {
                result.put(id, value);
            }
        }
        return result;
    }

    @Override
    public void saveAll(Map<String, Long> states) {
        BusySpin.spinMicros(roundTripMicros, sink);
        for (Map.Entry<String, Long> entry : states.entrySet()) {
            BusySpin.spinMicros(perKeyMicros, sink);
            store.put(entry.getKey(), entry.getValue());
        }
    }
}
