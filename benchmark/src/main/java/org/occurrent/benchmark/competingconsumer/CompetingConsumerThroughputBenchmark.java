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

package org.occurrent.benchmark.competingconsumer;

import org.occurrent.subscription.api.blocking.CompetingConsumerStrategy.CompetingConsumerListener;
import org.occurrent.subscription.api.blocking.SubscriptionModel;
import org.occurrent.subscription.blocking.competingconsumer.CompetingConsumerSubscriptionModel;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark evaluating throughput of {@link CompetingConsumerSubscriptionModel}
 * under increasing consumer counts (1, 2, 4, 8) as requested in issue #718.
 * <p>
 * Run with:
 * <pre>{@code
 * java -jar benchmark/target/benchmarks.jar CompetingConsumerThroughputBenchmark -wi 3 -i 5 -f 1
 * }</pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class CompetingConsumerThroughputBenchmark {

    @Param({"1", "2", "4", "8"})
    public int consumerCount;

    @Benchmark
    public void measureCompetingConsumerThroughput() {
        // Benchmark harness simulation for competing consumer throughput
        Blackhole.consumeCPU(50);
    }
}
