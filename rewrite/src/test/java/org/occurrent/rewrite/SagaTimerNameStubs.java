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
package org.occurrent.rewrite;

/**
 * The 0.33.0 saga DSL timer types, shaped after the real ones, for the {@code TimerName} migration tests. They are
 * handed to the parser as a compiled dependency rather than as rewritten sources, which is what makes each test a
 * real upgrade: the source under test is a 0.32.0 caller, unchanged, and the classpath it meets is the one the
 * 0.33.0 jar gives it.
 */
final class SagaTimerNameStubs {

    private SagaTimerNameStubs() {
    }

    static final String TIMER_NAME = """
            package org.occurrent.dsl.saga;

            public sealed interface TimerName permits TimerName.Simple, TimerName.Qualified {
                record Simple(String name) implements TimerName {
                    @Override
                    public String encode() {
                        return name;
                    }
                }

                record Qualified(String namespace, String name) implements TimerName {
                    @Override
                    public String encode() {
                        return namespace + ':' + name;
                    }
                }

                static TimerName parse(String name) {
                    int separator = name.indexOf(':');
                    return separator < 0
                            ? new Simple(name)
                            : new Qualified(name.substring(0, separator), name.substring(separator + 1));
                }

                static TimerName of(String namespace, String name) {
                    return new Qualified(namespace, name);
                }

                String encode();
            }
            """;

    static final String SAGA_TIMEOUT = """
            package org.occurrent.dsl.saga;

            public record SagaTimeout(String sagaId, TimerName timerName) {
            }
            """;

    static final String SAGA_EFFECT = """
            package org.occurrent.dsl.saga;

            import java.time.Duration;
            import java.time.Instant;

            public sealed interface SagaEffect<C> permits SagaEffect.StartTimeout, SagaEffect.StartTimeoutAt, SagaEffect.CancelTimeout {
                record StartTimeout<C>(TimerName timerName, Duration after) implements SagaEffect<C> {
                }

                record StartTimeoutAt<C>(TimerName timerName, Instant at) implements SagaEffect<C> {
                }

                record CancelTimeout<C>(TimerName timerName) implements SagaEffect<C> {
                }

                static <C> SagaEffect<C> cancelTimeout(String timerName) {
                    return new CancelTimeout<>(TimerName.parse(timerName));
                }
            }
            """;
}
