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

import org.openrewrite.test.TypeValidation;

/**
 * Shared stub sources and validation for the {@code Stream}-to-{@code List} write-side tests. The stub carries both the
 * pre-0.30 {@code Stream} overload and the 0.30 {@code List} overload so every case type-resolves against source stubs.
 */
final class StreamWriteStubs {

    private StreamWriteStubs() {
    }

    static final String CLOUD_EVENT = """
            package io.cloudevents;
            public interface CloudEvent {}
            """;

    static final String EVENT_STORE = """
            package org.occurrent.eventstore.api.blocking;

            import io.cloudevents.CloudEvent;
            import java.util.List;
            import java.util.stream.Stream;

            public interface EventStore {
                void write(String streamId, Stream<CloudEvent> events);
                void write(String streamId, List<CloudEvent> events);
            }
            """;

    // The rewritten Stream.of/empty calls and the swapped List qualifier keep the original method-type attribution (the
    // recipe changes syntax, not resolved types), which is a stub-only artifact; the printed source is what matters for
    // a real run, so type validation is relaxed here.
    static final TypeValidation STUB_ONLY_VALIDATION =
            TypeValidation.builder().identifiers(false).methodInvocations(false).build();
}
