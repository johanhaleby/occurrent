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
 * The 0.35.0 shape of {@code DcbAppendResult}, {@code DcbEventStream} and {@code DcbReadOptions}, five, four and six
 * components (ADR 148), for the record-pattern migration tests. Handed to the parser as a compiled dependency, the
 * same reason {@link AppendResultStubs} exists, which stays at the 0.34.0 shape for the 0.34.0 tests. The source
 * under test is a 0.34.0 caller's record pattern, unchanged, meeting the 0.35.0 classpath whose canonical arity it no
 * longer matches. Reuse {@link AppendResultStubs#APPEND_ID} for the append id and {@link AppendResultStubs#WRITE_RESULT}
 * for the type that must stay untouched.
 */
final class DcbMarkerVersionsStubs {

    private DcbMarkerVersionsStubs() {
    }

    static final String CLOUD_EVENT = """
            package io.cloudevents;

            public interface CloudEvent {
            }
            """;

    static final String POSITION_RANGE = """
            package org.occurrent.eventstore.api;

            import java.util.OptionalLong;

            public record PositionRange(OptionalLong afterPosition, OptionalLong upToPosition) {
            }
            """;

    static final String DCB_CONSISTENCY_TOKEN = """
            package org.occurrent.eventstore.api.dcb;

            public final class DcbConsistencyToken {

                private DcbConsistencyToken() {
                }

                public long version() {
                    return 0;
                }
            }
            """;

    static final String DCB_MARKER_VERSIONS = """
            package org.occurrent.eventstore.api.dcb;

            import java.util.Map;

            public record DcbMarkerVersions(Map<String, MarkerVersion> markers) {

                public record MarkerVersion(String epoch, long version, long unstamped) {
                }
            }
            """;

    static final String DCB_APPEND_RESULT = """
            package org.occurrent.eventstore.api.dcb;

            import org.occurrent.eventstore.api.AppendId;

            import java.util.Optional;

            public record DcbAppendResult(long firstSequencePosition, long lastSequencePosition, int eventCount, Optional<AppendId> appendId,
                                          Optional<DcbMarkerVersions> markerVersions) {
            }
            """;

    static final String DCB_EVENT_STREAM = """
            package org.occurrent.eventstore.api.dcb;

            import io.cloudevents.CloudEvent;

            import java.util.List;
            import java.util.Optional;

            public record DcbEventStream(List<CloudEvent> events, long lastSequencePosition, DcbConsistencyToken consistencyToken,
                                         Optional<DcbMarkerVersions> markerVersions) {
            }
            """;

    static final String DCB_READ_OPTIONS = """
            package org.occurrent.eventstore.api.dcb;

            import org.occurrent.eventstore.api.PositionRange;

            import java.util.Optional;
            import java.util.OptionalInt;

            public record DcbReadOptions(PositionRange positionRange, Direction direction, int skip, OptionalInt limit,
                                         boolean versionsRequested, Optional<DcbMarkerVersions> afterVersions) {

                public enum Direction {
                    FORWARD,
                    BACKWARD
                }
            }
            """;
}
