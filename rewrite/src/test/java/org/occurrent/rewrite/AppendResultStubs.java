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
 * The 0.34.0 shape of {@code WriteResult} and {@code DcbAppendResult}, four components each (ADR 132), for the
 * record-pattern migration tests. Handed to the parser as a compiled dependency, the same reason
 * {@code SagaJoinStubs} exists: the source under test is a 0.33.0 caller's record pattern, unchanged, meeting the
 * 0.34.0 classpath whose canonical arity it no longer matches.
 */
final class AppendResultStubs {

    private AppendResultStubs() {
    }

    static final String APPEND_ID = """
            package org.occurrent.eventstore.api;

            public record AppendId(java.util.UUID value) {
            }
            """;

    static final String WRITE_RESULT = """
            package org.occurrent.eventstore.api;

            import java.util.Optional;

            public record WriteResult(String streamId, long oldStreamVersion, long newStreamVersion, Optional<AppendId> appendId) {
            }
            """;

    static final String DCB_APPEND_RESULT = """
            package org.occurrent.eventstore.api.dcb;

            import org.occurrent.eventstore.api.AppendId;

            import java.util.Optional;

            public record DcbAppendResult(long firstSequencePosition, long lastSequencePosition, int eventCount, Optional<AppendId> appendId) {
            }
            """;
}
