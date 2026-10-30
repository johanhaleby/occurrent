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

import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;

import java.util.List;

/**
 * Adds the bindings a {@code DcbAppendResult}, {@code DcbEventStream}, {@code DcbReadOptions} or snapshot DSL
 * {@code Snapshot} record pattern now needs (ADR 148). Each record's canonical constructor grew components, so a record pattern that still names the old
 * ones stops compiling, since a record pattern has to name every component of the canonical constructor. A
 * {@code DcbAppendResult} pattern with four bindings gains {@code var markerVersions}, a {@code DcbEventStream}
 * pattern with three gains {@code var markerVersions}, and a {@code DcbReadOptions} pattern with four gains
 * {@code var versionsRequested, var afterVersions}, and a {@code Snapshot} pattern with three gains
 * {@code var markerVersions}. A binding name already bound in scope gets a numbered variant
 * ({@code markerVersions1} and so on). A pattern of any other arity is left alone. Nothing in this repository uses
 * these patterns, so this exists for a caller's own source. See doc/migration/upgrading-to-0.35.0.md.
 */
public class MigrateDcbMarkerVersionsRecordPattern extends Recipe {

    private static final List<AddRecordPatternBindings.Target> TARGETS = List.of(
            AddRecordPatternBindings.Target.of("org.occurrent.eventstore.api.dcb.DcbAppendResult", 4, "markerVersions"),
            AddRecordPatternBindings.Target.of("org.occurrent.eventstore.api.dcb.DcbEventStream", 3, "markerVersions"),
            AddRecordPatternBindings.Target.of("org.occurrent.eventstore.api.dcb.DcbReadOptions", 4, "versionsRequested", "afterVersions"),
            AddRecordPatternBindings.Target.of("org.occurrent.dsl.snapshot.Snapshot", 3, "markerVersions"));

    @Override
    public String getDisplayName() {
        return "Add the marker version bindings to a DcbAppendResult/DcbEventStream/DcbReadOptions/Snapshot record pattern";
    }

    @Override
    public String getDescription() {
        return "DcbAppendResult, DcbEventStream, DcbReadOptions and the snapshot DSL's Snapshot canonical constructors grew components for " +
               "marker versions (ADR 148). A record pattern naming only the original components stops compiling, " +
               "since a record pattern has to name every canonical component. This appends `var markerVersions` " +
               "to a four-component DcbAppendResult pattern and a three-component DcbEventStream pattern, and " +
               "`var versionsRequested, var afterVersions` to a four-component DcbReadOptions pattern, and " +
               "`var markerVersions` to a three-component Snapshot pattern, in a switch " +
               "case or an instanceof pattern alike. See doc/migration/upgrading-to-0.35.0.md.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return AddRecordPatternBindings.visitor(TARGETS);
    }
}
