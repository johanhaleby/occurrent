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
 * Adds the fourth binding a {@code WriteResult}/{@code DcbAppendResult} record pattern now needs (ADR 132). Both
 * records' canonical constructor grew a fourth component, the append id, so a record pattern that still names three
 * components stops compiling, since a record pattern has to name every component of the canonical constructor.
 * Unlike the by-hand cases {@link MigrateSagaTimerName} leaves a review marker for, a record pattern's arity is a
 * compiler-enforced fact rather than a judgement call, so this rewrite is unconditional wherever it applies. Any
 * three-binding deconstruction pattern against either type gains a fourth, {@code var appendId} by default, or a
 * scope-checked variant of that name ({@code appendId1} and so on) when {@code appendId} is already bound in scope,
 * whether the first three bindings are typed or {@code var}. Nothing in this repository uses either pattern (ADR
 * 132 says so directly), so this exists for a caller's own source. See doc/migration/upgrading-to-0.34.0.md.
 */
public class MigrateAppendResultRecordPattern extends Recipe {

    // Three is the only pre-upgrade arity. The shared mechanics leave four alone, since that pattern already names
    // the append id (a second run of this recipe, or a caller who added it by hand).
    private static final List<AddRecordPatternBindings.Target> TARGETS = List.of(
            AddRecordPatternBindings.Target.of("org.occurrent.eventstore.api.WriteResult", 3, "appendId"),
            AddRecordPatternBindings.Target.of("org.occurrent.eventstore.api.dcb.DcbAppendResult", 3, "appendId"));

    @Override
    public String getDisplayName() {
        return "Add the append id binding to a WriteResult/DcbAppendResult record pattern";
    }

    @Override
    public String getDescription() {
        return "WriteResult and DcbAppendResult's canonical constructor grew a fourth component, the append id " +
               "(ADR 132). A record pattern naming only the original three components stops compiling, since a " +
               "record pattern has to name every canonical component. This appends `var appendId` as the fourth " +
               "binding to any three-component deconstruction pattern against either type, in a switch case or an " +
               "instanceof pattern alike. See doc/migration/upgrading-to-0.34.0.md.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return AddRecordPatternBindings.visitor(TARGETS);
    }
}
