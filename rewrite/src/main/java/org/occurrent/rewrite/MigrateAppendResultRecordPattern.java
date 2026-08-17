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
import org.openrewrite.Tree;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.VariableNameUtils;
import org.openrewrite.java.VariableNameUtils.GenerationStrategy;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.Space;
import org.openrewrite.java.tree.TypeUtils;
import org.openrewrite.marker.Markers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Adds the fourth binding a {@code WriteResult}/{@code DcbAppendResult} record pattern now needs (ADR 132). Both
 * records' canonical constructor grew a fourth component, the append id, so a record pattern that still names three
 * components stops compiling, since a record pattern has to name every component of the canonical constructor.
 * Unlike the by-hand cases {@link MigrateSagaTimerName} leaves a review marker for, a record pattern's arity is a
 * compiler-enforced fact rather than a judgement call, so this rewrite is unconditional wherever it applies. Any
 * three-binding deconstruction pattern against either type gains a fourth, {@code var appendId} by default, or a
 * scope-checked variant of that name ({@code appendId2} and so on) when {@code appendId} is already bound in scope,
 * whether the first three bindings are typed or {@code var}. Nothing in this repository uses either pattern (ADR
 * 132 says so directly), so this exists for a caller's own source. See doc/migration/upgrading-to-0.34.0.md.
 */
public class MigrateAppendResultRecordPattern extends Recipe {

    private static final String WRITE_RESULT = "org.occurrent.eventstore.api.WriteResult";
    private static final String DCB_APPEND_RESULT = "org.occurrent.eventstore.api.dcb.DcbAppendResult";
    private static final String APPEND_ID_BINDING_NAME = "appendId";

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
        return new JavaIsoVisitor<>() {

            @Override
            public J.DeconstructionPattern visitDeconstructionPattern(J.DeconstructionPattern pattern, ExecutionContext ctx) {
                J.DeconstructionPattern d = super.visitDeconstructionPattern(pattern, ctx);

                if (!TypeUtils.isOfClassType(d.getType(), WRITE_RESULT) && !TypeUtils.isOfClassType(d.getType(), DCB_APPEND_RESULT)) {
                    return d;
                }

                List<J> nested = d.getNested();
                // Three is the only pre-upgrade arity; four means this pattern already names the append id (a
                // second run of this recipe, or a caller who added it by hand), and anything else is not the shape
                // this recipe knows how to extend, so it is left alone rather than guessed at.
                if (nested.size() != 3) {
                    return d;
                }
                if (!(nested.get(nested.size() - 1) instanceof J.VariableDeclarations last)) {
                    return d;
                }
                if (last.getVariables().size() != 1) {
                    return d;
                }

                J.VariableDeclarations.NamedVariable lastVar = last.getVariables().get(0);

                // Resolved from the record's own member list rather than guessed, so the synthesized binding is
                // fully type-attributed instead of leaving a gap in the LST for a later visitor to trip over.
                JavaType appendIdType = appendIdComponentType(d.getType());

                J.Identifier varKeyword = new J.Identifier(Tree.randomId(), Space.EMPTY, Markers.EMPTY,
                        Collections.emptyList(), "var", appendIdType, null);

                // Scope-checked rather than a bare "appendId" literal, so a pattern whose own first component is
                // already named "appendId" (or an enclosing local of that name) gets a non-colliding binding
                // instead of a rewrite that still fails to compile.
                String bindingName = VariableNameUtils.generateVariableName(APPEND_ID_BINDING_NAME, getCursor(), GenerationStrategy.INCREMENT_NUMBER);

                JavaType.Variable lastFieldType = lastVar.getName().getFieldType();
                JavaType.Variable appendIdFieldType = lastFieldType == null ? null :
                        lastFieldType.withName(bindingName).withType(appendIdType);
                J.Identifier newName = lastVar.getName().withId(Tree.randomId())
                        .withSimpleName(bindingName)
                        .withType(appendIdType)
                        .withFieldType(appendIdFieldType);

                JavaType.Variable lastVariableType = lastVar.getVariableType();
                JavaType.Variable appendIdVariableType = lastVariableType == null ? null :
                        lastVariableType.withName(bindingName).withType(appendIdType);
                J.VariableDeclarations.NamedVariable newVar = lastVar.withId(Tree.randomId())
                        .withName(newName)
                        .withInitializer(null)
                        .withVariableType(appendIdVariableType);

                // Cloned from the last binding rather than built from scratch, so the new element's own prefix (the
                // space after the comma the printer inserts) matches every other non-first binding in the pattern.
                // The type expression is always forced to var, never cloned, because the last binding may be
                // explicitly typed and that type is never the append id's.
                J.VariableDeclarations appendIdBinding = last.withId(Tree.randomId())
                        .withLeadingAnnotations(Collections.emptyList())
                        .withTypeExpression(varKeyword)
                        .withVarargs(null)
                        .withVariables(List.of(newVar));

                List<J> withAppendId = new ArrayList<>(nested);
                withAppendId.add(appendIdBinding);
                return d.withNested(withAppendId);
            }

            private JavaType appendIdComponentType(JavaType recordType) {
                JavaType.FullyQualified fullyQualified = TypeUtils.asFullyQualified(recordType);
                if (fullyQualified == null) {
                    return null;
                }
                for (JavaType.Variable member : fullyQualified.getMembers()) {
                    if (APPEND_ID_BINDING_NAME.equals(member.getName())) {
                        return member.getType();
                    }
                }
                return null;
            }
        };
    }
}
