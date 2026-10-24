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

import org.openrewrite.Cursor;
import org.openrewrite.ExecutionContext;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The shared mechanics of the record-pattern recipes, {@link MigrateAppendResultRecordPattern} and
 * {@link MigrateDcbMarkerVersionsRecordPattern}. A record grew components, so a deconstruction pattern against it
 * that still has the old number of bindings stops compiling. For each {@link Target} this appends one
 * {@code var <component name>} binding per added component to any pattern of exactly the old arity. Any other arity
 * is left alone, since four where three was expected means the binding is already there, and anything else is not
 * a shape this knows how to extend.
 */
final class AddRecordPatternBindings {

    private static final String SYNTHESIZED_NAMES = AddRecordPatternBindings.class.getName() + ".synthesizedNames";

    /**
     * A record that grew components. {@code addedComponents} are the names of the trailing components the old
     * canonical constructor lacked, in declaration order. Each is also the default binding name.
     */
    record Target(String recordType, int oldArity, List<String> addedComponents) {

        static Target of(String recordType, int oldArity, String... addedComponents) {
            return new Target(recordType, oldArity, List.of(addedComponents));
        }
    }

    private AddRecordPatternBindings() {
    }

    static TreeVisitor<?, ExecutionContext> visitor(List<Target> targets) {
        return new JavaIsoVisitor<>() {

            @Override
            public J.DeconstructionPattern visitDeconstructionPattern(J.DeconstructionPattern pattern, ExecutionContext ctx) {
                J.DeconstructionPattern d = super.visitDeconstructionPattern(pattern, ctx);

                Target target = targets.stream()
                        .filter(t -> TypeUtils.isOfClassType(d.getType(), t.recordType()))
                        .findFirst()
                        .orElse(null);
                if (target == null) {
                    return d;
                }

                List<J> nested = d.getNested();
                if (nested.size() != target.oldArity()) {
                    return d;
                }
                if (!(nested.get(nested.size() - 1) instanceof J.VariableDeclarations last)) {
                    return d;
                }
                if (last.getVariables().size() != 1) {
                    return d;
                }

                List<J> extended = new ArrayList<>(nested);
                for (String component : target.addedComponents()) {
                    extended.add(binding(last, d.getType(), component));
                }
                return d.withNested(extended);
            }

            private J.VariableDeclarations binding(J.VariableDeclarations last, JavaType recordType, String component) {
                J.VariableDeclarations.NamedVariable lastVar = last.getVariables().get(0);

                // Resolved from the record's own member list rather than guessed, so the synthesized binding is
                // fully type-attributed instead of leaving a gap in the LST for a later visitor to trip over.
                JavaType componentType = componentType(recordType, component);

                J.Identifier varKeyword = new J.Identifier(Tree.randomId(), Space.EMPTY, Markers.EMPTY,
                        Collections.emptyList(), "var", componentType, null);

                // Scope-checked rather than a bare literal, so a pattern whose own earlier component is already
                // named like this (or an enclosing local of that name) gets a non-colliding binding instead of a
                // rewrite that still fails to compile.
                String bindingName = bindingName(component);

                JavaType.Variable lastFieldType = lastVar.getName().getFieldType();
                JavaType.Variable fieldType = lastFieldType == null ? null :
                        lastFieldType.withName(bindingName).withType(componentType);
                J.Identifier newName = lastVar.getName().withId(Tree.randomId())
                        .withSimpleName(bindingName)
                        .withType(componentType)
                        .withFieldType(fieldType);

                JavaType.Variable lastVariableType = lastVar.getVariableType();
                JavaType.Variable variableType = lastVariableType == null ? null :
                        lastVariableType.withName(bindingName).withType(componentType);
                J.VariableDeclarations.NamedVariable newVar = lastVar.withId(Tree.randomId())
                        .withName(newName)
                        .withInitializer(null)
                        .withVariableType(variableType);

                // Cloned from the last binding rather than built from scratch, so the new element's own prefix (the
                // space after the comma the printer inserts) matches every other non-first binding in the pattern.
                // The type expression is always forced to var, never cloned, because the last binding may be
                // explicitly typed and that type is never the new component's.
                return last.withId(Tree.randomId())
                        .withLeadingAnnotations(Collections.emptyList())
                        .withTypeExpression(varKeyword)
                        .withVarargs(null)
                        .withVariables(List.of(newVar));
            }

            // The scope check reads the tree as it was before this visit, so it can't see a binding this visitor
            // already added to a sibling pattern in the same case label or block. Those names are kept on the
            // enclosing case or block and skipped too.
            private String bindingName(String component) {
                Set<String> synthesized = new HashSet<>();
                Cursor anchor = null;
                for (Cursor c = getCursor(); c != null; c = c.getParent()) {
                    if (c.getValue() instanceof J.Case || c.getValue() instanceof J.Block) {
                        if (anchor == null) {
                            anchor = c;
                        }
                        Set<String> names = c.getMessage(SYNTHESIZED_NAMES);
                        if (names != null) {
                            synthesized.addAll(names);
                        }
                    }
                }
                String name = VariableNameUtils.generateVariableName(component, getCursor(), GenerationStrategy.INCREMENT_NUMBER);
                if (synthesized.contains(name)) {
                    Set<String> inScope = VariableNameUtils.findNamesInScope(getCursor());
                    int suffix = 1;
                    do {
                        name = component + suffix++;
                    } while (synthesized.contains(name) || inScope.contains(name));
                }
                if (anchor != null) {
                    anchor.<Set<String>>computeMessageIfAbsent(SYNTHESIZED_NAMES, k -> new HashSet<>()).add(name);
                }
                return name;
            }

            private JavaType componentType(JavaType recordType, String component) {
                JavaType.FullyQualified fullyQualified = TypeUtils.asFullyQualified(recordType);
                if (fullyQualified == null) {
                    return null;
                }
                for (JavaType.Variable member : fullyQualified.getMembers()) {
                    if (component.equals(member.getName())) {
                        return member.getType();
                    }
                }
                return null;
            }
        };
    }
}
