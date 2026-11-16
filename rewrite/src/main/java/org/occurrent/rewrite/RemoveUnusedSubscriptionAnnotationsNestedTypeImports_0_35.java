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

import org.jspecify.annotations.Nullable;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.RemoveImport;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.TypeUtils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Removes an import of a nested type of the internal {@code SubscriptionAnnotations} when the file never names that
 * type by its simple name, which is the unused import {@code ChangeType} adds for an enum constant access.
 * <p>
 * The {@code ChangeType} entries for the nested types in {@code spring-boot-common-internal-0_35.yml} rewrite
 * {@code import org.occurrent.springboot.common.SubscriptionAnnotations.HandlerParameterKind;} into an import of the
 * internal {@code SubscriptionAnnotations} and qualify every use as {@code SubscriptionAnnotations.HandlerParameterKind}.
 * When a use is an enum constant access such as {@code HandlerParameterKind.EVENT}, OpenRewrite's {@code AddImport}
 * counts the qualified {@code SubscriptionAnnotations.HandlerParameterKind.EVENT} as a reference that needs the nested
 * type imported, since the expression has the enum's own type, so it also adds
 * {@code import org.occurrent.springboot.common.internal.SubscriptionAnnotations.HandlerParameterKind;}, which nothing
 * uses. This recipe runs after those entries and removes that import.
 * <p>
 * A file keeps the import when any identifier outside the imports has the nested type's simple name and is not the
 * name part of a qualified reference, whatever its type. That keeps an import a file really uses, at the cost of
 * leaving an unused one in a file that also has an unrelated identifier with the same name.
 */
public class RemoveUnusedSubscriptionAnnotationsNestedTypeImports_0_35 extends Recipe {

    private static final String SUBSCRIPTION_ANNOTATIONS = "org.occurrent.springboot.common.internal.SubscriptionAnnotations";

    @Override
    public String getDisplayName() {
        return "Remove unused imports of the nested types of the internal `SubscriptionAnnotations`";
    }

    @Override
    public String getDescription() {
        return "Removes an import of a nested type of `org.occurrent.springboot.common.internal.SubscriptionAnnotations` " +
               "when the file only refers to that type qualified as `SubscriptionAnnotations.<Nested>`. `ChangeType` " +
               "adds such an import when it qualifies an enum constant access like `HandlerParameterKind.EVENT`.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<>() {
            @Override
            public J.CompilationUnit visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
                J.CompilationUnit c = cu;
                for (J.Import anImport : cu.getImports()) {
                    String nestedType = nestedSubscriptionAnnotationsType(anImport);
                    if (nestedType != null && !isUsedBySimpleName(cu, anImport.getQualid().getSimpleName())) {
                        c = (J.CompilationUnit) new RemoveImport<ExecutionContext>(nestedType, true).visitNonNull(c, ctx, getCursor().getParentOrThrow());
                    }
                }
                return c;
            }
        };
    }

    private static @Nullable String nestedSubscriptionAnnotationsType(J.Import anImport) {
        String simpleName = anImport.getQualid().getSimpleName();
        if (anImport.isStatic() || "*".equals(simpleName)) {
            return null;
        }
        String nestedType = SUBSCRIPTION_ANNOTATIONS + "." + simpleName;
        return TypeUtils.fullyQualifiedNamesAreEqual(anImport.getTypeName(), nestedType) ? nestedType : null;
    }

    private static boolean isUsedBySimpleName(J.CompilationUnit cu, String simpleName) {
        AtomicBoolean used = new AtomicBoolean(false);
        new JavaIsoVisitor<AtomicBoolean>() {
            @Override
            public J.Import visitImport(J.Import anImport, AtomicBoolean found) {
                return anImport;
            }

            @Override
            public J.Package visitPackage(J.Package pkg, AtomicBoolean found) {
                return pkg;
            }

            @Override
            public J.Identifier visitIdentifier(J.Identifier identifier, AtomicBoolean found) {
                if (identifier.getSimpleName().equals(simpleName) && !isNameOfQualifiedReference(identifier)) {
                    found.set(true);
                }
                return super.visitIdentifier(identifier, found);
            }

            private boolean isNameOfQualifiedReference(J.Identifier identifier) {
                Object parent = getCursor().getParentTreeCursor().getValue();
                return parent instanceof J.FieldAccess fieldAccess && fieldAccess.getName().getId().equals(identifier.getId());
            }
        }.visit(cu, used);
        return used.get();
    }
}
