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
import org.openrewrite.Cursor;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Rewrites {@code StepBuilder.join(List, Continuation[, Function])} to {@code on(StepCondition.allOf(...), Continuation[,
 * Function])} (0.34.0 removes {@code join} and {@code Expectation}, see ADR 125's rejected-alternatives note and
 * {@code doc/migration/upgrading-to-0.34.0.md}). Only the literal shape this recipe can prove is rewritten: the
 * {@code expecting} argument must itself be a literal {@code List.of(...)}/{@code Arrays.asList(...)} call, and every
 * element of it a literal {@code Expectation.of(Class)}/{@code Expectation.of(Class, int)} call. A list built elsewhere
 * (a variable, a method call) or containing anything else is left alone, and so is a duplicate-typed pair of
 * expectations whose counts are not both integer literals, since the recipe cannot then prove which one
 * {@code join}'s own collapsing would have kept. None of these left-alone calls compile once {@code join} is removed,
 * so the compiler finds every one of them; see the migration guide for the by-hand translation.
 * <p>
 * Java only. Kotlin's {@code join}/{@code expect<T>} use named arguments, a trailing lambda and an elided default
 * parameter, none of which this recipe's Java-template machinery (or {@code rewrite-kotlin}'s own, more limited
 * template support) can express, the same limitation {@link MigrateSagaTimerName} already documents for this module.
 */
public class MigrateSagaJoinToStepCondition extends Recipe {

    private static final String STEP_BUILDER = "org.occurrent.dsl.saga.flow.StepBuilder";
    private static final String STEP_CONDITION = "org.occurrent.dsl.saga.flow.StepCondition";
    private static final String CONTINUATION = "org.occurrent.dsl.saga.flow.Continuation";
    private static final String EXPECTATION = "org.occurrent.dsl.saga.flow.Expectation";

    private static final MethodMatcher JOIN_WITH_REACTION =
            new MethodMatcher(STEP_BUILDER + " join(java.util.List, " + CONTINUATION + ", java.util.function.Function)");
    private static final MethodMatcher JOIN_WITHOUT_REACTION =
            new MethodMatcher(STEP_BUILDER + " join(java.util.List, " + CONTINUATION + ")");

    // What the template itself needs to resolve StepBuilder.on(...) and StepCondition.allOf/event against, independent
    // of whatever classpath the file being migrated carries. ReceivedEvents is only here because StepBuilder's own
    // 3-argument on(...) overload names it; the template never refers to it directly, since the whenFulfilled
    // placeholder below is typed as the raw java.util.function.Function.
    private static final String STEP_BUILDER_STUB = """
            package org.occurrent.dsl.saga.flow;
            public final class StepBuilder<E, C> {
                public StepBuilder<E, C> on(StepCondition<? extends E> condition, Continuation then) {
                    return this;
                }
                public StepBuilder<E, C> on(StepCondition<? extends E> condition, Continuation then,
                                             java.util.function.Function<ReceivedEvents<E>, java.util.List<C>> whenFulfilled) {
                    return this;
                }
            }
            """;
    private static final String STEP_CONDITION_STUB = """
            package org.occurrent.dsl.saga.flow;
            public interface StepCondition<E> {
                static <E, T extends E> StepCondition<E> event(Class<T> eventType) {
                    return null;
                }
                static <E, T extends E> StepCondition<E> event(Class<T> eventType, int count) {
                    return null;
                }
                @SafeVarargs
                static <E> StepCondition<E> allOf(StepCondition<? extends E> first, StepCondition<? extends E>... rest) {
                    return null;
                }
            }
            """;
    private static final String CONTINUATION_STUB = """
            package org.occurrent.dsl.saga.flow;
            public interface Continuation {
                static Continuation next() {
                    return null;
                }
                static Continuation end() {
                    return null;
                }
            }
            """;
    private static final String RECEIVED_EVENTS_STUB = """
            package org.occurrent.dsl.saga.flow;
            public interface ReceivedEvents<E> {
            }
            """;

    @Override
    public String getDisplayName() {
        return "Migrate a flow saga's `join` to `on(StepCondition.allOf(...))`";
    }

    @Override
    public String getDescription() {
        return "Rewrites `StepBuilder.join(List, Continuation[, Function])` to `on(StepCondition.allOf(...), " +
               "Continuation[, Function])`, the shape `join`'s own deprecation javadoc already documented as " +
               "equivalent. Only proves the literal `List.of(...)`/`Arrays.asList(...)` of literal " +
               "`Expectation.of(...)` shape, collapsing a duplicate-typed pair to the higher of their counts the same " +
               "way `join` did, but only when every one of that pair's counts is an integer literal. Everything else " +
               "is left for the compiler to point at and `doc/migration/upgrading-to-0.34.0.md` to translate by " +
               "hand, Kotlin included.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<>() {

            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);

                if (!isJavaSource() || m.getSelect() == null) {
                    return m;
                }
                boolean withReaction = JOIN_WITH_REACTION.matches(m);
                if (!withReaction && !JOIN_WITHOUT_REACTION.matches(m)) {
                    return m;
                }

                List<Expression> args = m.getArguments();
                List<ConditionSlot> slots = extractConditionSlots(args.get(0));
                if (slots == null) {
                    return m;
                }

                StringBuilder code = new StringBuilder("#{any(" + STEP_BUILDER + ")}.on(StepCondition.allOf(");
                List<Object> templateArgs = new ArrayList<>();
                templateArgs.add(m.getSelect());
                for (int i = 0; i < slots.size(); i++) {
                    ConditionSlot slot = slots.get(i);
                    if (i > 0) {
                        code.append(", ");
                    }
                    code.append("StepCondition.event(#{any(java.lang.Class)}");
                    templateArgs.add(slot.classLiteral);
                    if (slot.collapsedCount != null) {
                        code.append(", ").append(slot.collapsedCount.intValue());
                    } else if (slot.countExpr != null) {
                        code.append(", #{any(int)}");
                        templateArgs.add(slot.countExpr);
                    }
                    code.append(")");
                }
                code.append("), #{any(").append(CONTINUATION).append(")}");
                templateArgs.add(args.get(1));
                if (withReaction) {
                    code.append(", #{any(java.util.function.Function)}");
                    templateArgs.add(args.get(2));
                }
                code.append(")");

                maybeAddImport(STEP_CONDITION);
                // Safe unconditionally: OpenRewrite checks the whole tree for remaining references before actually
                // dropping the import, so a file with one unreachable join call left using Expectation keeps it.
                maybeRemoveImport(EXPECTATION);
                JavaTemplate template = JavaTemplate.builder(code.toString())
                        .imports(STEP_CONDITION)
                        .javaParser(JavaParser.fromJavaVersion()
                                .dependsOn(STEP_BUILDER_STUB, STEP_CONDITION_STUB, CONTINUATION_STUB, RECEIVED_EVENTS_STUB))
                        .build();
                J.MethodInvocation rewritten = template.apply(getCursor(), m.getCoordinates().replace(), templateArgs.toArray());
                return rewritten.withPrefix(m.getPrefix());
            }

            // rewrite-kotlin represents a Kotlin call with the same J nodes the Java LST uses, and K.CompilationUnit
            // is not a J.CompilationUnit, so this is what keeps the Java-syntax template off a Kotlin source (see
            // MigrateSagaTimerName). A Kotlin caller is left to the by-hand steps in doc/migration/upgrading-to-0.34.0.md.
            private boolean isJavaSource() {
                return getCursor().firstEnclosing(J.CompilationUnit.class) != null;
            }

            // Proves the expecting argument is a literal List.of(...)/Arrays.asList(...) of literal Expectation.of(...)
            // calls, in first-appearance-per-type order, and folds each type down to one ConditionSlot. Returns null the
            // moment the shape stops being provable: an expecting expression that is not that literal list, a list
            // element that is not a literal Expectation.of(...) call, an unresolvable class literal, or a duplicate
            // type whose counts are not all integer literals.
            private @Nullable List<ConditionSlot> extractConditionSlots(Expression expecting) {
                if (!(expecting instanceof J.MethodInvocation listCall) || !isListLiteralCall(listCall)) {
                    return null;
                }
                LinkedHashMap<String, List<ExpectationCall>> byType = new LinkedHashMap<>();
                for (Expression element : listCall.getArguments()) {
                    if (!(element instanceof J.MethodInvocation call) || !isExpectationOfCall(call)) {
                        return null;
                    }
                    List<Expression> expectationArgs = call.getArguments();
                    Expression classLiteral = expectationArgs.get(0);
                    Expression countExpr = expectationArgs.size() > 1 ? expectationArgs.get(1) : null;
                    String typeKey = resolvedTypeKey(classLiteral);
                    if (typeKey == null) {
                        return null;
                    }
                    byType.computeIfAbsent(typeKey, key -> new ArrayList<>()).add(new ExpectationCall(classLiteral, countExpr));
                }
                if (byType.isEmpty()) {
                    return null;
                }
                List<ConditionSlot> slots = new ArrayList<>();
                for (List<ExpectationCall> group : byType.values()) {
                    if (group.size() == 1) {
                        ExpectationCall only = group.get(0);
                        slots.add(new ConditionSlot(only.classLiteral, only.countExpr, null));
                        continue;
                    }
                    // Two or more expectations naming the same type: join collapsed these to the higher count (see
                    // StepBuilder's removed toConditions), and a naive per-element translation would instead emit an
                    // allOf with two children matching the same events, which allOf refuses at build time. Reproducing
                    // that collapse needs every count in the group known at rewrite time.
                    int max = 1;
                    for (ExpectationCall expectationCall : group) {
                        Integer literal = literalIntValue(expectationCall.countExpr);
                        if (literal == null) {
                            return null;
                        }
                        max = Math.max(max, literal);
                    }
                    slots.add(new ConditionSlot(group.get(0).classLiteral, null, max));
                }
                return slots;
            }

            private boolean isListLiteralCall(J.MethodInvocation call) {
                JavaType.Method type = call.getMethodType();
                if (type == null) {
                    return false;
                }
                String name = call.getSimpleName();
                if ("of".equals(name) && TypeUtils.isOfClassType(type.getDeclaringType(), "java.util.List")) {
                    return true;
                }
                return "asList".equals(name) && TypeUtils.isOfClassType(type.getDeclaringType(), "java.util.Arrays");
            }

            private boolean isExpectationOfCall(J.MethodInvocation call) {
                JavaType.Method type = call.getMethodType();
                return type != null && "of".equals(call.getSimpleName())
                       && TypeUtils.isOfClassType(type.getDeclaringType(), EXPECTATION);
            }

            private @Nullable String resolvedTypeKey(Expression classLiteral) {
                if (!(classLiteral instanceof J.FieldAccess fieldAccess) || !"class".equals(fieldAccess.getName().getSimpleName())) {
                    return null;
                }
                JavaType.FullyQualified fq = TypeUtils.asFullyQualified(fieldAccess.getTarget().getType());
                return fq == null ? null : fq.getFullyQualifiedName();
            }

            // Expectation.of(Class) with no count argument means 1, the same default Expectation's own constructor applies.
            private @Nullable Integer literalIntValue(@Nullable Expression countExpr) {
                if (countExpr == null) {
                    return 1;
                }
                return countExpr instanceof J.Literal literal && literal.getValue() instanceof Integer value ? value : null;
            }
        };
    }

    private record ExpectationCall(Expression classLiteral, @Nullable Expression countExpr) {
    }

    // Exactly one of countExpr/collapsedCount is set for a group with more than one member; a singleton group carries
    // its original countExpr (possibly null, meaning the default 1) unchanged, since a lone expectation needs no
    // collapsing and its count, literal or not, carries over verbatim.
    private record ConditionSlot(Expression classLiteral, @Nullable Expression countExpr, @Nullable Integer collapsedCount) {
    }
}
