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
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.Comment;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.Space;
import org.openrewrite.java.tree.TextComment;
import org.openrewrite.java.tree.TypeUtils;
import org.openrewrite.marker.Markers;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites {@code Stream.of(...)} / {@code Stream.empty()} passed as the events argument to
 * {@code EventStore.write(...)} into {@code List.of(...)} / {@code Collections.emptyList()}, and leaves a review
 * comment on any other {@code Stream}-typed write argument. This is the safe, mechanical slice of the Stream -> List
 * write-side change (ADR 54); everything that would require reading a Stream pipeline's intent is flagged, not
 * transformed.
 */
public class MigrateEventStoreWriteStreamToList extends Recipe {

    // The marker tag is what makes re-runs idempotent: addReviewComment embeds it and isReviewComment looks for it, so
    // both must agree. Keeping it a single constant means a reworded marker can never silently stop being recognized.
    private static final String MARKER_TAG = "Occurrent 0.30 upgrade";
    private static final String REVIEW_MARKER =
            " TODO [" + MARKER_TAG + "]: EventStore.write(...) now takes List<CloudEvent> instead of Stream<CloudEvent>." +
            " Convert this argument (and any Stream operations feeding it) to a List manually. ";

    // write is declared across the EventStore capability interfaces; match on any of them so an inherited call on an
    // EventStore-typed reference is caught regardless of which interface the resolved method belongs to.
    private static final List<MethodMatcher> WRITE_MATCHERS = List.of(
            new MethodMatcher("org.occurrent.eventstore.api.blocking.EventStore write(..)", true),
            new MethodMatcher("org.occurrent.eventstore.api.blocking.UnconditionallyWriteToEventStream write(..)", true),
            new MethodMatcher("org.occurrent.eventstore.api.blocking.ConditionallyWriteToEventStream write(..)", true));

    private static final MethodMatcher STREAM_OF = new MethodMatcher("java.util.stream.Stream of(..)");
    private static final MethodMatcher STREAM_EMPTY = new MethodMatcher("java.util.stream.Stream empty()");

    @Override
    public String getDisplayName() {
        return "Migrate `EventStore.write(...)` arguments from `Stream` to `List`";
    }

    @Override
    public String getDescription() {
        return "Rewrites `Stream.of(...)`/`Stream.empty()` passed to `EventStore.write(...)` into `List.of(...)`/" +
               "`Collections.emptyList()`, and flags any other `Stream`-typed write argument with a review comment " +
               "rather than attempting an unsafe transformation.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);

                boolean isWrite = WRITE_MATCHERS.stream().anyMatch(matcher -> matcher.matches(m));
                if (!isWrite || m.getArguments().isEmpty()) {
                    return m;
                }

                // The events argument is always the last one (write(id, events) or write(id, condition, events)).
                List<Expression> args = m.getArguments();
                Expression eventsArg = args.get(args.size() - 1);

                // Stream.of(a, b) -> List.of(a, b) and Stream.empty() -> Collections.emptyList() have identical call
                // shape, so both are just a qualifier swap (plus a method-name swap for empty()). retarget clears the
                // now-stale method type so the result cannot re-match Stream.of or look Stream-typed on a later cycle,
                // which would otherwise append a spurious review comment.
                if (STREAM_OF.matches(eventsArg)) {
                    maybeAddImport("java.util.List");
                    maybeRemoveImport("java.util.stream.Stream");
                    return replaceLastArgument(m, args, retarget((J.MethodInvocation) eventsArg, "java.util.List", null));
                }

                if (STREAM_EMPTY.matches(eventsArg)) {
                    maybeAddImport("java.util.Collections");
                    maybeRemoveImport("java.util.stream.Stream");
                    return replaceLastArgument(m, args, retarget((J.MethodInvocation) eventsArg, "java.util.Collections", "emptyList"));
                }

                // Anything else that is Stream-typed (a variable, a Function/Consumer over Stream, a lambda) cannot be
                // rewritten safely. Flag it once for manual migration, but never touch an already-migrated List argument.
                if (isStreamTyped(eventsArg) && !alreadyMarked(m)) {
                    return addReviewComment(m);
                }
                return m;
            }

            private J.MethodInvocation replaceLastArgument(J.MethodInvocation m, List<Expression> args, Expression replacement) {
                List<Expression> newArgs = new ArrayList<>(args);
                newArgs.set(newArgs.size() - 1, replacement);
                return m.withArguments(newArgs);
            }

            // Swap the qualifier (and optionally the method name) of a static call, dropping the stale method type.
            private J.MethodInvocation retarget(J.MethodInvocation call, String targetFqn, String newMethodName) {
                J.MethodInvocation retargeted = call
                        .withSelect(qualifier(call.getSelect(), targetFqn))
                        .withMethodType(null);
                return newMethodName == null ? retargeted : retargeted.withName(retargeted.getName().withSimpleName(newMethodName));
            }

            private J.Identifier qualifier(Expression original, String fullyQualifiedName) {
                Space prefix = original == null ? Space.EMPTY : original.getPrefix();
                String simpleName = fullyQualifiedName.substring(fullyQualifiedName.lastIndexOf('.') + 1);
                return new J.Identifier(org.openrewrite.Tree.randomId(), prefix, Markers.EMPTY,
                        new ArrayList<>(), simpleName, JavaType.ShallowClass.build(fullyQualifiedName), null);
            }

            private boolean isStreamTyped(Expression arg) {
                return TypeUtils.isAssignableTo("java.util.stream.Stream", arg.getType());
            }

            private boolean alreadyMarked(J.MethodInvocation m) {
                return m.getPrefix().getComments().stream().anyMatch(this::isReviewComment);
            }

            private boolean isReviewComment(Comment comment) {
                return comment instanceof TextComment && ((TextComment) comment).getText().contains(MARKER_TAG);
            }

            private J.MethodInvocation addReviewComment(J.MethodInvocation m) {
                Space prefix = m.getPrefix();
                List<Comment> comments = new ArrayList<>(prefix.getComments());
                // Print the comment on its own line, then re-indent the call using the invocation's existing whitespace.
                String indent = prefix.getWhitespace();
                comments.add(new TextComment(true, REVIEW_MARKER, indent, Markers.EMPTY));
                return m.withPrefix(prefix.withComments(comments));
            }
        };
    }
}
