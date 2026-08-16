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
 * Finds a call to {@code ReplayAwareSubscriptionModel.of(Object)} or {@code IntrospectableSubscriptionModel.of(Object)},
 * on either the blocking or reactor stack, whose argument is itself typed {@code Object}, and marks it with a
 * review comment. This runs before the rest of {@code renames-0_33.yml} renames the call to
 * {@code findIn(SubscriptionModelCapability)}, a parameter narrowed from {@code Object} to
 * {@code SubscriptionModelCapability}, since {@code of(Object)} is the last point at which the call still matches
 * its own 0.32.0 signature.
 * <p>
 * An {@code Object}-typed argument is the 0.32.0 shape of a caller that resolved its subscription model through a
 * broader static type rather than one of the capability interfaces. The rename would otherwise give such a caller
 * a bare compile error with no pointer back to the migration guide, since the recipe cannot narrow the argument's
 * declared type without knowing what it actually holds. This is the same best-effort-plus-marker shape as
 * {@link AddCheckpointStorageConditionalWriteStubs} and {@link MigrateEventStoreWriteStreamToList}, flagging rather
 * than guessing at a fix.
 */
public class FlagObjectTypedCapabilityLookup extends Recipe {

    private static final String MARKER_TAG = "Occurrent 0.33 upgrade";
    private static final String REVIEW_MARKER =
            " TODO [" + MARKER_TAG + "]: this argument is typed Object, and findIn(SubscriptionModelCapability) " +
            "will not accept that once this call is renamed from of(Object). Type it as SubscriptionModelCapability " +
            "(or a narrower capability) so this compiles again. See doc/migration/upgrading-to-0.33.0.md. ";

    private static final List<MethodMatcher> CAPABILITY_LOOKUP_OF_MATCHERS = List.of(
            new MethodMatcher("org.occurrent.subscription.api.blocking.ReplayAwareSubscriptionModel of(java.lang.Object)"),
            new MethodMatcher("org.occurrent.subscription.api.reactor.ReplayAwareSubscriptionModel of(java.lang.Object)"),
            new MethodMatcher("org.occurrent.subscription.api.blocking.IntrospectableSubscriptionModel of(java.lang.Object)"),
            new MethodMatcher("org.occurrent.subscription.api.reactor.IntrospectableSubscriptionModel of(java.lang.Object)"));

    @Override
    public String getDisplayName() {
        return "Flag an `Object`-typed argument at a renamed capability-lookup call site";
    }

    @Override
    public String getDescription() {
        return "Finds a call to `ReplayAwareSubscriptionModel.of(Object)` or `IntrospectableSubscriptionModel.of(Object)` " +
               "whose argument is itself typed `Object`, and marks it with a review comment before the rest of " +
               "this recipe list renames the call to `findIn(SubscriptionModelCapability)`, a parameter narrowed " +
               "from `Object` that an `Object`-typed argument no longer satisfies.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new JavaIsoVisitor<>() {
            @Override
            public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
                J.MethodInvocation m = super.visitMethodInvocation(method, ctx);

                boolean isCapabilityLookup = CAPABILITY_LOOKUP_OF_MATCHERS.stream().anyMatch(matcher -> matcher.matches(m));
                if (!isCapabilityLookup || m.getArguments().size() != 1) {
                    return m;
                }

                Expression arg = m.getArguments().get(0);
                if (isObjectTyped(arg) && !alreadyMarked(m)) {
                    return addReviewComment(m);
                }
                return m;
            }

            private boolean isObjectTyped(Expression arg) {
                JavaType.FullyQualified fq = TypeUtils.asFullyQualified(arg.getType());
                return fq != null && "java.lang.Object".equals(fq.getFullyQualifiedName());
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
                String indent = prefix.getWhitespace();
                comments.add(new TextComment(true, REVIEW_MARKER, indent, Markers.EMPTY));
                return m.withPrefix(prefix.withComments(comments));
            }
        };
    }
}
