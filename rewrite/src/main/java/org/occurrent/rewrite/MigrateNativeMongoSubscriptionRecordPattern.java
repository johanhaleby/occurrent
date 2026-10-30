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
 * Adds the binding a {@code NativeMongoSubscription} record pattern now needs. The record gained {@code startFailure}
 * as its third component, so that {@code waitUntilStarted} can throw a refused start, and a record pattern has to name
 * every component of the canonical constructor. A pattern with two bindings gains {@code var startFailure}. A binding
 * name already bound in scope gets a numbered variant ({@code startFailure1} and so on). A pattern of any other arity
 * is left alone. See doc/migration/upgrading-to-0.35.0.md.
 */
public class MigrateNativeMongoSubscriptionRecordPattern extends Recipe {

    private static final List<AddRecordPatternBindings.Target> TARGETS = List.of(
            AddRecordPatternBindings.Target.of("org.occurrent.subscription.mongodb.nativedriver.blocking.NativeMongoSubscription", 2, "startFailure"));

    @Override
    public String getDisplayName() {
        return "Add the startFailure binding to a NativeMongoSubscription record pattern";
    }

    @Override
    public String getDescription() {
        return "NativeMongoSubscription's canonical constructor gained a startFailure component. A record pattern " +
               "naming only the original two components stops compiling, since a record pattern has to name every " +
               "canonical component. This appends `var startFailure` to a two-component pattern, in a switch case " +
               "or an instanceof pattern alike. See doc/migration/upgrading-to-0.35.0.md.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return AddRecordPatternBindings.visitor(TARGETS);
    }
}
