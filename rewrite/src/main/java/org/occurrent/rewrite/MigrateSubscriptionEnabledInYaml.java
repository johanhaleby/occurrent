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
import org.openrewrite.yaml.ChangePropertyKey;
import org.openrewrite.yaml.ChangePropertyValue;
import org.openrewrite.yaml.YamlIsoVisitor;
import org.openrewrite.yaml.search.FindProperty;
import org.openrewrite.yaml.tree.Yaml;

/**
 * Rewrites {@code occurrent.subscription.enabled: oldValue} to {@code occurrent.subscription.mode: newValue},
 * checking each {@link Yaml.Document} in a multi-document file on its own rather than the file as a whole.
 * <p>
 * {@code org.openrewrite.yaml.search.FindProperty} used declaratively as a precondition operates on the
 * {@code Yaml.Documents} source file rather than the individual {@code Yaml.Document} within it. In a file with
 * more than one document, separated by {@code ---} for Spring profiles, a single document setting
 * {@code occurrent.subscription.enabled} to {@code oldValue} satisfies the precondition for the entire file.
 * {@code org.openrewrite.yaml.ChangePropertyValue} then correctly skips a document whose own value differs, but
 * {@code org.openrewrite.yaml.ChangePropertyKey} does not check the value at all: it renames the key in every
 * document that has it, including one whose own {@code enabled} held the other boolean and was never touched by
 * the value change. That document ends up holding the raw boolean under {@code occurrent.subscription.mode}, an
 * enum it does not bind to. This recipe re-runs the value check per document instead, so a document is only
 * touched when its own value matches.
 * <p>
 * Not referenced directly from a declarative recipe list: it has no public no-arg constructor, and this module
 * neither compiles with {@code -parameters} nor depends on Lombok, so there is no way for the declarative loader
 * to bind an {@code oldValue}/{@code newValue} options map onto it by reflection the way it does for a stock
 * OpenRewrite recipe. Each value gets its own no-arg subclass instead, the same shape
 * {@link DropRedundantYamlProperty} already uses for its two property pairs.
 */
public abstract class MigrateSubscriptionEnabledInYaml extends Recipe {

    private static final String OLD_PROPERTY_KEY = "occurrent.subscription.enabled";
    private static final String NEW_PROPERTY_KEY = "occurrent.subscription.mode";

    private final String oldValue;
    private final String newValue;

    protected MigrateSubscriptionEnabledInYaml(String oldValue, String newValue) {
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    @Override
    public String getDisplayName() {
        return "Migrate `" + OLD_PROPERTY_KEY + "=" + oldValue + "` in a YAML document";
    }

    @Override
    public String getDescription() {
        return "Rewrites `" + OLD_PROPERTY_KEY + ": " + oldValue + "` to `" + NEW_PROPERTY_KEY + ": " + newValue +
               "`, checked document by document so a multi-document file cannot rewrite a document whose own " +
               "value never matched.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new YamlIsoVisitor<ExecutionContext>() {
            @Override
            public Yaml.Document visitDocument(Yaml.Document document, ExecutionContext ctx) {
                Yaml.Document d = super.visitDocument(document, ctx);
                if (!holdsOldValue(d)) {
                    return d;
                }
                // Fresh visitors per document, as in DropRedundantYamlProperty: each recipe's own visitor
                // expects to start at the tree it is asked to change, and this document is that tree here, not
                // the file the document lives in.
                Yaml.Document withValueChanged = (Yaml.Document) new ChangePropertyValue(
                        OLD_PROPERTY_KEY, newValue, oldValue, null, null, null).getVisitor().visit(d, ctx);
                return (Yaml.Document) new ChangePropertyKey(
                        OLD_PROPERTY_KEY, NEW_PROPERTY_KEY, null, null, null).getVisitor().visit(withValueChanged, ctx);
            }

            private boolean holdsOldValue(Yaml.Document d) {
                for (Yaml.Block value : FindProperty.find(d, OLD_PROPERTY_KEY, null)) {
                    if (value instanceof Yaml.Scalar && ((Yaml.Scalar) value).getValue().equals(oldValue)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
