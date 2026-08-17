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
import org.openrewrite.yaml.DeleteProperty;
import org.openrewrite.yaml.YamlIsoVisitor;
import org.openrewrite.yaml.search.FindProperty;
import org.openrewrite.yaml.tree.Yaml;

/**
 * Drops {@code oldPropertyKey} from a YAML document that already sets {@code newPropertyKey}, checking each
 * {@link Yaml.Document} in a multi-document file on its own rather than the file as a whole.
 * <p>
 * {@code org.openrewrite.yaml.search.FindProperty} used declaratively as a precondition, and
 * {@code org.openrewrite.yaml.DeleteProperty} as the guarded action, both operate on the {@code Yaml.Documents}
 * source file rather than the individual {@code Yaml.Document} within it. In a file with more than one document,
 * separated by {@code ---} for Spring profiles, that means a single document setting {@code newPropertyKey}
 * satisfies the precondition for the entire file, and the deletion then removes {@code oldPropertyKey} from
 * every document that has it, including one that never set {@code newPropertyKey} at all. This recipe re-runs
 * the same check per document instead, so a document is only touched when it sets both keys itself.
 * <p>
 * Not referenced directly from a declarative recipe list: it has no public no-arg constructor, and this module
 * neither compiles with {@code -parameters} nor depends on Lombok, so there is no way for the declarative loader
 * to bind an {@code oldPropertyKey}/{@code newPropertyKey} options map onto it by reflection the way it does for
 * a stock OpenRewrite recipe. Each property pair instead gets its own no-arg subclass that hardcodes the two
 * keys in its constructor, the same shape {@link RenameMappedSubscriptionEnabledKey} already uses for a recipe
 * with nothing to configure.
 */
public abstract class DropRedundantYamlProperty extends Recipe {

    private final String oldPropertyKey;
    private final String newPropertyKey;

    protected DropRedundantYamlProperty(String oldPropertyKey, String newPropertyKey) {
        this.oldPropertyKey = oldPropertyKey;
        this.newPropertyKey = newPropertyKey;
    }

    @Override
    public String getDisplayName() {
        return "Drop `" + oldPropertyKey + "` from a YAML document where `" + newPropertyKey + "` is already set";
    }

    @Override
    public String getDescription() {
        return "Removes `" + oldPropertyKey + "` from a YAML document that already sets `" + newPropertyKey + "`, " +
               "checked document by document so a multi-document file cannot lose the deprecated key from a " +
               "document that never set the replacement.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new YamlIsoVisitor<ExecutionContext>() {
            @Override
            public Yaml.Document visitDocument(Yaml.Document document, ExecutionContext ctx) {
                Yaml.Document d = super.visitDocument(document, ctx);
                if (FindProperty.find(d, newPropertyKey, null).isEmpty()) {
                    return d;
                }
                // A fresh visitor per document: DeleteProperty's own visitor expects to start at the tree it is
                // asked to delete from, and this document is that tree here, not the file the document lives in.
                return (Yaml.Document) new DeleteProperty(oldPropertyKey, null, null, null).getVisitor().visit(d, ctx);
            }
        };
    }
}
