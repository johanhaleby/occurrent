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
import org.openrewrite.properties.PropertiesVisitor;
import org.openrewrite.properties.tree.Properties;

import java.util.Set;

/**
 * Renames {@code occurrent.subscription.enabled} to {@code occurrent.subscription.mode} in a {@code .properties} file,
 * but only where the value is already one the new property accepts. The value mapping runs before this, so an entry
 * still holding something else is one the mapping could not translate, an unresolved placeholder for example. Renaming
 * that entry's key would produce {@code occurrent.subscription.mode} holding a boolean, which does not bind and fails
 * the application at startup. Leaving it alone keeps it on the deprecated property, which still works.
 * <p>
 * The yaml half of the same migration is {@link MigrateSubscriptionEnabledInYaml}, an imperative recipe rather than
 * a declarative one, because a value-matching precondition there needs to be checked against each {@code Yaml.Document}
 * of a multi-document file rather than the file as a whole.
 */
public class RenameMappedSubscriptionEnabledKey extends Recipe {

    private static final String DEPRECATED_KEY = "occurrent.subscription.enabled";
    private static final String NEW_KEY = "occurrent.subscription.mode";
    private static final Set<String> MAPPED_VALUES = Set.of("disabled", "manual", "auto");

    @Override
    public String getDisplayName() {
        return "Rename occurrent.subscription.enabled once its value has been mapped";
    }

    @Override
    public String getDescription() {
        return "Renames the deprecated occurrent.subscription.enabled to occurrent.subscription.mode in .properties " +
               "files, but only for an entry whose value is already one occurrent.subscription.mode accepts. An entry " +
               "holding anything else keeps the deprecated key, which still works, rather than gaining a new key it " +
               "cannot bind.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
        return new PropertiesVisitor<>() {
            @Override
            public Properties visitEntry(Properties.Entry entry, ExecutionContext ctx) {
                if (DEPRECATED_KEY.equals(entry.getKey()) && MAPPED_VALUES.contains(entry.getValue().getText())) {
                    entry = entry.withKey(NEW_KEY).withPrefix(entry.getPrefix());
                }
                return super.visitEntry(entry, ctx);
            }
        };
    }
}
