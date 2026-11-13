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

package org.occurrent.eventstore.mongodb.spring.blocking;

import org.jspecify.annotations.Nullable;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A copy of a {@link TransactionTemplate} whose transactions have labels. {@code MongoTransactionManager} reads a
 * transaction's labels only from a {@link TransactionAttribute}, and the template passes itself as the definition, so
 * the copy is both.
 */
final class LabelledTransactionTemplate extends TransactionTemplate implements TransactionAttribute {

    private final List<String> labels;

    LabelledTransactionTemplate(TransactionTemplate template, Collection<String> labels) {
        super(requireNonNull(template.getTransactionManager(), "The transaction template has no transaction manager"), template);
        this.labels = List.copyOf(labels);
    }

    @Override
    public @Nullable String getQualifier() {
        return null;
    }

    @Override
    public Collection<String> getLabels() {
        return labels;
    }

    // TransactionTemplate rolls back on any RuntimeException or Error itself, so this only answers the interface
    @Override
    public boolean rollbackOn(Throwable ex) {
        return ex instanceof RuntimeException || ex instanceof Error;
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof LabelledTransactionTemplate that && super.equals(that) && labels.equals(that.labels);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + labels.hashCode();
    }
}
