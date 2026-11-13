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

package org.occurrent.eventstore.mongodb.spring.reactor;

import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.reactive.TransactionalOperator;

import java.util.Collection;

import static java.util.Objects.requireNonNull;

/**
 * Transactional operators whose transactions have labels. {@code ReactiveMongoTransactionManager} reads a
 * transaction's labels only from a {@code TransactionAttribute}, so an operator built from a plain definition can't be
 * given them afterwards.
 */
final class LabelledTransactions {

    private LabelledTransactions() {
    }

    /**
     * An operator on {@code manager} with the default definition and {@code labels}.
     */
    static TransactionalOperator operator(ReactiveTransactionManager manager, Collection<String> labels) {
        DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
        attribute.setLabels(labels);
        return TransactionalOperator.create(requireNonNull(manager, ReactiveTransactionManager.class.getSimpleName() + " cannot be null"), attribute);
    }
}
