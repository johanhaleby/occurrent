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

package org.occurrent.testsupport.mongodb;

import org.springframework.transaction.reactive.TransactionCallback;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * A {@link TransactionalOperator} that runs a hook before it wraps a transaction, so a test can park an append of the
 * reactive {@code ReactorMongoEventStore} after it has reserved its position and before it begins its transaction.
 * <p>
 * The hook returns a {@code Mono} rather than blocking, so a held append does not occupy a thread. A hook that
 * returns {@code Mono.empty()} lets the transaction through. Needs Spring's transaction support and Reactor on the
 * classpath, which is why both are optional dependencies of this module.
 */
public final class HoldingTransactionalOperator implements TransactionalOperator {

    private final TransactionalOperator delegate;
    private final Supplier<Mono<Void>> hold;

    public HoldingTransactionalOperator(TransactionalOperator delegate, Supplier<Mono<Void>> hold) {
        this.delegate = requireNonNull(delegate, "delegate cannot be null");
        this.hold = requireNonNull(hold, "hold cannot be null");
    }

    @Override
    public <T> Mono<T> transactional(Mono<T> mono) {
        return Mono.defer(hold).then(delegate.transactional(mono));
    }

    @Override
    public <T> Flux<T> transactional(Flux<T> flux) {
        return Mono.defer(hold).thenMany(delegate.transactional(flux));
    }

    @Override
    public <T> Flux<T> execute(TransactionCallback<T> action) {
        return Mono.defer(hold).thenMany(delegate.execute(action));
    }
}
