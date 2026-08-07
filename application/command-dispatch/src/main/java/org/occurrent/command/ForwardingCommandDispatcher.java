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

package org.occurrent.command;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A base for a {@link CommandDispatcher} decorator that forwards both {@link #dispatch} and {@link #dispatchAll} to a
 * delegate, so overriding one does not silently drop the other.
 * <p>
 * {@code CommandDispatcher} is a {@code @FunctionalInterface} with one abstract method, {@code dispatch}, and a default
 * {@code dispatchAll} that loops over it. A decorator written the natural way, {@code (C command) -> { ...; delegate.dispatch(command); }},
 * overrides only {@code dispatch}. It then inherits the interface's own default {@code dispatchAll}, which loops over
 * <em>this</em> object's {@code dispatch} rather than forwarding to {@code delegate.dispatchAll}. If the delegate
 * overrides {@code dispatchAll} for batch atomicity (see the interface's javadoc), that override is silently bypassed:
 * every batch call through the decorator goes back to one append per command, reintroducing the partial-progress hazard
 * ADR 76 removed. Extending this class instead of implementing {@code CommandDispatcher} directly forwards both methods,
 * so overriding just one leaves the other correctly delegated.
 *
 * @param <C> the command type
 */
public abstract class ForwardingCommandDispatcher<C> implements CommandDispatcher<C> {

    protected final CommandDispatcher<C> delegate;

    protected ForwardingCommandDispatcher(CommandDispatcher<C> delegate) {
        this.delegate = requireNonNull(delegate, "delegate cannot be null");
    }

    @Override
    public void dispatch(C command) {
        delegate.dispatch(command);
    }

    @Override
    public void dispatchAll(List<C> commands) {
        delegate.dispatchAll(commands);
    }
}
