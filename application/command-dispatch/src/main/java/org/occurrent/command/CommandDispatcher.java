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

/**
 * The producer-facing port for issuing a command. A command producer, such as a saga or a policy, hands a command to a
 * dispatcher and stays ignorant of the write mechanics. The dispatcher owns the routing, deciding which stream the
 * command targets (see {@link StreamIdResolver}) and which decider or handler applies, and calls the write engine (an
 * {@code ApplicationService}). It sits one layer above the application service, which takes an already-resolved stream
 * id and a decider or function. The common case is a lambda over an {@code ApplicationService}, for example
 * {@code cmd -> applicationService.execute(cmd.orderId(), events -> Order.cancel(events, cmd))}.
 * <p>
 * Command dispatch is at-least-once: the same command may be dispatched more than once (a crash between dispatch and a
 * state save, or a compare-and-set retry). A dispatcher should therefore be idempotent, which an
 * {@code ApplicationService}-backed one is by construction, since it re-folds the authoritative stream and the target's
 * invariants reject a stale or already-applied command.
 * <p>
 * {@link #dispatchAll} is a seam a dispatcher may override for batch atomicity, see its javadoc.
 *
 * @param <C> the command type
 */
@FunctionalInterface
public interface CommandDispatcher<C> {

    /** Issue {@code command}. May be called more than once for the same logical command; must be idempotent. */
    void dispatch(C command);

    /**
     * Issue {@code commands} as a unit from the caller's point of view. The default forwards each command to
     * {@link #dispatch} in order, so a plain lambda dispatcher gets this behaviour for free and a failure partway
     * through still leaves the earlier commands dispatched. Override this to make the batch atomic, for example one
     * transaction covering every command, when that is possible for the dispatcher's target.
     * <p>
     * <strong>Decorator hazard:</strong> a decorator that overrides only {@code dispatch} and delegates inherits
     * this default rather than the delegate's own {@code dispatchAll}, so it silently turns a delegate's atomic batch
     * back into one call to {@code dispatch} per command, restoring the partial-progress hazard an overridden
     * {@code dispatchAll} exists to remove. Extend {@link ForwardingCommandDispatcher} instead of implementing this
     * interface directly to forward both methods.
     */
    default void dispatchAll(List<C> commands) {
        for (C command : commands) {
            dispatch(command);
        }
    }
}
