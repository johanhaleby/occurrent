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

package org.occurrent.dsl.saga.blocking;

/**
 * How the executor issues a command a saga produced. The common case is a lambda over an {@code ApplicationService}, with
 * or without a decider, for example {@code cmd -> applicationService.execute(cmd.orderId(), events -> Order.cancel(events,
 * cmd))}; see {@link CommandDispatchers#decider} for a decider-backed convenience.
 * <p>
 * Command dispatch is at-least-once: the same command may be dispatched more than once (a crash between dispatch and the
 * state save, or a compare-and-set retry). A dispatcher should therefore be idempotent, which an
 * {@code ApplicationService}-backed one is by construction, since it re-folds the authoritative stream and the target's
 * invariants reject a stale or already-applied command.
 *
 * @param <C> the command type
 */
@FunctionalInterface
public interface CommandDispatcher<C> {

    /** Issue {@code command}. May be called more than once for the same logical command; must be idempotent. */
    void dispatch(C command);
}
