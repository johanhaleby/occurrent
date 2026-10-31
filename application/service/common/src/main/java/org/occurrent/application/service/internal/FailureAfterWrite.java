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

package org.occurrent.application.service.internal;

import org.jspecify.annotations.Nullable;

/**
 * Keeps a failure after a write away from the application service's retry when the attempt didn't open a transaction.
 * <p>
 * One instance per execute. The failure stays here, and only a stand-in exception passes through the transaction executor,
 * so an executor that opens a transaction without reporting it still rolls back. The service rethrows the failure once the
 * retry has returned. It asks {@link #isHeld()} rather than matching on a type, so neither a retry predicate that matches
 * every {@link RuntimeException} nor an executor that translates or retries what passes through it can run the command again.
 */
public final class FailureAfterWrite {

    private volatile @Nullable StandIn held;

    /**
     * Throws {@code failure} as it is when the service takes this attempt to have opened a transaction, so the retry may run the command again. Otherwise holds it and returns the stand-in for the caller to throw through the executor.
     */
    public RuntimeException propagate(Throwable failure, boolean attemptOpenedTransaction) {
        if (attemptOpenedTransaction) {
            throw FailureAfterWrite.<RuntimeException>sneakyThrow(failure);
        }
        return hold(failure);
    }

    /**
     * Holds {@code failure}.
     *
     * @return the stand-in for the caller to throw through the executor
     */
    public RuntimeException hold(Throwable failure) {
        StandIn standIn = new StandIn(failure);
        held = standIn;
        return standIn;
    }

    public boolean isHeld() {
        return held != null;
    }

    /**
     * Whether a failure is held once the executor let {@code thrown} out. When {@code thrown} isn't the stand-in or a wrapper
     * of it, for example a failed rollback, it stays with the failure as a suppressed exception.
     */
    public boolean isHeldAfter(Throwable thrown) {
        StandIn standIn = held;
        if (standIn == null) {
            return false;
        }
        if (!isCausedBy(thrown, standIn)) {
            standIn.addSuppressed(thrown);
        }
        return true;
    }

    private static boolean isCausedBy(Throwable thrown, StandIn standIn) {
        // Bounded, since a chain of causes can be a cycle
        Throwable cause = thrown;
        for (int depth = 0; cause != null && depth < 32; depth++) {
            if (cause == standIn) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    /**
     * Throws the stand-in if a failure is held. The action of a transaction executor calls this first, so an executor that
     * retries what passes through it can't write a second time.
     */
    public void refuseIfHeld() {
        StandIn standIn = held;
        if (standIn != null) {
            throw standIn;
        }
    }

    /**
     * Throws the held failure, checked or not, if there is one.
     */
    public void rethrowIfHeld() {
        if (isHeld()) {
            throw FailureAfterWrite.<RuntimeException>sneakyThrow(failure());
        }
    }

    /**
     * The held failure, with what an executor attached to the stand-in, for example a failed cleanup.
     *
     * @throws IllegalStateException if no failure is held
     */
    public Throwable failure() {
        StandIn standIn = held;
        if (standIn == null) {
            throw new IllegalStateException("No failure is held");
        }
        Throwable failure = standIn.getCause();
        for (Throwable suppressed : standIn.getSuppressed()) {
            if (suppressed != failure) {
                failure.addSuppressed(suppressed);
            }
        }
        return failure;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> T sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }

    private static final class StandIn extends RuntimeException {
        private StandIn(Throwable failure) {
            super(null, failure, true, false);
        }
    }
}
