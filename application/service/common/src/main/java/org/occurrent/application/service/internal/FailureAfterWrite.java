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

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps the application service's retry from running the command again for a failure after a write when the attempt
 * didn't open a transaction.
 * <p>
 * One instance per execute. The failure stays here, and only a stand-in exception passes through the transaction executor,
 * so an executor that opens a transaction without reporting it still rolls back. The service rethrows the failure once the
 * retry has returned. It asks {@link #isHeld()} rather than matching on a type, so neither a retry predicate that matches
 * every {@link RuntimeException} nor an executor that translates or retries what passes through it can run the command again.
 */
public final class FailureAfterWrite {

    private volatile @Nullable StandIn held;
    private volatile @Nullable Throwable keptBeforeWrite;

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

    /**
     * Whether a failure is held. The action of a transaction executor asks this first and returns an empty attempt when it
     * is, so an executor that runs the action again after the stand-in can't write a second time, and a retry of its own
     * that runs until the action returns stops there.
     */
    public boolean isHeld() {
        return held != null;
    }

    /**
     * Whether a failure is held once the executor let {@code thrown} out. When {@code thrown} isn't the stand-in or a wrapper
     * of it, for example a failed rollback, it stays with the failure as a suppressed exception. When it's a wrapper, what
     * the executor attached to the wrapper stays with the failure the same way.
     */
    public boolean isHeldAfter(Throwable thrown) {
        StandIn standIn = held;
        if (standIn == null) {
            return false;
        }
        if (thrown == standIn.getCause()) {
            // The held failure itself, which the attempt throws again when the executor returned
            return true;
        }
        List<Throwable> wrappers = new ArrayList<>();
        Throwable cause = thrown;
        // At most 32 causes, since a chain of causes can be a cycle
        for (int depth = 0; cause != null && cause != standIn && depth < 32; depth++) {
            wrappers.add(cause);
            cause = cause.getCause();
        }
        if (cause != standIn) {
            standIn.addSuppressed(thrown);
            return true;
        }
        for (Throwable wrapper : wrappers) {
            for (Throwable suppressed : wrapper.getSuppressed()) {
                if (suppressed != standIn) {
                    standIn.addSuppressed(suppressed);
                }
            }
        }
        return true;
    }

    /**
     * Keeps a failure of the read or the decision, which goes around the executor so it can't mark a transaction the caller
     * owns rollback-only. {@code null} clears it, since an executor can run the action again.
     */
    public void keepBeforeWrite(@Nullable Throwable failure) {
        keptBeforeWrite = failure;
    }

    /**
     * What the attempt throws once the executor let {@code thrown} out, or returned when {@code thrown} is {@code null}. A
     * held failure comes first, also when an executor swallowed the stand-in and returned. Then a kept one, which takes
     * {@code thrown} as a suppressed exception, the way a try-with-resources keeps the body's failure over the close's.
     * Without either it's {@code thrown}, or nothing when the executor returned.
     */
    public @Nullable Throwable failureOfAttempt(@Nullable Throwable thrown) {
        Throwable kept = keptBeforeWrite;
        keptBeforeWrite = null;
        if (thrown == null ? isHeld() : isHeldAfter(thrown)) {
            return failure();
        }
        if (kept == null) {
            return thrown;
        }
        if (thrown != null && thrown != kept) {
            kept.addSuppressed(thrown);
        }
        return kept;
    }

    /**
     * Returns {@code attempt}, which the executor returned, unless the attempt has a failure to throw. That covers a failure
     * held while an executor swallowed the stand-in, so it still reaches the retry strategy.
     */
    public <T> T attemptOrFailure(T attempt) {
        Throwable failure = failureOfAttempt(null);
        if (failure != null) {
            throw FailureAfterWrite.<RuntimeException>sneakyThrow(failure);
        }
        return attempt;
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
        // Asked again when a strategy that ignores the shutdown predicate returns, so an exception is attached only once
        List<Throwable> alreadySuppressed = List.of(failure.getSuppressed());
        for (Throwable suppressed : standIn.getSuppressed()) {
            if (suppressed != failure && !alreadySuppressed.contains(suppressed)) {
                failure.addSuppressed(suppressed);
            }
        }
        return failure;
    }

    /**
     * Throws {@code failure} as it is, checked or not.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Throwable> T sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }

    private static final class StandIn extends RuntimeException {
        private StandIn(Throwable failure) {
            super(null, failure, true, false);
        }
    }
}
