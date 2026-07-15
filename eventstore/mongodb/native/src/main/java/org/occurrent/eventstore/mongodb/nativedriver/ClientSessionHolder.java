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

package org.occurrent.eventstore.mongodb.nativedriver;

import com.mongodb.client.ClientSession;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Thread-bound holder for an externally managed MongoDB {@link ClientSession}.
 * <p>
 * A native (non-Spring) transaction executor binds the {@link ClientSession} whose transaction it opened to the
 * current thread here, before running the unit of work, and {@link #remove() unbinds} it afterwards. While a session
 * is bound, {@link MongoEventStore}'s write and DCB append join that session instead of opening one of their own, so
 * the event write and any synchronous subscription handlers commit atomically within the single transaction the
 * executor controls.
 * <p>
 * The holder lives in the native event-store module (not in the executor module) so that the store can consult it
 * without depending on the executor, avoiding a module dependency cycle. When no session is bound, the store behaves
 * exactly as before, managing its own session per write/append.
 */
@NullMarked
public final class ClientSessionHolder {

    private static final ThreadLocal<@Nullable ClientSession> CURRENT_SESSION = new ThreadLocal<>();

    private ClientSessionHolder() {
    }

    /**
     * @return The {@link ClientSession} bound to the current thread, or {@code null} when none is bound.
     */
    public static @Nullable ClientSession get() {
        return CURRENT_SESSION.get();
    }

    /**
     * Bind a {@link ClientSession} to the current thread. Its active transaction is joined by subsequent
     * {@link MongoEventStore} writes/appends on this thread until {@link #remove()} is called.
     *
     * @param clientSession The session to bind. Must not be {@code null}.
     */
    public static void set(ClientSession clientSession) {
        CURRENT_SESSION.set(Objects.requireNonNull(clientSession, "clientSession cannot be null"));
    }

    /**
     * Unbind any {@link ClientSession} from the current thread. Safe to call when nothing is bound.
     */
    public static void remove() {
        CURRENT_SESSION.remove();
    }
}
