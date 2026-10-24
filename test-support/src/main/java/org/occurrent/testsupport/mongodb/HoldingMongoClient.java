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

import com.mongodb.client.MongoClient;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

import static java.util.Objects.requireNonNull;

/**
 * A {@link MongoClient} that runs a hook before it starts a session, so a test can park an append of the native
 * {@code MongoEventStore} after it has reserved its position and before it begins its transaction.
 * <p>
 * The store calls {@code startSession} once per write attempt, after it has reserved the position block. A hook that
 * blocks on the first call therefore holds the write in the window where its position is assigned and nothing is
 * committed.
 */
public final class HoldingMongoClient {

    private HoldingMongoClient() {
    }

    /**
     * Wraps {@code delegate} so that {@code hook} runs on the calling thread before every {@code startSession}. The
     * hook decides whether to block, and a hook that returns lets the call through.
     */
    public static MongoClient beforeEachSession(MongoClient delegate, Runnable hook) {
        requireNonNull(delegate, "delegate cannot be null");
        requireNonNull(hook, "hook cannot be null");
        return (MongoClient) Proxy.newProxyInstance(MongoClient.class.getClassLoader(), new Class<?>[]{MongoClient.class}, (proxy, method, args) -> {
            if (method.getName().equals("startSession")) {
                hook.run();
            }
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }
}
