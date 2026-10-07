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

package org.occurrent.subscription.mongodb.spring.reactor;

import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.ResettableClassFileTransformer;
import net.bytebuddy.asm.Advice;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * Breaks the token read of the driver's change stream cursor from the outside, since the driver offers no way to make
 * one fail. The advice is woven into the driver's cursor, which only the subscription model calls
 * {@code getPostBatchResumeToken} on. The driver reads the token through the cursor inside it, so it keeps working.
 * <p>
 * The class and its members are public because the advice runs inside the driver's package.
 */
public final class DriverCursorFaults {
    private static final String CHANGE_STREAM_CURSOR = "com.mongodb.internal.operation.AsyncChangeStreamBatchCursor";

    /**
     * While {@code true}, every read of the token through the driver's change stream cursor throws.
     */
    public static volatile boolean failTokenReads;

    /**
     * While {@code true}, every driver change stream cursor that is built is added to {@link #cursors}.
     */
    public static volatile boolean recordCursors;

    public static final List<Object> cursors = new CopyOnWriteArrayList<>();

    private static ResettableClassFileTransformer transformer;

    private DriverCursorFaults() {
    }

    public static synchronized void install() {
        if (transformer != null) {
            return;
        }
        transformer = new AgentBuilder.Default()
                .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
                .disableClassFormatChanges()
                .type(named(CHANGE_STREAM_CURSOR))
                .transform((builder, type, classLoader, module, protectionDomain) -> builder
                        .visit(Advice.to(TokenRead.class).on(named("getPostBatchResumeToken")))
                        .visit(Advice.to(Constructed.class).on(isConstructor())))
                .installOn(ByteBuddyAgent.install());
    }

    public static synchronized void uninstall() {
        if (transformer == null) {
            return;
        }
        transformer.reset(ByteBuddyAgent.getInstrumentation(), AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);
        transformer = null;
    }

    public static void reset() {
        failTokenReads = false;
        recordCursors = false;
        cursors.clear();
    }

    public static final class TokenRead {
        private TokenRead() {
        }

        @Advice.OnMethodEnter
        public static void enter() {
            if (DriverCursorFaults.failTokenReads) {
                throw new IllegalStateException("The token read was broken by DriverCursorFaults");
            }
        }
    }

    public static final class Constructed {
        private Constructed() {
        }

        @Advice.OnMethodExit
        public static void exit(@Advice.This Object cursor) {
            if (DriverCursorFaults.recordCursors) {
                DriverCursorFaults.cursors.add(cursor);
            }
        }
    }
}
