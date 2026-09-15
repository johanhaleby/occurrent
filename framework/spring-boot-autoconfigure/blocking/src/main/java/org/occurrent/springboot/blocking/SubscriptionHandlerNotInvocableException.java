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

package org.occurrent.springboot.blocking;

import java.lang.reflect.Method;

/**
 * Thrown when a {@code @Subscription}, {@code @StreamSubscription}, {@code @DcbSubscription} or
 * {@code @SynchronousSubscription} handler method cannot be invoked through the bean's Spring proxy. A JDK interface
 * proxy that does not implement the method reaches it, so does a private method, which a CGLIB proxy can never
 * override either, and so does a final method on a CGLIB proxy specifically, since an unproxied bean invokes a
 * final method directly with no proxy to lose.
 * <p>
 * Invoking the method on the raw bean instead would run it with no advice applied, including
 * {@code @Transactional}, on every delivery for as long as the application runs. Refused rather than done silently.
 * <p>
 * Also thrown when the bean the handler would be invoked on is not the implementation the method was read from, a
 * prototype whose factory returned an unrelated class or a subclass overriding the method. Invoking it would run that
 * other class's code under this method's subscription id.
 */
public final class SubscriptionHandlerNotInvocableException extends IllegalStateException {

    SubscriptionHandlerNotInvocableException(Method method, String reason) {
        super("Cannot invoke %s.%s through its Spring proxy, so its advice would never apply. %s"
                .formatted(method.getDeclaringClass().getName(), method.getName(), reason));
    }

    private SubscriptionHandlerNotInvocableException(String message) {
        super(message);
    }

    static SubscriptionHandlerNotInvocableException notTheImplementationOf(Method method, Class<?> implementation) {
        return new SubscriptionHandlerNotInvocableException(("Cannot invoke %s.%s on a %s, because that class does not run the implementation the handler was read from. " +
                "The bean's factory built a different class than the one Occurrent read the handler from, either an unrelated class or a subclass overriding the method. Return the same implementation on every call.")
                .formatted(method.getDeclaringClass().getName(), method.getName(), implementation.getName()));
    }
}
