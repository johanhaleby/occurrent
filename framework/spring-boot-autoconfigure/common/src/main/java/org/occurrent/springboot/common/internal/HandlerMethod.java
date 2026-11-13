/*
 *
 *  Copyright 2024 Johan Haleby
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.occurrent.springboot.common.internal;

import org.occurrent.springboot.common.SubscriptionAnnotations;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;

/**
 * A handler method a scan found, with the signature {@link RegisteredHandlers} keys it by. The signature is the method
 * name and parameter types, so two classes a non-singleton bean's factory returns, each declaring its own handler of
 * one signature, register that handler once. The declaring class is added for a private or package-private method
 * a superclass of the scanned class declares, so a private method of the same signature on the scanned class, which
 * doesn't override it, registers as a separate handler. Internal to the Spring Boot starters, not an API.
 *
 * @param method    the method {@link SubscriptionAnnotations#handlerMethods} returned
 * @param signature the part of the registration key that identifies the handler within its bean
 */
public record HandlerMethod(Method method, String signature) {

    /**
     * @param scannedClass the class the scan read, the one passed to {@link SubscriptionAnnotations#handlerMethods}
     * @param method       a method that scan returned
     * @return {@code method} with its signature
     */
    public static HandlerMethod in(Class<?> scannedClass, Method method) {
        int modifiers = method.getModifiers();
        boolean overridable = Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
        // A private or package-private annotated method a varying-class prototype's B inherits from a superclass
        // gets a different key than A's own method, so B registers its id again and the registration throws
        String owner = overridable || method.getDeclaringClass().equals(scannedClass) ? "" : method.getDeclaringClass().getName() + '#';
        return new HandlerMethod(method, owner + method.getName() + Arrays.toString(method.getParameterTypes()));
    }
}
