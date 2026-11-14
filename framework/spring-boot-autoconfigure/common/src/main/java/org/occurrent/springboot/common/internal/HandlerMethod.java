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
import java.util.Set;

/**
 * A handler method a scan found, with the signature {@link RegisteredHandlers} keys it by. The signature is the method
 * name and parameter types, so two classes a non-singleton bean's factory returns, each declaring its own handler of
 * one signature, register that handler once. The declaring class is added for a private or package-private method
 * when the scan also returned a handler of the same signature that a subclass of its declaring class declares. That
 * handler doesn't override it, so the two register as separate handlers. A method of the same signature that isn't a
 * handler adds nothing, so a class that inherits the handler keys it the way a scan of its declaring class does.
 * Internal to the Spring Boot starters, not an API.
 *
 * @param method    the method {@link SubscriptionAnnotations#handlerMethods} returned
 * @param signature the part of the registration key that identifies the handler within its bean
 */
public record HandlerMethod(Method method, String signature) {

    /**
     * @param handlers every method the scan returned, from {@link SubscriptionAnnotations#handlerMethods}
     * @param method   one of {@code handlers}
     * @return {@code method} with its signature
     */
    public static HandlerMethod in(Set<Method> handlers, Method method) {
        String owner = isShadowedIn(handlers, method) ? method.getDeclaringClass().getName() + '#' : "";
        return new HandlerMethod(method, owner + method.getName() + Arrays.toString(method.getParameterTypes()));
    }

    // A private or package-private method a varying-class prototype's B inherits from A keys the way A's scan keyed
    // it, so B skips the handler A registered. Only a handler of the same signature declared below A, which doesn't
    // override it, needs the owner to tell the two handlers apart
    private static boolean isShadowedIn(Set<Method> handlers, Method method) {
        int modifiers = method.getModifiers();
        Class<?> declaringClass = method.getDeclaringClass();
        if (Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers) || declaringClass.isInterface()) {
            return false;
        }
        for (Method handler : handlers) {
            if (handler.getDeclaringClass() != declaringClass && declaringClass.isAssignableFrom(handler.getDeclaringClass())
                    && handler.getName().equals(method.getName()) && Arrays.equals(handler.getParameterTypes(), method.getParameterTypes())) {
                return true;
            }
        }
        return false;
    }
}
