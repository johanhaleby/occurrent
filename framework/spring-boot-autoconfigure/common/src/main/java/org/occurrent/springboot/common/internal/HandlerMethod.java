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
 * a superclass of the scanned class declares when the scanned class, or a class between the two, declares a method of
 * the same signature. That method doesn't override it, so the two register as separate handlers. Without such a
 * method, a class that inherits the handler keys it the way a scan of its declaring class does. Internal to the Spring
 * Boot starters, not an API.
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
        String owner = isShadowedIn(scannedClass, method) ? method.getDeclaringClass().getName() + '#' : "";
        return new HandlerMethod(method, owner + method.getName() + Arrays.toString(method.getParameterTypes()));
    }

    // A private or package-private method a varying-class prototype's B inherits from A keys the way A's scan keyed
    // it, so B skips the handler A registered. Only a method of the same signature on B, or on a class between B and
    // A, which doesn't override it, needs the owner to tell the two handlers apart
    private static boolean isShadowedIn(Class<?> scannedClass, Method method) {
        int modifiers = method.getModifiers();
        Class<?> declaringClass = method.getDeclaringClass();
        if (Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers) || declaringClass.isInterface() || !declaringClass.isAssignableFrom(scannedClass)) {
            return false;
        }
        for (Class<?> candidateClass = scannedClass; candidateClass != declaringClass; candidateClass = candidateClass.getSuperclass()) {
            try {
                candidateClass.getDeclaredMethod(method.getName(), method.getParameterTypes());
                return true;
            } catch (NoSuchMethodException e) {
                // no method of that signature on this class
            }
        }
        return false;
    }
}
