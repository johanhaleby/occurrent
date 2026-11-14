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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A handler method a scan found, with the signature {@link RegisteredHandlers} keys it by and every handler of that
 * signature the same scan found. The signature is the method name and parameter types, as in 0.34.0, so every scan of
 * the method gives it the same signature. A private or package-private method and a method of the same signature that
 * a subclass declares don't override each other, so they are two handlers, and a scan of the subclass finds both.
 * Internal to the Spring Boot starters, not an API.
 *
 * @param method        the method {@link HandlerScan#handlerMethods} returned
 * @param signature     the part of the registration key that identifies the handler within its bean
 * @param sameSignature every method of {@code signature} the scan returned, {@code method} among them
 */
public record HandlerMethod(Method method, String signature, Set<Method> sameSignature) {

    /**
     * @param handlers every method the scan returned, from {@link HandlerScan#handlerMethods}
     * @param method   one of {@code handlers}
     * @return {@code method} with its signature and the handlers of that signature in {@code handlers}
     */
    public static HandlerMethod in(Set<Method> handlers, Method method) {
        String signature = signatureOf(method);
        Set<Method> sameSignature = handlers.stream()
                .filter(handler -> signatureOf(handler).equals(signature))
                .collect(Collectors.toUnmodifiableSet());
        return new HandlerMethod(method, signature, sameSignature);
    }

    private static String signatureOf(Method method) {
        return method.getName() + Arrays.toString(method.getParameterTypes());
    }
}
