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

/**
 * A handler method a scan found, with the signature {@link RegisteredHandlers} keys it by. The signature is the method
 * name and parameter types, as in 0.34.0, so every scan of the method gives it the same signature. Internal to the
 * Spring Boot starters, not an API.
 *
 * @param method the method {@link HandlerScan#handlerMethods} returned
 */
public record HandlerMethod(Method method) {

    /**
     * @return the part of the registration key that identifies the handler within its bean
     */
    public String signature() {
        return method.getName() + Arrays.toString(method.getParameterTypes());
    }
}
