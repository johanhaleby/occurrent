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

package org.occurrent.springboot.common;

/**
 * A store starter's contribution to the workaround for
 * <a href="https://github.com/spring-projects/spring-framework/issues/32904">spring-framework#32904</a>: annotation
 * registration runs from {@code afterSingletonsInstantiated}, and a bean first created from there can be missed, so
 * each starter forces its own affected beans into existence before a subscription is started. Every
 * {@code StartupWorkaround} bean is applied, in no particular order.
 */
@FunctionalInterface
public interface StartupWorkaround {

    void apply();
}
