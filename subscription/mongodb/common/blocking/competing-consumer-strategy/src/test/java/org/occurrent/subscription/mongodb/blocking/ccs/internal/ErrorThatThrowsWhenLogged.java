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

package org.occurrent.subscription.mongodb.blocking.ccs.internal;

// Reading its message throws, as logging it could when memory runs out. Neither is an OutOfMemoryError, since JUnit
// rethrows one from a failing assertion and that ends the whole run instead of failing the test.
class ErrorThatThrowsWhenLogged extends Error {
    @Override
    public String getMessage() {
        throw new Error("thrown while logging");
    }
}
