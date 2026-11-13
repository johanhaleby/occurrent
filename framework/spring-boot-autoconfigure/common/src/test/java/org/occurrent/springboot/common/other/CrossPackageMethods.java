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

package org.occurrent.springboot.common.other;

/**
 * Declared outside {@code org.occurrent.springboot.common}, so a subclass in that package inherits a package-private
 * method from another package, the one case a CGLIB proxy of the subclass cannot override.
 */
public abstract class CrossPackageMethods {

    String packagePrivate() {
        return "package-private-ran";
    }

    static String packagePrivateStatic() {
        return "package-private-static-ran";
    }

    public String publicMethod() {
        return "public-ran";
    }

    protected String protectedMethod() {
        return "protected-ran";
    }

    @SuppressWarnings("unused")
    private String privateMethod() {
        return "private-ran";
    }
}
