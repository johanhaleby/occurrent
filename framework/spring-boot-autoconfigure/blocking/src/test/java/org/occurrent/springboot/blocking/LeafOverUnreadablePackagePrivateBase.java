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

import org.occurrent.annotation.Subscription;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Declares nothing itself. javac adds the visibility bridge for the public handler of its package-private superclass,
 * and copies the handler annotation to it, while the superclass can't be read.
 */
public class LeafOverUnreadablePackagePrivateBase extends UnreadablePackagePrivateBase {
    public static final List<String> RAN = new CopyOnWriteArrayList<>();
}

class UnreadablePackagePrivateBase {
    @Subscription(id = "visibility-bridge-handler")
    public void on(UnreadableHierarchyRegistrationTest.TestEvent event) {
        LeafOverUnreadablePackagePrivateBase.RAN.add("visibility-bridge-body");
    }

    @SuppressWarnings("unused")
    void uses(UnreadableHierarchyRegistrationTest.AbsentType absent) {
    }
}
