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

public class LeafOverUnreadableBaseWithPackagePrivateOverload extends UnreadableBaseWithPackagePrivateOverload {
}

// javac adds a visibility bridge for on(TestEvent), and none for the package-private on(Integer)
class UnreadableBaseWithPackagePrivateOverload {
    @Subscription(id = "visibility-bridge-handler")
    public void on(UnreadableHierarchyRegistrationTest.TestEvent event) {
    }

    @Subscription(id = "package-private-handler")
    void on(Integer event) {
    }

    @SuppressWarnings("unused")
    void uses(UnreadableHierarchyRegistrationTest.AbsentType absent) {
    }
}
