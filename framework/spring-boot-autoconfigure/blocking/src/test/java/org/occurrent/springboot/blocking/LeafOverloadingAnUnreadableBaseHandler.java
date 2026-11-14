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

/**
 * Declares an annotated overload of the handler of its package-private superclass. javac adds the visibility bridge
 * for the superclass handler, and copies the handler annotation to it, while the superclass can't be read.
 */
public class LeafOverloadingAnUnreadableBaseHandler extends UnreadablePackagePrivateBaseWithBaseEventHandler {
    public sealed interface BaseEvent permits SpecificEvent {
    }

    public record SpecificEvent() implements BaseEvent {
    }

    @Subscription(id = "overload-handler")
    public void on(SpecificEvent event) {
    }
}

class UnreadablePackagePrivateBaseWithBaseEventHandler {
    @Subscription(id = "base-handler")
    public void on(LeafOverloadingAnUnreadableBaseHandler.BaseEvent event) {
    }

    @SuppressWarnings("unused")
    void uses(UnreadableHierarchyRegistrationTest.AbsentType absent) {
    }
}
