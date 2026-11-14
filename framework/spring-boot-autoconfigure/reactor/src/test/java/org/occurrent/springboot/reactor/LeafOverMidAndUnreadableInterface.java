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

package org.occurrent.springboot.reactor;

import org.occurrent.annotation.StartupMode;
import org.occurrent.annotation.Subscription;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Declares nothing itself. javac adds a visibility bridge that runs the handler of the package-private superclass and
 * copies its annotation, while the default handler of the interface of the same signature is the method the bridge
 * resolves to. The interface can't be read.
 */
public class LeafOverMidAndUnreadableInterface extends PackagePrivateMidWithHandler implements UnreadableInterfaceWithDefaultHandler {
    public static final List<String> RAN = new CopyOnWriteArrayList<>();
}

class PackagePrivateMidWithHandler {
    @Subscription(id = "mid", startupMode = StartupMode.BACKGROUND)
    public Mono<Void> on(UnreadableHierarchyRegistrationTest.TestEvent event) {
        LeafOverMidAndUnreadableInterface.RAN.add("mid-body");
        return Mono.empty();
    }
}

interface UnreadableInterfaceWithDefaultHandler {
    @SuppressWarnings("unused")
    private void uses(UnreadableHierarchyRegistrationTest.AbsentType absent) {
    }

    @Subscription(id = "iface", startupMode = StartupMode.BACKGROUND)
    default Mono<Void> on(UnreadableHierarchyRegistrationTest.TestEvent event) {
        LeafOverMidAndUnreadableInterface.RAN.add("iface-body");
        return Mono.empty();
    }
}
