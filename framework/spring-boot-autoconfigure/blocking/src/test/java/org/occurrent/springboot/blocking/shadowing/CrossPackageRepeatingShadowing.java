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

package org.occurrent.springboot.blocking.shadowing;

import org.occurrent.annotation.Subscription;
import org.occurrent.springboot.blocking.other.CrossPackageEvent;

import java.util.List;

/**
 * Overrides the shadowing method of {@link CrossPackageShadowing} from its own package and repeats the annotation
 * on it, so the scan accepts this class and finds both the repeated handler and the one it shadows.
 */
public class CrossPackageRepeatingShadowing extends CrossPackageShadowing {

    public CrossPackageRepeatingShadowing(List<String> bodies) {
        super(bodies);
    }

    @Subscription(id = "repeating-handler")
    @Override
    void on(CrossPackageEvent event) {
        bodies.add("repeating-body");
    }
}
