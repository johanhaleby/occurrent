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
package org.occurrent.rewrite;

/**
 * The YAML half of {@code DropRedundantSubscriptionEnabledInProperties_0_32} in {@code subscription-mode-0_32.yml},
 * dropping {@code occurrent.subscription.enabled} from a YAML document that already sets
 * {@code occurrent.subscription.mode}, scoped to the document that sets both rather than the whole file.
 */
public class DropRedundantSubscriptionEnabledInYaml_0_32 extends DropRedundantYamlProperty {

    public DropRedundantSubscriptionEnabledInYaml_0_32() {
        super("occurrent.subscription.enabled", "occurrent.subscription.mode");
    }
}
