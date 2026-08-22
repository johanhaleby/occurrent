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
 * The true half of {@code org.occurrent.MigrateSubscriptionEnabledTrueInYaml_0_32} in
 * {@code subscription-mode-0_32.yml}, scoped to the YAML document it rewrites rather than the whole file.
 */
public class MigrateSubscriptionEnabledTrueInYaml extends MigrateSubscriptionEnabledInYaml {

    public MigrateSubscriptionEnabledTrueInYaml() {
        super("true", "auto");
    }
}
