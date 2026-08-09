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

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;
import org.openrewrite.test.TypeValidation;

import static org.openrewrite.kotlin.Assertions.kotlin;

/**
 * Proves the recipe is Java only: rewrite-kotlin represents an ordinary Kotlin class with the same
 * {@code J.ClassDeclaration} the Java LST uses, so a type check alone would also match a Kotlin implementer, and
 * inserting the Java-syntax stub there would be wrong for the file. A Kotlin implementer needs the manual steps in
 * doc/migration/upgrading-to-0.33.0.md instead.
 */
class CheckpointStorageConditionalWriteStubsKotlinTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/checkpoint-storage-stubs-0_33.yml",
                "org.occurrent.MigrateCheckpointStorageConditionalWrite_0_33")
                // Type attribution on stubbed types is looser under the Kotlin parser than the real compiled types;
                // the printed source is what matters for a real run, so relax kind validation here.
                .typeValidationOptions(TypeValidation.builder().identifiers(false).methodInvocations(false).build());
    }

    @Test
    void leavesAKotlinImplementerMissingBothMembersUntouched() {
        rewriteRun(
                kotlin(
                        """
                        package org.occurrent.subscription
                        interface Checkpoint
                        """
                ),
                kotlin(
                        """
                        package org.occurrent.subscription
                        interface CheckpointWriteCondition
                        """
                ),
                kotlin(
                        """
                        package org.occurrent.subscription.api.blocking

                        import org.occurrent.subscription.Checkpoint
                        import org.occurrent.subscription.CheckpointWriteCondition
                        import java.util.OptionalLong

                        interface CheckpointStorage {
                            fun read(subscriptionId: String): Checkpoint?
                            fun save(subscriptionId: String, checkpoint: Checkpoint, condition: CheckpointWriteCondition): Checkpoint
                            fun writeVersion(subscriptionId: String): OptionalLong
                            fun delete(subscriptionId: String)
                            fun exists(subscriptionId: String): Boolean
                        }
                        """
                ),
                // No change expected: the recipe only ever inserts Java syntax, so it leaves a Kotlin file alone
                // even though this implementer is missing both members.
                kotlin(
                        """
                        package com.example

                        import org.occurrent.subscription.Checkpoint
                        import org.occurrent.subscription.api.blocking.CheckpointStorage

                        class InMemoryCheckpointStorage : CheckpointStorage {
                            override fun read(subscriptionId: String): Checkpoint? = null
                            override fun delete(subscriptionId: String) {
                            }
                            override fun exists(subscriptionId: String): Boolean = false
                        }
                        """
                )
        );
    }
}
