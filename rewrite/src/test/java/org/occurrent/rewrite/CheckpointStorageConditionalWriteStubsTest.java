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

import static org.occurrent.rewrite.CheckpointStorageStubs.*;
import static org.openrewrite.java.Assertions.java;

class CheckpointStorageConditionalWriteStubsTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipeFromResource("/META-INF/rewrite/checkpoint-storage-stubs-0_33.yml",
                "org.occurrent.MigrateCheckpointStorageConditionalWrite_0_33");
    }

    @Test
    void stubsBothMissingMembersOnABlockingImplementer() {
        // A class that already implements CheckpointStorage was necessarily written against the 0.32.0 interface,
        // which had no three-argument save at all, so its two-argument save override is real pre-existing behaviour,
        // not something this fixture invents. The generated three-argument stub calls it for any(), which is what
        // proves the delegation binds to the class's own method rather than recursing into the interface default.
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        class InMemoryCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
                                return checkpoint;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        import java.util.OptionalLong;

                        class InMemoryCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
                                return checkpoint;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this only refuses a condition stronger than any(), delegating any() to the existing two-argument save. Evaluate `condition` for real if this storage can, otherwise this is the permanent answer. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                if (!(condition instanceof CheckpointWriteCondition.Any)) {
                                    throw new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported.");
                                }
                                return save(subscriptionId, checkpoint);
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this always answers empty, correct if this storage cannot evaluate a condition. Return the version a condition is judged against if it can. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void stubsBothMissingMembersOnAReactorImplementer() {
        // Same reasoning as the blocking case: the two-argument save is pre-existing 0.32.0 behaviour, and the
        // generated three-argument stub's any() branch calls it rather than the interface default.
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(MONO),
                java(REACTOR_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.api.reactor.CheckpointStorage;
                        import reactor.core.publisher.Mono;

                        class InMemoryReactorCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Mono<Checkpoint> read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
                                return Mono.just(checkpoint);
                            }

                            @Override
                            public Mono<Void> delete(String subscriptionId) {
                                return null;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.reactor.CheckpointStorage;
                        import reactor.core.publisher.Mono;

                        class InMemoryReactorCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Mono<Checkpoint> read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
                                return Mono.just(checkpoint);
                            }

                            @Override
                            public Mono<Void> delete(String subscriptionId) {
                                return null;
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this only refuses a condition stronger than any(), delegating any() to the existing two-argument save. Evaluate `condition` for real if this storage can, otherwise this is the permanent answer. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                if (!(condition instanceof CheckpointWriteCondition.Any)) {
                                    return Mono.error(new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported."));
                                }
                                return save(subscriptionId, checkpoint);
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this always answers an empty Mono, correct if this storage cannot evaluate a condition. Signal the version a condition is judged against if it can. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Mono<Long> writeVersion(String subscriptionId) {
                                return Mono.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void generatesAnAlwaysRefusingSaveOnABlockingImplementerWithNoOwnTwoArgumentSave() {
        // InMemoryCheckpointStorage declares no two-argument save of its own, so a call to save(subscriptionId,
        // checkpoint) resolves to CheckpointStorage's own default, which calls the three-argument save with any().
        // If the generated three-argument save delegated any() to save(subscriptionId, checkpoint) the way the
        // usual stub does, that call would land back on the interface default and recurse into this same stub
        // (StackOverflowError on the first checkpoint write, whatever a partial hand-migration left in place). The
        // always-refusing shape below never calls back into any three-argument save, so nothing can recurse.
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        class InMemoryCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        import java.util.OptionalLong;

                        class InMemoryCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this class has no own two-argument save, only the CheckpointStorage default, which calls this method for any(), so delegating any() here would recurse. This always refuses instead, even any(). Give the class its own two-argument save, or evaluate `condition` for real here. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                throw new UnsupportedOperationException("This storage cannot evaluate " + condition + ". It has no two-argument save to fall back on, so even any() is refused.");
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this always answers empty, correct if this storage cannot evaluate a condition. Return the version a condition is judged against if it can. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void generatesAnAlwaysRefusingSaveOnAReactorImplementerWithNoOwnTwoArgumentSave() {
        // Same reasoning as the blocking case above: no own two-argument save means save(subscriptionId,
        // checkpoint) resolves to CheckpointStorage's own default, which would recurse into a delegating
        // three-argument stub. The always-refusing shape below cannot recurse.
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(MONO),
                java(REACTOR_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.api.reactor.CheckpointStorage;
                        import reactor.core.publisher.Mono;

                        class InMemoryReactorCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Mono<Checkpoint> read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Mono<Void> delete(String subscriptionId) {
                                return null;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.reactor.CheckpointStorage;
                        import reactor.core.publisher.Mono;

                        class InMemoryReactorCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Mono<Checkpoint> read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Mono<Void> delete(String subscriptionId) {
                                return null;
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this class has no own two-argument save, only the CheckpointStorage default, which calls this method for any(), so delegating any() here would recurse. This always refuses instead, even any(). Give the class its own two-argument save, or evaluate `condition` for real here. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                return Mono.error(new UnsupportedOperationException("This storage cannot evaluate " + condition + ". It has no two-argument save to fall back on, so even any() is refused."));
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this always answers an empty Mono, correct if this storage cannot evaluate a condition. Signal the version a condition is judged against if it can. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Mono<Long> writeVersion(String subscriptionId) {
                                return Mono.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesABlockingImplementerWhoseSaveAndWriteVersionComeFromAnInSourceAbstractBaseUntouched() {
        // AbstractCheckpointStorage is abstract, so the recipe never stubs it directly (only a concrete class has
        // to answer every member). InMemoryCheckpointStorage below declares neither the three-argument save nor
        // writeVersion itself, only inheriting both, concretely implemented, from that abstract base. Before this
        // fix, alreadyDeclared only looked at a class's own body, so it would have missed this inherited
        // implementation and generated a stub that overrides it with the any()-only shape, discarding whatever
        // real persistence logic the abstract base provided.
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        import java.util.OptionalLong;

                        abstract class AbstractCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                return checkpoint;
                            }

                            @Override
                            public OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }
                        }
                        """
                ),
                // No change expected. Both members are already concretely implemented on the abstract base.
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;

                        class InMemoryCheckpointStorage extends AbstractCheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesABlockingImplementerWhoseSaveComesFromTwoLevelsUpAnInSourceAbstractBaseUntouched() {
        // BaseCheckpointStorage declares the three-argument save two levels above the concrete class.
        // MiddleCheckpointStorage, the immediate superclass, declares neither save nor writeVersion itself, only
        // inheriting both. hasOwnConcreteImplementation has to walk past that immediate superclass to find the
        // concrete declaration, not stop there and conclude nothing overrides the interface.
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        import java.util.OptionalLong;

                        abstract class BaseCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                return checkpoint;
                            }

                            @Override
                            public OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        abstract class MiddleCheckpointStorage extends BaseCheckpointStorage {
                        }
                        """
                ),
                // No change expected. Both members are concretely implemented two levels up, on BaseCheckpointStorage.
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;

                        class InMemoryCheckpointStorage extends MiddleCheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesABlockingImplementerOfACustomInterfaceThatDefaultsBothMembersUntouched() {
        // CustomCheckpointStorage is a capability interface of its own, extending CheckpointStorage and supplying
        // both members as defaults. InMemoryCheckpointStorage implements CustomCheckpointStorage directly, with no
        // class in between, so the concrete class's own declared interface, not a superclass, is where the real
        // implementation lives. hasOwnConcreteImplementation has to look at that interface too, not only the
        // superclass chain, or it would discard both defaults.
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        import java.util.OptionalLong;

                        interface CustomCheckpointStorage extends CheckpointStorage {
                            @Override
                            default Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                return checkpoint;
                            }

                            @Override
                            default OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }
                        }
                        """
                ),
                // No change expected. Both members are concretely implemented on CustomCheckpointStorage, the
                // interface this class directly implements.
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;

                        class InMemoryCheckpointStorage implements CustomCheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void generatesAnAlwaysRefusingSaveWhenTheOnlyMatchingSignatureOnAnAbstractBaseIsPrivate() {
        // AbstractCheckpointStorage's own save(String, Checkpoint) is private, an unrelated helper that happens to
        // share the two-argument save's name and signature. A private method cannot override an interface member,
        // so it is never reached by a call through the CheckpointStorage-typed reference the generated stub makes,
        // and InMemoryCheckpointStorage still resolves to CheckpointStorage's own default. Treating the private
        // method as though it overrides something would reintroduce the delegating stub, and with it the
        // recursion this recipe exists to avoid.
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        abstract class AbstractCheckpointStorage implements CheckpointStorage {
                            private Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
                                return checkpoint;
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;

                        class InMemoryCheckpointStorage extends AbstractCheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;

                        import java.util.OptionalLong;

                        class InMemoryCheckpointStorage extends AbstractCheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this class has no own two-argument save, only the CheckpointStorage default, which calls this method for any(), so delegating any() here would recurse. This always refuses instead, even any(). Give the class its own two-argument save, or evaluate `condition` for real here. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                throw new UnsupportedOperationException("This storage cannot evaluate " + condition + ". It has no two-argument save to fall back on, so even any() is refused.");
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this always answers empty, correct if this storage cannot evaluate a condition. Return the version a condition is judged against if it can. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void generatesAWriteVersionStubWhenTheOnlyMatchingSignatureOnAnAbstractBaseIsPackagePrivate() {
        // AbstractCheckpointStorage's own writeVersion(String) has no access modifier at all, package-private, even
        // though it sits in the same package as InMemoryCheckpointStorage. Java refuses to compile a class where a
        // package-private method stands in for a public interface member, same package or not (confirmed directly
        // with javac: "attempting to assign weaker access privileges"), so it does not implement the interface, and
        // the stub still has to be generated. InMemoryCheckpointStorage declares its own two-argument save, so the
        // three-argument save gets the ordinary delegating stub, isolating this test to the writeVersion gap.
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        import java.util.OptionalLong;

                        abstract class AbstractCheckpointStorage implements CheckpointStorage {
                            OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }
                        }
                        """
                ),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;

                        class InMemoryCheckpointStorage extends AbstractCheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
                                return checkpoint;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;

                        import java.util.OptionalLong;

                        class InMemoryCheckpointStorage extends AbstractCheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
                                return checkpoint;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this only refuses a condition stronger than any(), delegating any() to the existing two-argument save. Evaluate `condition` for real if this storage can, otherwise this is the permanent answer. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                if (!(condition instanceof CheckpointWriteCondition.Any)) {
                                    throw new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported.");
                                }
                                return save(subscriptionId, checkpoint);
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this always answers empty, correct if this storage cannot evaluate a condition. Return the version a condition is judged against if it can. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void generatesAWriteVersionStubWhenTheClassOwnMatchingSignatureIsPrivate() {
        // InMemoryCheckpointStorage's own writeVersion(String) is private, an unrelated helper. writeVersion did
        // not exist on CheckpointStorage in 0.32.0, so nothing about that name was reserved yet, and this could be
        // a genuine 0.32.0 implementation detail sharing the name by coincidence. A private method cannot
        // implement the new public interface member, so it still has to be stubbed, and the review comment still
        // has to appear, even though the class's own body is where the mismatch lives this time rather than a
        // supertype's. The stub is generated anyway, alongside the private method, which the review comment does
        // not resolve by itself. A class silently left without a stub or a marker at all is worse though. The
        // compile error becomes a duplicate-signature one instead of a missing-member one, with a comment
        // pointing at why.
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        import java.util.OptionalLong;

                        class InMemoryCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
                                return checkpoint;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }

                            private OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        import java.util.OptionalLong;

                        class InMemoryCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
                                return checkpoint;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }

                            private OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this only refuses a condition stronger than any(), delegating any() to the existing two-argument save. Evaluate `condition` for real if this storage can, otherwise this is the permanent answer. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                if (!(condition instanceof CheckpointWriteCondition.Any)) {
                                    throw new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported.");
                                }
                                return save(subscriptionId, checkpoint);
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this always answers empty, correct if this storage cannot evaluate a condition. Return the version a condition is judged against if it can. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void generatesAWriteVersionStubWhenTheClassOwnMatchingSignatureIsStatic() {
        // InMemoryCheckpointStorage's own writeVersion(String) is public but static, another unrelated helper the
        // interface's new member cannot reuse. A static method cannot override an instance member (confirmed
        // directly with javac: "overriding method is static"), so this is the same coincidental-collision shape as
        // the private case above, just with a different reason the method cannot implement the interface.
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        import java.util.OptionalLong;

                        class InMemoryCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
                                return checkpoint;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }

                            public static OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        import java.util.OptionalLong;

                        class InMemoryCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint) {
                                return checkpoint;
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }

                            public static OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this only refuses a condition stronger than any(), delegating any() to the existing two-argument save. Evaluate `condition` for real if this storage can, otherwise this is the permanent answer. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                if (!(condition instanceof CheckpointWriteCondition.Any)) {
                                    throw new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported.");
                                }
                                return save(subscriptionId, checkpoint);
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this always answers empty, correct if this storage cannot evaluate a condition. Return the version a condition is judged against if it can. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void generatesAWriteVersionStubOnTheReactorStackWhenTheOwnMatchingSignatureReturnsTheWrongMonoTypeArgument() {
        // InMemoryReactorCheckpointStorage's own writeVersion(String) returns Mono<String>, not the required
        // Mono<Long>, an unrelated helper writeVersion did not exist to collide with in 0.32.0. A raw-type check
        // alone would accept any Mono<T> here, so this specifically has to fail the type-argument comparison, not
        // just the raw one.
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(MONO),
                java(REACTOR_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.api.reactor.CheckpointStorage;
                        import reactor.core.publisher.Mono;

                        class InMemoryReactorCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Mono<Checkpoint> read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
                                return Mono.just(checkpoint);
                            }

                            @Override
                            public Mono<Void> delete(String subscriptionId) {
                                return null;
                            }

                            public Mono<String> writeVersion(String subscriptionId) {
                                return Mono.just("unrelated");
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.reactor.CheckpointStorage;
                        import reactor.core.publisher.Mono;

                        class InMemoryReactorCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Mono<Checkpoint> read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint) {
                                return Mono.just(checkpoint);
                            }

                            @Override
                            public Mono<Void> delete(String subscriptionId) {
                                return null;
                            }

                            public Mono<String> writeVersion(String subscriptionId) {
                                return Mono.just("unrelated");
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this only refuses a condition stronger than any(), delegating any() to the existing two-argument save. Evaluate `condition` for real if this storage can, otherwise this is the permanent answer. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                if (!(condition instanceof CheckpointWriteCondition.Any)) {
                                    return Mono.error(new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported."));
                                }
                                return save(subscriptionId, checkpoint);
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this always answers an empty Mono, correct if this storage cannot evaluate a condition. Signal the version a condition is judged against if it can. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Mono<Long> writeVersion(String subscriptionId) {
                                return Mono.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void generatesADelegatingSaveWhenTheClassOwnTwoArgumentSaveHasARawMonoReturnType() {
        // InMemoryReactorCheckpointStorage's own two-argument save returns a raw Mono, not the parameterized
        // Mono<Checkpoint> the interface declares. That still compiles as an override, with an unchecked-conversion
        // warning rather than an error, so it is a genuine two-argument save this class can delegate to, not a
        // coincidental unrelated method. The delegating stub is still the right one to generate.
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(MONO),
                java(REACTOR_CHECKPOINT_STORAGE),
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.api.reactor.CheckpointStorage;
                        import reactor.core.publisher.Mono;

                        class InMemoryReactorCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Mono<Checkpoint> read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Mono save(String subscriptionId, Checkpoint checkpoint) {
                                return Mono.just(checkpoint);
                            }

                            @Override
                            public Mono<Void> delete(String subscriptionId) {
                                return null;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.reactor.CheckpointStorage;
                        import reactor.core.publisher.Mono;

                        class InMemoryReactorCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Mono<Checkpoint> read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Mono save(String subscriptionId, Checkpoint checkpoint) {
                                return Mono.just(checkpoint);
                            }

                            @Override
                            public Mono<Void> delete(String subscriptionId) {
                                return null;
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this only refuses a condition stronger than any(), delegating any() to the existing two-argument save. Evaluate `condition` for real if this storage can, otherwise this is the permanent answer. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                if (!(condition instanceof CheckpointWriteCondition.Any)) {
                                    return Mono.error(new UnsupportedOperationException("This storage cannot evaluate " + condition + ", only any() is supported."));
                                }
                                return save(subscriptionId, checkpoint);
                            }

                            /* TODO [Occurrent 0.33 upgrade]: this always answers an empty Mono, correct if this storage cannot evaluate a condition. Signal the version a condition is judged against if it can. See doc/migration/upgrading-to-0.33.0.md. */
                            @Override
                            public Mono<Long> writeVersion(String subscriptionId) {
                                return Mono.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesABlockingImplementerThatAlreadyHasBothMembersUntouched() {
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                // No change expected: both members are already declared by hand, so the recipe finds nothing missing.
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        import java.util.OptionalLong;

                        class InMemoryCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Checkpoint read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Checkpoint save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                return checkpoint;
                            }

                            @Override
                            public OptionalLong writeVersion(String subscriptionId) {
                                return OptionalLong.empty();
                            }

                            @Override
                            public void delete(String subscriptionId) {
                            }

                            @Override
                            public boolean exists(String subscriptionId) {
                                return false;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAReactorImplementerThatAlreadyHasBothMembersUntouched() {
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(MONO),
                java(REACTOR_CHECKPOINT_STORAGE),
                // No change expected: both members are already declared by hand, so the recipe finds nothing missing.
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.CheckpointWriteCondition;
                        import org.occurrent.subscription.api.reactor.CheckpointStorage;
                        import reactor.core.publisher.Mono;

                        class InMemoryReactorCheckpointStorage implements CheckpointStorage {
                            @Override
                            public Mono<Checkpoint> read(String subscriptionId) {
                                return null;
                            }

                            @Override
                            public Mono<Checkpoint> save(String subscriptionId, Checkpoint checkpoint, CheckpointWriteCondition condition) {
                                return Mono.error(new UnsupportedOperationException());
                            }

                            @Override
                            public Mono<Long> writeVersion(String subscriptionId) {
                                return Mono.error(new UnsupportedOperationException());
                            }

                            @Override
                            public Mono<Void> delete(String subscriptionId) {
                                return null;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesCallingCodeUntouched() {
        rewriteRun(
                java(CHECKPOINT),
                java(CHECKPOINT_WRITE_CONDITION),
                java(BLOCKING_CHECKPOINT_STORAGE),
                // No change expected: this class merely calls the two-argument save, it does not implement the interface.
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.Checkpoint;
                        import org.occurrent.subscription.api.blocking.CheckpointStorage;

                        class Foo {
                            void run(CheckpointStorage storage, String subscriptionId, Checkpoint checkpoint) {
                                storage.save(subscriptionId, checkpoint);
                            }
                        }
                        """
                )
        );
    }
}
