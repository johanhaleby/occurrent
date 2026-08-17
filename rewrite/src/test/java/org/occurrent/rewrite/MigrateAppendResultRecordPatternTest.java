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
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;
import org.openrewrite.test.TypeValidation;

import static org.occurrent.rewrite.AppendResultStubs.APPEND_ID;
import static org.occurrent.rewrite.AppendResultStubs.DCB_APPEND_RESULT;
import static org.occurrent.rewrite.AppendResultStubs.WRITE_RESULT;
import static org.openrewrite.java.Assertions.java;

/**
 * Every case here is an actual 0.33.0 caller's record pattern, unchanged, meeting the 0.34.0
 * {@link AppendResultStubs} classpath whose canonical arity it no longer matches.
 * <p>
 * Identifier type validation is off. This version's parser leaves a binding pattern's own identifier unattributed
 * inside a {@code switch} case label specifically, not an {@code instanceof} pattern, and not anything this
 * recipe's own output causes. It is reproducible with a plain, unmodified {@code record Point(int x, int y, int z)}
 * switch pattern under no recipe at all. The type this recipe stamps on its own synthesized binding is still
 * asserted structurally, through the expected "after" source each rewriting test compares against.
 */
class MigrateAppendResultRecordPatternTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new MigrateAppendResultRecordPattern())
                .parser(JavaParser.fromJavaVersion().dependsOn(APPEND_ID, WRITE_RESULT, DCB_APPEND_RESULT))
                .typeValidationOptions(TypeValidation.builder().identifiers(false).build());
    }

    @Test
    void addsTheAppendIdBindingToAVarWriteResultSwitchPattern() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.WriteResult;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case WriteResult(var streamId, var oldStreamVersion, var newStreamVersion) ->
                                            streamId + ":" + oldStreamVersion + "->" + newStreamVersion;
                                    default -> "unknown";
                                };
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.WriteResult;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case WriteResult(var streamId, var oldStreamVersion, var newStreamVersion, var appendId) ->
                                            streamId + ":" + oldStreamVersion + "->" + newStreamVersion;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void addsTheAppendIdBindingToAnExplicitlyTypedWriteResultInstanceofPattern() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.WriteResult;

                        class Reader {
                            String describe(Object result) {
                                if (result instanceof WriteResult(String streamId, long oldStreamVersion, long newStreamVersion)) {
                                    return streamId;
                                }
                                return "unknown";
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.WriteResult;

                        class Reader {
                            String describe(Object result) {
                                if (result instanceof WriteResult(String streamId, long oldStreamVersion, long newStreamVersion, var appendId)) {
                                    return streamId;
                                }
                                return "unknown";
                            }
                        }
                        """
                )
        );
    }

    @Test
    void addsTheAppendIdBindingToADcbAppendResultPattern() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbAppendResult(var first, var last, var count) -> first + ".." + last + " (" + count + ")";
                                    default -> "unknown";
                                };
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbAppendResult(var first, var last, var count, var appendId) -> first + ".." + last + " (" + count + ")";
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAnAlreadyFourComponentPatternAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.WriteResult;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case WriteResult(var streamId, var oldStreamVersion, var newStreamVersion, var appendId) ->
                                            streamId + ":" + appendId;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAnUnrelatedThreeComponentRecordPatternAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        class Reader {
                            record Point(int x, int y, int z) {
                            }

                            String describe(Object o) {
                                return switch (o) {
                                    case Point(var x, var y, var z) -> x + "," + y + "," + z;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }
}
