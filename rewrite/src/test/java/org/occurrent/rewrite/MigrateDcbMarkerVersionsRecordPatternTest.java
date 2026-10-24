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
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.TypeUtils;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;
import org.openrewrite.test.TypeValidation;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.rewrite.AppendResultStubs.APPEND_ID;
import static org.occurrent.rewrite.AppendResultStubs.WRITE_RESULT;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.CLOUD_EVENT;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.DCB_APPEND_RESULT;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.DCB_CONSISTENCY_TOKEN;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.DCB_EVENT_STREAM;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.DCB_MARKER_VERSIONS;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.DCB_READ_OPTIONS;
import static org.occurrent.rewrite.DcbMarkerVersionsStubs.POSITION_RANGE;
import static org.openrewrite.java.Assertions.java;

/**
 * Every case here is an actual 0.34.0 caller's record pattern, unchanged, meeting the 0.35.0
 * {@link DcbMarkerVersionsStubs} classpath whose canonical arity it no longer matches.
 * <p>
 * Identifier type validation is off for the same reason as in {@link MigrateAppendResultRecordPatternTest}. This
 * version's parser leaves a binding pattern's own identifier unattributed inside a {@code switch} case label, and
 * that is not something this recipe causes. The type this recipe stamps on its own synthesized binding is asserted
 * explicitly in the cases that read it back off the rewritten tree, and structurally through the expected "after"
 * source everywhere else.
 */
class MigrateDcbMarkerVersionsRecordPatternTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new MigrateDcbMarkerVersionsRecordPattern())
                .parser(JavaParser.fromJavaVersion().dependsOn(APPEND_ID, WRITE_RESULT, CLOUD_EVENT, POSITION_RANGE,
                        DCB_CONSISTENCY_TOKEN, DCB_MARKER_VERSIONS, DCB_APPEND_RESULT, DCB_EVENT_STREAM, DCB_READ_OPTIONS))
                .typeValidationOptions(TypeValidation.builder().identifiers(false).build());
    }

    @Test
    void addsTheMarkerVersionsBindingToAVarDcbAppendResultSwitchPattern() {
        rewriteRun(
                java(
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
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbAppendResult(var first, var last, var count, var appendId, var markerVersions) -> first + ".." + last + " (" + count + ")";
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void addsTheMarkerVersionsBindingToAnExplicitlyTypedDcbAppendResultInstanceofPattern() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.AppendId;
                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        import java.util.Optional;

                        class Reader {
                            long lastPosition(Object result) {
                                if (result instanceof DcbAppendResult(long first, long last, int count, Optional<AppendId> appendId)) {
                                    return last;
                                }
                                return -1;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.AppendId;
                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        import java.util.Optional;

                        class Reader {
                            long lastPosition(Object result) {
                                if (result instanceof DcbAppendResult(long first, long last, int count, Optional<AppendId> appendId, var markerVersions)) {
                                    return last;
                                }
                                return -1;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void addsTheMarkerVersionsBindingToAVarDcbEventStreamSwitchPattern() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbEventStream;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbEventStream(var events, var lastSequencePosition, var consistencyToken) -> events.size() + "@" + lastSequencePosition;
                                    default -> "unknown";
                                };
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbEventStream;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbEventStream(var events, var lastSequencePosition, var consistencyToken, var markerVersions) -> events.size() + "@" + lastSequencePosition;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void addsTheMarkerVersionsBindingToAnExplicitlyTypedDcbEventStreamInstanceofPattern() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import io.cloudevents.CloudEvent;
                        import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
                        import org.occurrent.eventstore.api.dcb.DcbEventStream;

                        import java.util.List;

                        class Reader {
                            int count(Object result) {
                                if (result instanceof DcbEventStream(List<CloudEvent> events, long lastSequencePosition, DcbConsistencyToken consistencyToken)) {
                                    return events.size();
                                }
                                return 0;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import io.cloudevents.CloudEvent;
                        import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
                        import org.occurrent.eventstore.api.dcb.DcbEventStream;

                        import java.util.List;

                        class Reader {
                            int count(Object result) {
                                if (result instanceof DcbEventStream(List<CloudEvent> events, long lastSequencePosition, DcbConsistencyToken consistencyToken, var markerVersions)) {
                                    return events.size();
                                }
                                return 0;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void addsTheVersionsRequestedAndAfterVersionsBindingsInThatOrderToAVarDcbReadOptionsSwitchPattern() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbReadOptions;

                        class Reader {
                            String describe(Object options) {
                                return switch (options) {
                                    case DcbReadOptions(var positionRange, var direction, var skip, var limit) -> direction + ":" + skip;
                                    default -> "unknown";
                                };
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbReadOptions;

                        class Reader {
                            String describe(Object options) {
                                return switch (options) {
                                    case DcbReadOptions(var positionRange, var direction, var skip, var limit, var versionsRequested, var afterVersions) -> direction + ":" + skip;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void addsTheVersionsRequestedAndAfterVersionsBindingsToAnExplicitlyTypedDcbReadOptionsInstanceofPattern() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.PositionRange;
                        import org.occurrent.eventstore.api.dcb.DcbReadOptions;
                        import org.occurrent.eventstore.api.dcb.DcbReadOptions.Direction;

                        import java.util.OptionalInt;

                        class Reader {
                            int skipped(Object options) {
                                if (options instanceof DcbReadOptions(PositionRange positionRange, Direction direction, int skip, OptionalInt limit)) {
                                    return skip;
                                }
                                return 0;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.PositionRange;
                        import org.occurrent.eventstore.api.dcb.DcbReadOptions;
                        import org.occurrent.eventstore.api.dcb.DcbReadOptions.Direction;

                        import java.util.OptionalInt;

                        class Reader {
                            int skipped(Object options) {
                                if (options instanceof DcbReadOptions(PositionRange positionRange, Direction direction, int skip, OptionalInt limit, var versionsRequested, var afterVersions)) {
                                    return skip;
                                }
                                return 0;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void rewritesATargetPatternNestedInsideAnotherRecordPattern() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            record Outcome(String name, Object result) {
                            }

                            String describe(Object o) {
                                return switch (o) {
                                    case Outcome(var name, DcbAppendResult(var first, var last, var count, var appendId)) -> name + ":" + last;
                                    default -> "unknown";
                                };
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            record Outcome(String name, Object result) {
                            }

                            String describe(Object o) {
                                return switch (o) {
                                    case Outcome(var name, DcbAppendResult(var first, var last, var count, var appendId, var markerVersions)) -> name + ":" + last;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void givesTwoSiblingTargetPatternsInOneCaseLabelDistinctMarkerVersionsNames() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;
                        import org.occurrent.eventstore.api.dcb.DcbEventStream;

                        class Reader {
                            record Pair(DcbAppendResult append, DcbEventStream read) {
                            }

                            String describe(Object o) {
                                return switch (o) {
                                    case Pair(DcbAppendResult(var a, var b, var c, var d), DcbEventStream(var events, var last, var token)) -> events.size() + ":" + b;
                                    default -> "unknown";
                                };
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;
                        import org.occurrent.eventstore.api.dcb.DcbEventStream;

                        class Reader {
                            record Pair(DcbAppendResult append, DcbEventStream read) {
                            }

                            String describe(Object o) {
                                return switch (o) {
                                    case Pair(DcbAppendResult(var a, var b, var c, var d, var markerVersions), DcbEventStream(var events, var last, var token, var markerVersions1)) -> events.size() + ":" + b;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void generatesACollisionSafeNameWhenAnExistingBindingIsAlreadyCalledMarkerVersions() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbEventStream;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbEventStream(var events, var lastSequencePosition, var markerVersions) -> events.size() + ":" + markerVersions;
                                    default -> "unknown";
                                };
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbEventStream;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbEventStream(var events, var lastSequencePosition, var markerVersions, var markerVersions1) -> events.size() + ":" + markerVersions;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void generatesACollisionSafeNameWhenAnEnclosingLocalIsAlreadyCalledMarkerVersions() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            String describe(Object result) {
                                String markerVersions = "unrelated";
                                if (result instanceof DcbAppendResult(var first, var last, var count, var appendId)) {
                                    return markerVersions + ":" + last;
                                }
                                return markerVersions;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            String describe(Object result) {
                                String markerVersions = "unrelated";
                                if (result instanceof DcbAppendResult(var first, var last, var count, var appendId, var markerVersions1)) {
                                    return markerVersions + ":" + last;
                                }
                                return markerVersions;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void generatesACollisionSafeNameForEachDcbReadOptionsBindingThatIsAlreadyTaken() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbReadOptions;

                        class Reader {
                            String describe(Object options, boolean versionsRequested) {
                                String afterVersions = "unrelated";
                                if (options instanceof DcbReadOptions(var positionRange, var direction, var skip, var limit)) {
                                    return afterVersions + ":" + versionsRequested + ":" + skip;
                                }
                                return afterVersions;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbReadOptions;

                        class Reader {
                            String describe(Object options, boolean versionsRequested) {
                                String afterVersions = "unrelated";
                                if (options instanceof DcbReadOptions(var positionRange, var direction, var skip, var limit, var versionsRequested1, var afterVersions1)) {
                                    return afterVersions + ":" + versionsRequested + ":" + skip;
                                }
                                return afterVersions;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void takesTheNextFreeNumberWhenMarkerVersionsAndMarkerVersions1AreBothTaken() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            String describe(Object result) {
                                String markerVersions = "a";
                                String markerVersions1 = "b";
                                if (result instanceof DcbAppendResult(var first, var last, var count, var appendId)) {
                                    return markerVersions + markerVersions1 + last;
                                }
                                return markerVersions;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            String describe(Object result) {
                                String markerVersions = "a";
                                String markerVersions1 = "b";
                                if (result instanceof DcbAppendResult(var first, var last, var count, var appendId, var markerVersions2)) {
                                    return markerVersions + markerVersions1 + last;
                                }
                                return markerVersions;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesADcbAppendResultPatternWithThreeBindingsAlone() {
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
                        """
                )
        );
    }

    @Test
    void leavesADcbAppendResultPatternWithFiveBindingsAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbAppendResult(var first, var last, var count, var appendId, var markerVersions) -> markerVersions + ":" + appendId;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesADcbEventStreamPatternWithFourBindingsAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbEventStream;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbEventStream(var events, var lastSequencePosition, var consistencyToken, var markerVersions) -> events.size() + ":" + markerVersions;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesADcbReadOptionsPatternWithFiveBindingsAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbReadOptions;

                        class Reader {
                            String describe(Object options) {
                                return switch (options) {
                                    case DcbReadOptions(var positionRange, var direction, var skip, var limit, var versionsRequested) -> direction + ":" + versionsRequested;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesADcbReadOptionsPatternWithSixBindingsAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbReadOptions;

                        class Reader {
                            String describe(Object options) {
                                return switch (options) {
                                    case DcbReadOptions(var positionRange, var direction, var skip, var limit, var versionsRequested, var afterVersions) -> direction + ":" + afterVersions;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAnUnrelatedFourComponentRecordPatternAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        class Reader {
                            record Rectangle(int x, int y, int width, int height) {
                            }

                            String describe(Object o) {
                                return switch (o) {
                                    case Rectangle(var x, var y, var width, var height) -> x + "," + y + "," + width + "," + height;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesARecordNamedLikeATargetInAnotherPackageAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        class Reader {
                            record DcbAppendResult(long first, long last, int count, String note) {
                            }

                            String describe(Object o) {
                                return switch (o) {
                                    case DcbAppendResult(var first, var last, var count, var note) -> first + ".." + last + note;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAFourComponentWriteResultPatternAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.WriteResult;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case WriteResult(var streamId, var oldStreamVersion, var newStreamVersion, var appendId) -> streamId + ":" + appendId;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    @Test
    void stampsTheMarkerVersionsBindingWithTheOptionalMarkerVersionsTypeOfTheDcbAppendResultComponent() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            boolean matches(Object result) {
                                return result instanceof DcbAppendResult(var first, var last, var count, var appendId);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbAppendResult;

                        class Reader {
                            boolean matches(Object result) {
                                return result instanceof DcbAppendResult(var first, var last, var count, var appendId, var markerVersions);
                            }
                        }
                        """,
                        spec -> spec.afterRecipe(cu -> assertThat(bindingTypes(cu, "markerVersions"))
                                .containsExactly("java.util.Optional<org.occurrent.eventstore.api.dcb.DcbMarkerVersions>"))
                )
        );
    }

    @Test
    void stampsTheMarkerVersionsBindingWithTheOptionalMarkerVersionsTypeOfTheDcbEventStreamComponent() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbEventStream;

                        class Reader {
                            boolean matches(Object result) {
                                return result instanceof DcbEventStream(var events, var last, var token);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbEventStream;

                        class Reader {
                            boolean matches(Object result) {
                                return result instanceof DcbEventStream(var events, var last, var token, var markerVersions);
                            }
                        }
                        """,
                        spec -> spec.afterRecipe(cu -> assertThat(bindingTypes(cu, "markerVersions"))
                                .containsExactly("java.util.Optional<org.occurrent.eventstore.api.dcb.DcbMarkerVersions>"))
                )
        );
    }

    @Test
    void stampsEachDcbReadOptionsBindingWithTheTypeOfItsOwnComponent() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbReadOptions;

                        class Reader {
                            boolean matches(Object options) {
                                return options instanceof DcbReadOptions(var range, var direction, var skip, var limit);
                            }
                        }
                        """,
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbReadOptions;

                        class Reader {
                            boolean matches(Object options) {
                                return options instanceof DcbReadOptions(var range, var direction, var skip, var limit, var versionsRequested, var afterVersions);
                            }
                        }
                        """,
                        spec -> spec.afterRecipe(cu -> {
                            assertThat(bindingTypes(cu, "versionsRequested")).containsExactly("boolean");
                            assertThat(bindingTypes(cu, "afterVersions"))
                                    .containsExactly("java.util.Optional<org.occurrent.eventstore.api.dcb.DcbMarkerVersions>");
                        })
                )
        );
    }

    /**
     * The consistency token is the one record in these three signatures that sits in a last position of the old arity,
     * so a pattern that destructures it is the one shape where the old last element is itself a nested record
     * pattern and not a plain binding. It is skipped, known and accepted, rather than guessed at.
     */
    @Test
    void leavesADcbEventStreamPatternWhoseLastElementIsANestedRecordPatternAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.eventstore.api.dcb.DcbConsistencyToken;
                        import org.occurrent.eventstore.api.dcb.DcbEventStream;

                        class Reader {
                            String describe(Object result) {
                                return switch (result) {
                                    case DcbEventStream(var events, var lastSequencePosition, DcbConsistencyToken(var value)) -> events.size() + ":" + value;
                                    default -> "unknown";
                                };
                            }
                        }
                        """
                )
        );
    }

    private static List<String> bindingTypes(J.CompilationUnit cu, String name) {
        List<String> types = new ArrayList<>();
        new JavaIsoVisitor<Void>() {
            @Override
            public J.VariableDeclarations.NamedVariable visitVariable(J.VariableDeclarations.NamedVariable variable, Void unused) {
                if (variable.getSimpleName().equals(name)) {
                    types.add(describe(variable.getVariableType() == null ? null : variable.getVariableType().getType()));
                }
                return super.visitVariable(variable, unused);
            }
        }.visit(cu, null);
        return types;
    }

    private static String describe(JavaType type) {
        if (type instanceof JavaType.Primitive primitive) {
            return primitive.getKeyword();
        }
        JavaType.Parameterized parameterized = TypeUtils.asParameterized(type);
        if (parameterized != null) {
            return parameterized.getFullyQualifiedName() + parameterized.getTypeParameters().stream()
                    .map(MigrateDcbMarkerVersionsRecordPatternTest::describe)
                    .collect(Collectors.joining(",", "<", ">"));
        }
        JavaType.FullyQualified fullyQualified = TypeUtils.asFullyQualified(type);
        return fullyQualified == null ? String.valueOf(type) : fullyQualified.getFullyQualifiedName();
    }
}
