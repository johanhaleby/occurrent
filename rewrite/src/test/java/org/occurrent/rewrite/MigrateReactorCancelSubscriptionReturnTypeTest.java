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

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.occurrent.rewrite.ReactorCancelSubscriptionStubs.CANCELLABLE_SUBSCRIPTIONS;
import static org.occurrent.rewrite.ReactorCancelSubscriptionStubs.DCB_SUBSCRIPTION_MODEL;
import static org.occurrent.rewrite.ReactorCancelSubscriptionStubs.MONO;
import static org.openrewrite.java.Assertions.java;

class MigrateReactorCancelSubscriptionReturnTypeTest implements RewriteTest {

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new MigrateReactorCancelSubscriptionReturnType())
                .parser(JavaParser.fromJavaVersion().dependsOn(MONO, CANCELLABLE_SUBSCRIPTIONS, DCB_SUBSCRIPTION_MODEL));
    }

    @Test
    void returnsAnEmptyMonoFromAnImplementationThatRanOffTheEnd() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        ids.remove(subscriptionId);
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        ids.remove(subscriptionId);
                        return Mono.empty();
                    }
                }
                """
        );
    }

    @Test
    void returnsAnEmptyMonoFromAnEmptyBody() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                class Model implements CancellableSubscriptions {
                    @Override
                    public void cancelSubscription(String subscriptionId) {
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                class Model implements CancellableSubscriptions {
                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        return Mono.empty();
                    }
                }
                """
        );
    }

    @Test
    void turnsAnEarlyReturnIntoAnEmptyMonoAndLeavesTheReturnInALambdaAlone() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        if (!ids.contains(subscriptionId)) {
                            return;
                        }
                        Runnable remove = () -> {
                            if (subscriptionId.isEmpty()) {
                                return;
                            }
                            ids.remove(subscriptionId);
                        };
                        remove.run();
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        if (!ids.contains(subscriptionId)) {
                            return Mono.empty();
                        }
                        Runnable remove = () -> {
                            if (subscriptionId.isEmpty()) {
                                return;
                            }
                            ids.remove(subscriptionId);
                        };
                        remove.run();
                        return Mono.empty();
                    }
                }
                """
        );
    }

    @Test
    void addsNothingAfterAThrow() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                class Model implements CancellableSubscriptions {
                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        throw new UnsupportedOperationException("Cancelling is not supported");
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                class Model implements CancellableSubscriptions {
                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        throw new UnsupportedOperationException("Cancelling is not supported");
                    }
                }
                """
        );
    }

    @Test
    void changesADcbSubscriptionModelImplementationToo() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.DcbSubscriptionModel;

                class Model implements DcbSubscriptionModel {
                    @Override
                    public void cancelSubscription(String subscriptionId) {
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.DcbSubscriptionModel;
                import reactor.core.publisher.Mono;

                class Model implements DcbSubscriptionModel {
                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        return Mono.empty();
                    }
                }
                """
        );
    }

    @Test
    void leavesAClassThatImplementsNeitherInterfaceAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        class Registry {
                            public void cancelSubscription(String subscriptionId) {
                            }
                        }
                        """
                )
        );
    }

    @Test
    void leavesAnImplementationThatAlreadyReturnsAMonoAlone() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                        import reactor.core.publisher.Mono;

                        class Model implements CancellableSubscriptions {
                            @Override
                            public Mono<Void> cancelSubscription(String subscriptionId) {
                                return Mono.empty();
                            }
                        }
                        """
                )
        );
    }

    @Test
    void movesABodyEndingInAnIfWhoseBranchesAllLeaveIntoAVoidMethod() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        if (ids.remove(subscriptionId)) {
                            return;
                        } else {
                            throw new IllegalArgumentException(subscriptionId);
                        }
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        doCancelSubscription(subscriptionId);
                        return Mono.empty();
                    }

                    private void doCancelSubscription(String subscriptionId) {
                        if (ids.remove(subscriptionId)) {
                            return;
                        } else {
                            throw new IllegalArgumentException(subscriptionId);
                        }
                    }
                }
                """
        );
    }

    @Test
    void movesABodyEndingInANestedBlockIntoAVoidMethod() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        {
                            ids.remove(subscriptionId);
                            return;
                        }
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        doCancelSubscription(subscriptionId);
                        return Mono.empty();
                    }

                    private void doCancelSubscription(String subscriptionId) {
                        {
                            ids.remove(subscriptionId);
                            return;
                        }
                    }
                }
                """
        );
    }

    @Test
    void movesABodyEndingInASynchronizedBlockIntoAVoidMethod() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        synchronized (ids) {
                            ids.remove(subscriptionId);
                            return;
                        }
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        doCancelSubscription(subscriptionId);
                        return Mono.empty();
                    }

                    private void doCancelSubscription(String subscriptionId) {
                        synchronized (ids) {
                            ids.remove(subscriptionId);
                            return;
                        }
                    }
                }
                """
        );
    }

    @Test
    void movesABodyEndingInATryFinallyIntoAVoidMethod() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        try {
                            ids.remove(subscriptionId);
                        } finally {
                            return;
                        }
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        doCancelSubscription(subscriptionId);
                        return Mono.empty();
                    }

                    private void doCancelSubscription(String subscriptionId) {
                        try {
                            ids.remove(subscriptionId);
                        } finally {
                            return;
                        }
                    }
                }
                """
        );
    }

    @Test
    void movesABodyEndingInALoopWithNoWayOutButAReturnIntoAVoidMethod() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        while (true) {
                            if (ids.remove(subscriptionId)) {
                                return;
                            }
                        }
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        doCancelSubscription(subscriptionId);
                        return Mono.empty();
                    }

                    private void doCancelSubscription(String subscriptionId) {
                        while (true) {
                            if (ids.remove(subscriptionId)) {
                                return;
                            }
                        }
                    }
                }
                """
        );
    }

    @Test
    void movesABodyEndingInASwitchWhoseBranchesAllLeaveIntoAVoidMethod() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        switch (subscriptionId) {
                            case "" -> throw new IllegalArgumentException("An empty id");
                            default -> {
                                ids.remove(subscriptionId);
                                return;
                            }
                        }
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        doCancelSubscription(subscriptionId);
                        return Mono.empty();
                    }

                    private void doCancelSubscription(String subscriptionId) {
                        switch (subscriptionId) {
                            case "" -> throw new IllegalArgumentException("An empty id");
                            default -> {
                                ids.remove(subscriptionId);
                                return;
                            }
                        }
                    }
                }
                """
        );
    }

    @Test
    void namesTheVoidMethodSoItDoesNotClashWithOneTheClassHas() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        if (ids.remove(subscriptionId)) {
                            doCancelSubscription();
                        }
                    }

                    private void doCancelSubscription() {
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        doCancelSubscription2(subscriptionId);
                        return Mono.empty();
                    }

                    private void doCancelSubscription2(String subscriptionId) {
                        if (ids.remove(subscriptionId)) {
                            doCancelSubscription();
                        }
                    }

                    private void doCancelSubscription() {
                    }
                }
                """
        );
    }

    @Test
    void namesTheVoidMethodSoItDoesNotClashWithOneTheClassInherits() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model extends Base implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        if (ids.remove(subscriptionId)) {
                            doCancelSubscription(subscriptionId);
                        }
                    }
                }

                class Base {
                    void doCancelSubscription(String subscriptionId) {
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model extends Base implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        doCancelSubscription2(subscriptionId);
                        return Mono.empty();
                    }

                    private void doCancelSubscription2(String subscriptionId) {
                        if (ids.remove(subscriptionId)) {
                            doCancelSubscription(subscriptionId);
                        }
                    }
                }

                class Base {
                    void doCancelSubscription(String subscriptionId) {
                    }
                }
                """
        );
    }

    @Test
    void returnsTheCancelOfTheWrappedModelThatTheBodyEndsIn() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();
                    private final CancellableSubscriptions wrapped;

                    Model(CancellableSubscriptions wrapped) {
                        this.wrapped = wrapped;
                    }

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        ids.remove(subscriptionId);
                        wrapped.cancelSubscription(subscriptionId);
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();
                    private final CancellableSubscriptions wrapped;

                    Model(CancellableSubscriptions wrapped) {
                        this.wrapped = wrapped;
                    }

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        ids.remove(subscriptionId);
                        return wrapped.cancelSubscription(subscriptionId);
                    }
                }
                """
        );
    }

    @Test
    void returnsTheCancelOfTheSuperclassThatTheBodyEndsIn() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    @Override
                    public void cancelSubscription(String subscriptionId) {
                    }
                }

                class CountingModel extends Model {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        ids.remove(subscriptionId);
                        super.cancelSubscription(subscriptionId);
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        return Mono.empty();
                    }
                }

                class CountingModel extends Model {
                    private final Set<String> ids = new HashSet<>();

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        ids.remove(subscriptionId);
                        return super.cancelSubscription(subscriptionId);
                    }
                }
                """
        );
    }

    @Test
    void flagsACancelOfTheWrappedModelThatItCannotReturn() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();
                    private final CancellableSubscriptions wrapped;

                    Model(CancellableSubscriptions wrapped) {
                        this.wrapped = wrapped;
                    }

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        wrapped.cancelSubscription(subscriptionId);
                        ids.remove(subscriptionId);
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();
                    private final CancellableSubscriptions wrapped;

                    Model(CancellableSubscriptions wrapped) {
                        this.wrapped = wrapped;
                    }

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        wrapped.cancelSubscription(subscriptionId);
                        ids.remove(subscriptionId);
                        // TODO: return the Mono of the cancelSubscription call this method makes, so that the Mono returned here waits for its cleanup
                        return Mono.empty();
                    }
                }
                """
        );
    }

    @Test
    void flagsACancelOfTheWrappedModelInsideABodyItMoves() {
        migrates(
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();
                    private final CancellableSubscriptions wrapped;

                    Model(CancellableSubscriptions wrapped) {
                        this.wrapped = wrapped;
                    }

                    @Override
                    public void cancelSubscription(String subscriptionId) {
                        if (ids.remove(subscriptionId)) {
                            wrapped.cancelSubscription(subscriptionId);
                        }
                    }
                }
                """,
                """
                package com.example;

                import org.occurrent.subscription.api.reactor.CancellableSubscriptions;
                import reactor.core.publisher.Mono;

                import java.util.HashSet;
                import java.util.Set;

                class Model implements CancellableSubscriptions {
                    private final Set<String> ids = new HashSet<>();
                    private final CancellableSubscriptions wrapped;

                    Model(CancellableSubscriptions wrapped) {
                        this.wrapped = wrapped;
                    }

                    @Override
                    public Mono<Void> cancelSubscription(String subscriptionId) {
                        // TODO: return the Mono of the cancelSubscription call this method makes, so that the Mono returned here waits for its cleanup
                        doCancelSubscription(subscriptionId);
                        return Mono.empty();
                    }

                    private void doCancelSubscription(String subscriptionId) {
                        if (ids.remove(subscriptionId)) {
                            wrapped.cancelSubscription(subscriptionId);
                        }
                    }
                }
                """
        );
    }

    private void migrates(String before, String after) {
        rewriteRun(java(before, after));
        assertCompiles(after);
    }

    // rewriteRun never compiles the result, so this compiles it against stubs of the 0.34.0 interfaces
    private static void assertCompiles(String source) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        List<JavaFileObject> sources = Stream.of(MONO, CANCELLABLE_SUBSCRIPTIONS, DCB_SUBSCRIPTION_MODEL, source)
                .map(MigrateReactorCancelSubscriptionReturnTypeTest::inMemory)
                .toList();
        try {
            Path classes = Files.createTempDirectory("migrated");
            boolean compiled = compiler.getTask(null, null, diagnostics, List.of("-proc:none", "-d", classes.toString()), null, sources).call();
            assertThat(compiled).as("the migrated source compiles, with these diagnostics: %s", diagnostics.getDiagnostics()).isTrue();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static JavaFileObject inMemory(String source) {
        Matcher packageName = Pattern.compile("package ([\\w.]+);").matcher(source);
        Matcher typeName = Pattern.compile("(?:class|interface) (\\w+)").matcher(source);
        if (!packageName.find() || !typeName.find()) {
            throw new IllegalArgumentException("No package or type in " + source);
        }
        URI uri = URI.create("string:///" + packageName.group(1).replace('.', '/') + "/" + typeName.group(1) + ".java");
        return new SimpleJavaFileObject(uri, JavaFileObject.Kind.SOURCE) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                return source;
            }
        };
    }
}
