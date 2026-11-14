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

package org.occurrent.springboot.common;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.annotation.Projection;
import org.occurrent.annotation.Saga;
import org.occurrent.annotation.Snapshot;
import org.occurrent.springboot.common.internal.HandlerScan;

import java.lang.annotation.Annotation;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A static factory repeating the annotation of the static factory of the same name and parameters it hides is a
 * factory of its own, so the scan returns both, whatever their visibility.
 */
@DisplayNameGeneration(ReplaceUnderscores.class)
class HandlerScanHiddenStaticFactoryTest {

    static class PublicBase {
        @Projection(id = "p")
        @SuppressWarnings("unused")
        public static void factory() {
        }
    }

    static class PublicHiding extends PublicBase {
        @Projection(id = "p")
        @SuppressWarnings("unused")
        public static void factory() {
        }
    }

    static class ProtectedBase {
        @Projection(id = "p")
        @SuppressWarnings("unused")
        protected static void factory() {
        }
    }

    static class ProtectedHiding extends ProtectedBase {
        @Projection(id = "p")
        @SuppressWarnings("unused")
        protected static void factory() {
        }
    }

    @Test
    void handlerMethods_returns_both_public_static_factories_when_the_one_hiding_the_other_repeats_the_annotation() throws Exception {
        assertScansBoth(PublicHiding.class, PublicBase.class);
    }

    @Test
    void handlerMethods_returns_both_protected_static_factories_when_the_one_hiding_the_other_repeats_the_annotation() throws Exception {
        assertScansBoth(ProtectedHiding.class, ProtectedBase.class);
    }

    private static void assertScansBoth(Class<?> hiding, Class<?> hidden) throws Exception {
        List<Class<? extends Annotation>> annotations = List.of(Projection.class, Snapshot.class, Saga.class);

        assertThat(HandlerScan.handlerMethods(hiding, annotations))
                .containsExactlyInAnyOrder(hiding.getDeclaredMethod("factory"), hidden.getDeclaredMethod("factory"));
    }
}
