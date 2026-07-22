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

package org.occurrent.command.annotation;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator.ReplaceUnderscores;
import org.junit.jupiter.api.Test;
import org.occurrent.command.StreamIdResolver;
import org.occurrent.annotation.TargetStream;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(ReplaceUnderscores.class)
class AnnotationStreamIdResolverTest {

    private final StreamIdResolver<Object> resolver = new AnnotationStreamIdResolver<>();

    @Test
    void resolves_the_stream_id_from_a_record_component() {
        String id = UUID.randomUUID().toString();
        assertThat(resolver.streamId(new PlaceOrder(id))).isEqualTo(id);
    }

    @Test
    void resolves_the_stream_id_from_a_getter() {
        assertThat(resolver.streamId(new GetterCommand("order-42"))).isEqualTo("order-42");
    }

    @Test
    void converts_a_non_string_member_with_toString() {
        UUID id = UUID.randomUUID();
        assertThat(resolver.streamId(new UuidCommand(id))).isEqualTo(id.toString());
    }

    @Test
    void caches_the_accessor_and_reuses_it_across_commands() {
        assertThat(resolver.streamId(new PlaceOrder("a"))).isEqualTo("a");
        assertThat(resolver.streamId(new PlaceOrder("b"))).isEqualTo("b");
    }

    @Test
    void throws_when_no_member_is_annotated() {
        assertThatThrownBy(() -> resolver.streamId(new Unannotated("x")))
                .isInstanceOf(AnnotationStreamIdResolver.AnnotationStreamIdResolverException.class)
                .hasMessageContaining("No @TargetStream member");
    }

    @Test
    void throws_when_more_than_one_member_is_annotated() {
        assertThatThrownBy(() -> resolver.streamId(new TwoTargets("a", "b")))
                .isInstanceOf(AnnotationStreamIdResolver.AnnotationStreamIdResolverException.class)
                .hasMessageContaining("exactly one target stream id");
    }

    @Test
    void throws_when_the_annotated_member_is_null() {
        assertThatThrownBy(() -> resolver.streamId(new PlaceOrder(null)))
                .isInstanceOf(AnnotationStreamIdResolver.AnnotationStreamIdResolverException.class)
                .hasMessageContaining("is null");
    }

    @Test
    void throws_when_the_annotated_member_is_blank() {
        assertThatThrownBy(() -> resolver.streamId(new PlaceOrder("  ")))
                .isInstanceOf(AnnotationStreamIdResolver.AnnotationStreamIdResolverException.class)
                .hasMessageContaining("is blank");
    }

    @Test
    void supports_a_custom_marker_annotation() {
        StreamIdResolver<Object> custom = new AnnotationStreamIdResolver<>(Aggregate.class);
        assertThat(custom.streamId(new CustomAnnotatedCommand("cart-9"))).isEqualTo("cart-9");
    }

    @Test
    void rejects_an_annotation_without_runtime_retention() {
        assertThatThrownBy(() -> new AnnotationStreamIdResolver<>(SourceOnly.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RUNTIME");
    }

    // --- fixtures ---

    record PlaceOrder(@TargetStream String orderId) {
    }

    record UuidCommand(@TargetStream UUID orderId) {
    }

    record Unannotated(String orderId) {
    }

    record TwoTargets(@TargetStream String a, @TargetStream String b) {
    }

    static final class GetterCommand {
        private final String orderId;

        GetterCommand(String orderId) {
            this.orderId = orderId;
        }

        @TargetStream
        String orderId() {
            return orderId;
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.RECORD_COMPONENT, ElementType.FIELD, ElementType.METHOD})
    @interface Aggregate {
    }

    record CustomAnnotatedCommand(@Aggregate String cartId) {
    }

    @Retention(RetentionPolicy.SOURCE)
    @interface SourceOnly {
    }
}
