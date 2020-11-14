package org.occurrent.eventstore.mongodb.cloudevent;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ContentTypeJsonTest {

    @SuppressWarnings("ConstantConditions")
    @Test
    void returns_true_when_content_type_is_null() {
        // When
        boolean isJson = ContentType.isJson(null);

        // Then
        assertThat(isJson).isTrue();
    }

    @Test
    void returns_true_when_content_type_is_application_json() {
        // Given
        String contentType = "application/json";

        // When
        boolean isJson = ContentType.isJson(contentType);

        // Then
        assertThat(isJson).isTrue();
    }

    @Test
    void returns_true_when_content_type_is_text_json() {
        // Given
        String contentType = "text/json";

        // When
        boolean isJson = ContentType.isJson(contentType);

        // Then
        assertThat(isJson).isTrue();
    }
    
    @Test
    void returns_true_when_content_type_is_contains_plus_json() {
        // Given
        String contentType = "application/cloudevents+json";

        // When
        boolean isJson = ContentType.isJson(contentType);

        // Then
        assertThat(isJson).isTrue();
    }

    @Test
    void returns_false_when_content_type_is_text_plain() {
        // Given
        String contentType = "text/plain";

        // When
        boolean isJson = ContentType.isJson(contentType);

        // Then
        assertThat(isJson).isFalse();
    }

    @Test
    void returns_false_when_content_type_is_not_a_string() {
        // Given
        byte[] contentType = new byte[0];

        // When
        boolean isJson = ContentType.isJson(contentType);

        // Then
        assertThat(isJson).isFalse();
    }
}