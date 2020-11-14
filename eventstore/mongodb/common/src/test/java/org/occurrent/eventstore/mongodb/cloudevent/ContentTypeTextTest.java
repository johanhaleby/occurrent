package org.occurrent.eventstore.mongodb.cloudevent;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ContentTypeTextTest {

    @SuppressWarnings("ConstantConditions")
    @Test
    void returns_false_when_content_type_is_null() {
        // When
        boolean isText = ContentType.isText(null);

        // Then
        assertThat(isText).isFalse();
    }

    @Test
    void returns_false_when_content_type_is_application_json() {
        // Given
        String contentType = "application/json";

        // When
        boolean isText = ContentType.isText(contentType);

        // Then
        assertThat(isText).isFalse();
    }

    @Test
    void returns_true_when_content_type_is_application_xml() {
        // Given
        String contentType = "application/xml";

        // When
        boolean isText = ContentType.isText(contentType);

        // Then
        assertThat(isText).isTrue();
    }

    @Test
    void returns_true_when_content_type_is_text_xml() {
        // Given
        String contentType = "text/xml";

        // When
        boolean isText = ContentType.isText(contentType);

        // Then
        assertThat(isText).isTrue();
    }
    
    @Test
    void returns_true_when_content_type_is_contains_plus_xml() {
        // Given
        String contentType = "application/cloudevents+xml";

        // When
        boolean isText = ContentType.isText(contentType);

        // Then
        assertThat(isText).isTrue();
    }

    @Test
    void returns_true_when_content_type_is_text_plain() {
        // Given
        String contentType = "text/plain";

        // When
        boolean isText = ContentType.isText(contentType);

        // Then
        assertThat(isText).isTrue();
    }

    @Test
    void returns_true_when_content_type_is_text_csv() {
        // Given
        String contentType = "text/csv";

        // When
        boolean isText = ContentType.isText(contentType);

        // Then
        assertThat(isText).isTrue();
    }

    @Test
    void returns_true_when_content_type_is_vnd_xml() {
        // Given
        String contentType = "application/occurrent+xml";

        // When
        boolean isText = ContentType.isText(contentType);

        // Then
        assertThat(isText).isTrue();
    }

    @Test
    void returns_true_when_content_type_is_vnd_csv() {
        // Given
        String contentType = "application/occurrent+csv";

        // When
        boolean isText = ContentType.isText(contentType);

        // Then
        assertThat(isText).isTrue();
    }

    @Test
    void returns_false_when_content_type_is_not_a_string() {
        // Given
        byte[] contentType = new byte[0];

        // When
        boolean isText = ContentType.isText(contentType);

        // Then
        assertThat(isText).isFalse();
    }
}