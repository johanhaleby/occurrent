package org.occurrent.eventstore.mongodb.cloudevent;

import org.jspecify.annotations.Nullable;

class ContentType {

    public static boolean isJson(@Nullable Object contentTypeObject) {
        if (contentTypeObject == null) {
            // An undefined content-type means application/json according to the cloud event spec
            return true;
        }
        if (!(contentTypeObject instanceof String contentType)) {
            return false;
        }
        String lowerCaseContentType = contentType.toLowerCase();
        return lowerCaseContentType.contains("/json") || lowerCaseContentType.contains("+json");
    }

    public static boolean isText(@Nullable Object contentTypeObject) {
        if (contentTypeObject == null) {
            // An undefined content-type means application/json according to the cloud event spec
            return false;
        }
        if (!(contentTypeObject instanceof String contentType)) {
            return false;
        }
        String lowerCaseContentType = contentType.toLowerCase();
        return lowerCaseContentType.trim().startsWith("text/") || lowerCaseContentType.contains("/xml") || lowerCaseContentType.contains("+xml") || lowerCaseContentType.contains("+csv");
    }
}
