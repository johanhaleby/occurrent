package org.occurrent.eventstore.mongodb.cloudevent;

import org.jspecify.annotations.Nullable;

class ContentType {

    public static boolean isJson(@Nullable Object contentTypeObject) {
        if (contentTypeObject == null) {
            // An undefined content-type means application/json according to the cloud event spec
            return true;
        } else if (!(contentTypeObject instanceof String)) {
            return false;
        }
        String contentType = ((String) contentTypeObject).toLowerCase();
        return contentType.contains("/json") || contentType.contains("+json");
    }

    public static boolean isText(@Nullable Object contentTypeObject) {
        if (contentTypeObject == null) {
            // An undefined content-type means application/json according to the cloud event spec
            return false;
        } else if (!(contentTypeObject instanceof String)) {
            return false;
        }
        String contentType = ((String) contentTypeObject).toLowerCase();
        return contentType.trim().startsWith("text/") || contentType.contains("/xml") || contentType.contains("+xml") || contentType.contains("+csv");
    }
}
