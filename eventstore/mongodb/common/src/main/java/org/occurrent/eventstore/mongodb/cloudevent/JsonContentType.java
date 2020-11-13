package org.occurrent.eventstore.mongodb.cloudevent;

class JsonContentType {

    public static boolean isJson(Object contentTypeObject) {
        if (contentTypeObject == null) {
            // An undefined content-type means application/json according to the cloud event spec
            return true;
        } else if (!(contentTypeObject instanceof String)) {
            return false;
        }
        String contentType = ((String) contentTypeObject).toLowerCase();
        return contentType.contains("/json") || contentType.contains("+json");
    }
}
