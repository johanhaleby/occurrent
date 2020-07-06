package se.haleby.occurrent;

public interface ReadEventStream {
    // TODO Add time as parameter and make this method a default method with time = now
    default <T> EventStream<T> read(String streamId, Class<T> t) {
        return read(streamId, 0, Integer.MAX_VALUE, t);
    }

    <T> EventStream<T> read(String streamId, int skip, int limit, Class<T> t);
}
