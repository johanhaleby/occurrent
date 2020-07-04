package se.haleby.occurrent;

public interface ReadEventStream {
    // TODO Add time as parameter and make this method a default method with time = now
    <T> EventStream<T> read(String streamId, Class<T> t);
}
