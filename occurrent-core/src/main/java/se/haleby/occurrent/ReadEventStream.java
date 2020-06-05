package se.haleby.occurrent;

public interface ReadEventStream {
    <T> EventStream<T> read(String streamId, Class<T> t);
}
