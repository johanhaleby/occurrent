package se.haleby.occurrent.changestreamer.mongodb.nativedriver.blocking;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

import java.util.Objects;

public class Subscription {
    public final String subscriptionId;
    final MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;

    Subscription(String subscriptionId, MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor) {
        this.subscriptionId = subscriptionId;
        this.cursor = cursor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Subscription)) return false;
        Subscription that = (Subscription) o;
        return Objects.equals(subscriptionId, that.subscriptionId) &&
                Objects.equals(cursor, that.cursor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscriptionId, cursor);
    }

    @Override
    public String toString() {
        return "Subscription{" +
                "subscriptionId='" + subscriptionId + '\'' +
                ", cursor=" + cursor +
                '}';
    }
}